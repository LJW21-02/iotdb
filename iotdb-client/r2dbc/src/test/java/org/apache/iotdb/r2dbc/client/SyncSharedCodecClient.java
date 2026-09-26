/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.r2dbc.client;

import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration;
import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.r2dbc.exception.ExceptionMapper;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.tsfile.read.common.block.TsBlock;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Blocking transport with the driver codec, used only to attribute benchmark differences.
 *
 * <p>It sends the same requests as {@link IoTDBConnection} through a blocking Thrift transport and
 * decodes every page with the same {@link RowDecoder} the reactive path uses, so the difference
 * between the two arms is the transport and its scheduling rather than the protocol or the row
 * decoding. It is test scope on purpose: it is not a second client, does not implement the SPI and
 * is never used outside the benchmark.
 */
public final class SyncSharedCodecClient implements AutoCloseable {

  private static final String CLIENT_VERSION = "V_1_0";
  private static final String PRECISION_MILLISECOND = "ms";
  private static final String PRECISION_MICROSECOND = "us";
  private static final String PRECISION_NANOSECOND = "ns";

  private final TTransport transport;
  private final IClientRPCService.Client client;
  private final IoTDBConnectionConfiguration configuration;
  private final long sessionId;
  private final long statementId;
  private final int timeFactor;

  private SyncSharedCodecClient(
      TTransport transport,
      IClientRPCService.Client client,
      IoTDBConnectionConfiguration configuration,
      long sessionId,
      long statementId,
      int timeFactor) {
    this.transport = transport;
    this.client = client;
    this.configuration = configuration;
    this.sessionId = sessionId;
    this.statementId = statementId;
    this.timeFactor = timeFactor;
  }

  /** Opens a session the same way the driver does, on a blocking socket. */
  public static SyncSharedCodecClient open(IoTDBConnectionConfiguration configuration)
      throws TException {
    TTransport transport = newTransport(configuration);
    try {
      if (!transport.isOpen()) {
        // The TLS transport factory connects while it builds the socket, the plain one does not.
        transport.open();
      }
      IClientRPCService.Client client =
          new IClientRPCService.Client(protocol(transport, configuration));
      TSOpenSessionResp response = client.openSession(openSessionRequest(configuration));
      ExceptionMapper.verifySuccess(response.getStatus(), null);
      long sessionId = response.getSessionId();
      if (sessionId <= 0) {
        throw new IoTDBR2dbcException(
            R2dbcMessages.EXCEPTION_THE_SERVER_DID_NOT_RETURN_A_SESSION_ID_F39EF954);
      }
      return new SyncSharedCodecClient(
          transport,
          client,
          configuration,
          sessionId,
          client.requestStatementId(sessionId),
          timeFactor(response));
    } catch (RuntimeException | TException e) {
      transport.close();
      throw e;
    }
  }

  /**
   * Runs one query and returns the sum of one integer column, decoded by the driver codec.
   *
   * <p>Paging mirrors {@link IoTDBQueryCursor}: every response carries the pages that are already
   * available and {@code moreData} asks for the next batch, until a response without a result set
   * ends the query.
   */
  public long sumOfIntegerColumn(String sql, int fetchSize, String column) throws TException {
    TSExecuteStatementResp executed =
        client.executeStatementV2(
            new TSExecuteStatementReq(sessionId, sql, statementId).setFetchSize(fetchSize));
    ExceptionMapper.verifySuccess(executed.getStatus(), sql);
    RowDecoder decoder = RowDecoder.of(executed, configuration.zoneId(), timeFactor);
    long queryId = executed.getQueryId();
    long sum = decodePages(executed.getQueryResult(), decoder, column);
    boolean moreData = executed.isMoreData();
    while (moreData) {
      TSFetchResultsResp fetched =
          client.fetchResultsV2(
              new TSFetchResultsReq(sessionId, sql, fetchSize, queryId, true)
                  .setStatementId(statementId));
      ExceptionMapper.verifySuccess(fetched.getStatus(), sql);
      // Same stop condition as the cursor: a page without a result set ends the query, even when
      // the server still reports more data.
      moreData = fetched.isHasResultSet() && fetched.isMoreData();
      if (fetched.isHasResultSet()) {
        sum += decodePages(fetched.getQueryResult(), decoder, column);
      }
    }
    client.closeOperation(
        new TSCloseOperationReq(sessionId).setStatementId(statementId).setQueryId(queryId));
    return sum;
  }

  private static long decodePages(List<ByteBuffer> pages, RowDecoder decoder, String column) {
    long sum = 0;
    if (pages == null) {
      return 0;
    }
    for (ByteBuffer page : pages) {
      TsBlock block = decoder.decode(page);
      for (int rowIndex = 0; rowIndex < block.getPositionCount(); rowIndex++) {
        sum += decoder.row(block, rowIndex).get(column, Integer.class);
      }
    }
    return sum;
  }

  @Override
  public void close() {
    try {
      client.closeSession(new TSCloseSessionReq(sessionId));
    } catch (TException e) {
      // The benchmark only needs the socket gone; a failed close cannot be recovered here.
    } finally {
      transport.close();
    }
  }

  private static TTransport newTransport(IoTDBConnectionConfiguration configuration)
      throws TTransportException {
    IoTDBConnectionConfiguration.TlsConfiguration tls = configuration.tls();
    TSocket socket;
    if (tls.enabled() && tls.trustStore() != null) {
      TSSLTransportFactory.TSSLTransportParameters parameters =
          new TSSLTransportFactory.TSSLTransportParameters();
      parameters.setTrustStore(tls.trustStore(), tls.trustStorePassword());
      socket =
          TSSLTransportFactory.getClientSocket(
              configuration.host(), configuration.port(), socketTimeout(configuration), parameters);
    } else {
      socket = new TSocket(configuration.host(), configuration.port());
      socket.setTimeout(socketTimeout(configuration));
      socket.setConnectTimeout((int) configuration.connectTimeout().toMillis());
    }
    return new TFramedTransport(socket);
  }

  private static int socketTimeout(IoTDBConnectionConfiguration configuration) {
    return (int) configuration.rpcTimeout().toMillis();
  }

  private static TProtocol protocol(
      TTransport transport, IoTDBConnectionConfiguration configuration) {
    return configuration.protocol() == IoTDBConnectionConfiguration.Protocol.COMPACT
        ? new TCompactProtocol(transport)
        : new TBinaryProtocol(transport);
  }

  private static TSOpenSessionReq openSessionRequest(IoTDBConnectionConfiguration configuration) {
    Map<String, String> connectConfiguration = new HashMap<>();
    connectConfiguration.put("version", CLIENT_VERSION);
    connectConfiguration.put(
        "sql_dialect",
        configuration.sqlDialect() == IoTDBConnectionConfiguration.SqlDialect.TABLE
            ? "table"
            : "tree");
    if (configuration.database() != null) {
      connectConfiguration.put("db", configuration.database());
    }
    return new TSOpenSessionReq()
        .setUsername(configuration.user())
        .setPassword(configuration.password())
        .setZoneId(configuration.zoneId().getId())
        .setConfiguration(connectConfiguration);
  }

  /** Mirrors the driver's mapping of the precision the server reports at handshake time. */
  private static int timeFactor(TSOpenSessionResp response) {
    String precision =
        response.isSetConfiguration()
            ? response.getConfiguration().get(RpcUtils.TIME_PRECISION)
            : null;
    if (PRECISION_MILLISECOND.equals(precision)) {
      return 1_000;
    }
    if (PRECISION_MICROSECOND.equals(precision)) {
      return 1_000_000;
    }
    if (PRECISION_NANOSECOND.equals(precision)) {
      return 1_000_000_000;
    }
    if (precision == null) {
      throw new IoTDBR2dbcException(
          R2dbcMessages.EXCEPTION_THE_SERVER_DID_NOT_REPORT_ITS_TIMESTAMP_PRECISION_730DC973);
    }
    throw new IoTDBR2dbcException(
        String.format(
            R2dbcMessages
                .EXCEPTION_THE_SERVER_REPORTED_AN_UNSUPPORTED_TIMESTAMP_PRECISION_ARG_478A6D95,
            precision));
  }
}
