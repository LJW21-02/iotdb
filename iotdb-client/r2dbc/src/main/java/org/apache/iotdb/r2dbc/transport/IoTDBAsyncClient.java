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

package org.apache.iotdb.r2dbc.transport;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration;
import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSDeallocatePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareReq;
import org.apache.iotdb.service.rpc.thrift.TSPrepareResp;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Low level non-blocking client for the DataNode RPC service.
 *
 * <p>One instance owns one socket and allows one outstanding RPC, which mirrors the protocol: the
 * generated async client keeps exactly one current method per transport, and a single Thrift
 * selector thread serves every connection of the same {@link TAsyncClientManager}. The generated
 * async client has no frame size check, therefore {@link FrameGuardSocket} validates response
 * frames before Thrift allocates memory for them.
 */
public final class IoTDBAsyncClient implements AutoCloseable {

  private final TNonblockingTransport socket;
  private final FrameLimitedTransport frameLimit;
  private final IClientRPCService.AsyncClient client;
  private final AtomicBoolean inFlight = new AtomicBoolean();

  @FunctionalInterface
  private interface Rpc<T> {
    void send(AsyncMethodCallback<T> callback) throws TException;
  }

  public IoTDBAsyncClient(
      IoTDBConnectionConfiguration configuration, TAsyncClientManager clientManager) {
    if (configuration.tls().enabled()) {
      this.socket = TlsSocketFactory.create(configuration);
    } else {
      try {
        this.socket =
            new FrameGuardSocket(
                configuration.host(), configuration.port(), configuration.maxFrameSize());
      } catch (IOException | TTransportException e) {
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages.EXCEPTION_FAILED_TO_CONNECT_TO_IOTDB_AT_ARG_ARG_2A934FC1,
                configuration.host(),
                configuration.port()),
            e);
      }
    }
    this.frameLimit = (FrameLimitedTransport) socket;
    TProtocolFactory protocolFactory =
        configuration.protocol() == IoTDBConnectionConfiguration.Protocol.COMPACT
            ? new TCompactProtocol.Factory()
            : new TBinaryProtocol.Factory();
    this.client = new IClientRPCService.AsyncClient(protocolFactory, clientManager, socket);
    this.client.setTimeout(rpcTimeoutMillis(configuration));
  }

  public CompletableFuture<TSOpenSessionResp> openSession(TSOpenSessionReq request) {
    return call(callback -> client.openSession(request, callback));
  }

  public CompletableFuture<Long> requestStatementId(long sessionId) {
    return call(callback -> client.requestStatementId(sessionId, callback));
  }

  public CompletableFuture<ServerProperties> getProperties() {
    return call(client::getProperties);
  }

  public CompletableFuture<TSExecuteStatementResp> executeStatement(TSExecuteStatementReq request) {
    return call(callback -> client.executeStatementV2(request, callback));
  }

  public CompletableFuture<TSFetchResultsResp> fetchResults(TSFetchResultsReq request) {
    return call(callback -> client.fetchResultsV2(request, callback));
  }

  public CompletableFuture<TSStatus> closeOperation(TSCloseOperationReq request) {
    return call(callback -> client.closeOperation(request, callback));
  }

  public CompletableFuture<TSStatus> closeSession(TSCloseSessionReq request) {
    return call(callback -> client.closeSession(request, callback));
  }

  public CompletableFuture<TSPrepareResp> prepareStatement(TSPrepareReq request) {
    return call(callback -> client.prepareStatement(request, callback));
  }

  public CompletableFuture<TSExecuteStatementResp> executePreparedStatement(
      TSExecutePreparedReq request) {
    return call(callback -> client.executePreparedStatement(request, callback));
  }

  public CompletableFuture<TSStatus> deallocatePreparedStatement(TSDeallocatePreparedReq request) {
    return call(callback -> client.deallocatePreparedStatement(request, callback));
  }

  /** Applies the stricter of the configured and the server announced frame limit. */
  public void limitFrameSize(int serverLimit) {
    frameLimit.limitFrameSize(serverLimit);
  }

  @Override
  public void close() {
    socket.close();
  }

  private <T> CompletableFuture<T> call(Rpc<T> rpc) {
    CompletableFuture<T> future = new CompletableFuture<>();
    if (!inFlight.compareAndSet(false, true)) {
      future.completeExceptionally(
          new IoTDBR2dbcException(
              R2dbcMessages
                  .EXCEPTION_THE_CONNECTION_CAN_ONLY_RUN_ONE_STATEMENT_AT_A_TIME_18CE7BB2));
      return future;
    }
    try {
      rpc.send(
          new AsyncMethodCallback<T>() {
            @Override
            public void onComplete(T response) {
              inFlight.set(false);
              future.complete(response);
            }

            @Override
            public void onError(Exception error) {
              inFlight.set(false);
              future.completeExceptionally(error);
            }
          });
    } catch (Exception e) {
      inFlight.set(false);
      future.completeExceptionally(e);
    }
    return future;
  }

  private static int rpcTimeoutMillis(IoTDBConnectionConfiguration configuration) {
    long millis = configuration.rpcTimeout().toMillis();
    return millis > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) millis;
  }
}
