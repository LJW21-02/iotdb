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

package org.apache.iotdb.r2dbc;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareResp;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Exercises multi-result ordering through real framed Thrift, including asynchronous paging. */
public class IoTDBR2dbcMultiResultTest {

  @Test
  public void batchWaitsForEachQueryAndItsCleanup() throws Exception {
    assertPagedResults(
        connection ->
            connection
                .createBatch()
                .add("SELECT v FROM probe")
                .add("SELECT v FROM probe")
                .execute(),
        false);
  }

  @Test
  public void compoundStatementWaitsForEachQueryAndItsCleanup() throws Exception {
    assertPagedResults(
        connection ->
            connection.createStatement("SELECT v FROM probe; SELECT v FROM probe").execute(),
        false);
  }

  @Test
  public void bindingSetsWaitForEachQueryAndItsCleanupWithOnePrefetchedResult() throws Exception {
    assertPagedResults(
        connection ->
            connection
                .createStatement("SELECT v FROM probe WHERE v > ?")
                .bind(0, 0)
                .add()
                .bind(0, 1)
                .execute(),
        true);
  }

  private static void assertPagedResults(
      Function<Connection, Publisher<? extends Result>> execution, boolean onePrefetchedResult)
      throws Exception {
    try (Node node = new Node()) {
      IoTDBConnectionConfiguration configuration =
          IoTDBConnectionConfiguration.from(
              ConnectionFactoryOptions.builder()
                  .option(ConnectionFactoryOptions.HOST, "127.0.0.1")
                  .option(ConnectionFactoryOptions.PORT, node.port)
                  .option(ConnectionFactoryOptions.USER, "test")
                  .option(ConnectionFactoryOptions.PASSWORD, "test")
                  .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table")
                  .option(IoTDBConnectionFactoryOptions.FETCH_SIZE, 1)
                  .option(IoTDBConnectionFactoryOptions.RPC_TIMEOUT, Duration.ofSeconds(5))
                  .build());
      try (IoTDBR2dbcConnectionFactory factory = new IoTDBR2dbcConnectionFactory(configuration)) {
        Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(5));
        try {
          Flux<? extends Result> results = Flux.from(execution.apply(connection));
          Flux<Integer> rows =
              onePrefetchedResult
                  ? results.concatMap(
                      result -> result.map((row, metadata) -> row.get("v", Integer.class)), 1)
                  : results.concatMap(
                      result -> result.map((row, metadata) -> row.get("v", Integer.class)));
          CompletableFuture<List<Integer>> completion = rows.collectList().toFuture();

          assertTrue(
              "the first query must fetch another page",
              node.firstFetch.await(5, TimeUnit.SECONDS));
          assertEquals(1, node.executions.get());
          assertFalse(
              "the result sequence must wait while the page is in flight", completion.isDone());
          node.allowFirstFetch.countDown();

          assertTrue("the first cursor must be closed", node.firstClose.await(5, TimeUnit.SECONDS));
          assertEquals("the next query must wait for cleanup", 1, node.executions.get());
          assertFalse("cleanup is still pending", completion.isDone());
          node.allowFirstClose.countDown();

          assertEquals(List.of(11, 12, 21, 22), completion.get(5, TimeUnit.SECONDS));
          assertEquals(2, node.executions.get());
          assertEquals(2, node.fetches.get());
          assertEquals(2, node.closes.get());
          assertEquals(
              List.of("execute:1", "fetch:1", "close:1", "execute:2", "fetch:2", "close:2"),
              node.completedOperations);
        } finally {
          node.allowFirstFetch.countDown();
          node.allowFirstClose.countDown();
          Mono.from(connection.close()).block(Duration.ofSeconds(5));
        }
      }
    }
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static ByteBuffer page(int value) throws Exception {
    TsBlockBuilder builder = new TsBlockBuilder(List.of(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(value);
    builder.getColumnBuilder(0).writeInt(value);
    builder.declarePosition();
    return new TsBlockSerde().serialize(builder.build());
  }

  private static final class Node implements AutoCloseable {

    private final AtomicInteger executions = new AtomicInteger();
    private final AtomicInteger fetches = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final CountDownLatch firstFetch = new CountDownLatch(1);
    private final CountDownLatch allowFirstFetch = new CountDownLatch(1);
    private final CountDownLatch firstClose = new CountDownLatch(1);
    private final CountDownLatch allowFirstClose = new CountDownLatch(1);
    private final List<String> completedOperations = new CopyOnWriteArrayList<>();
    private final ServerSocket socket;
    private final TThreadPoolServer server;
    private final ExecutorService executor;
    private final Thread thread;
    private final int port;

    private Node() throws Exception {
      socket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      port = socket.getLocalPort();
      IClientRPCService.Iface handler =
          (IClientRPCService.Iface)
              Proxy.newProxyInstance(
                  IClientRPCService.Iface.class.getClassLoader(),
                  new Class<?>[] {IClientRPCService.Iface.class},
                  (proxy, method, args) -> {
                    switch (method.getName()) {
                      case "openSession":
                        return new TSOpenSessionResp()
                            .setStatus(success())
                            .setSessionId(1)
                            .setConfiguration(Map.of("timestamp_precision", "ms"));
                      case "requestStatementId":
                        return 1L;
                      case "getProperties":
                        return new ServerProperties()
                            .setVersion("test")
                            .setSupportedTimeAggregationOperations(List.of())
                            .setTimestampPrecision("ms")
                            .setThriftMaxFrameSize(24 * 1024 * 1024);
                      case "prepareStatement":
                        return new TSPrepareResp().setStatus(success()).setParameterCount(1);
                      case "executeStatementV2":
                      case "executePreparedStatement":
                        int id = executions.incrementAndGet();
                        completedOperations.add("execute:" + id);
                        return new TSExecuteStatementResp()
                            .setStatus(success())
                            .setQueryId(id)
                            .setColumns(List.of("v"))
                            .setDataTypeList(List.of("INT32"))
                            .setIgnoreTimeStamp(true)
                            .setQueryResult(List.of(page(id * 10 + 1)))
                            .setMoreData(true);
                      case "fetchResultsV2":
                        long fetchedQuery = ((TSFetchResultsReq) args[0]).getQueryId();
                        fetches.incrementAndGet();
                        if (fetchedQuery == 1) {
                          firstFetch.countDown();
                          if (!allowFirstFetch.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException(
                                "Timed out waiting to return a query page");
                          }
                        }
                        completedOperations.add("fetch:" + fetchedQuery);
                        return new TSFetchResultsResp(success(), true, true)
                            .setQueryResult(List.of(page((int) fetchedQuery * 10 + 2)))
                            .setMoreData(false);
                      case "closeOperation":
                        long closedQuery = ((TSCloseOperationReq) args[0]).getQueryId();
                        closes.incrementAndGet();
                        if (closedQuery == 1) {
                          firstClose.countDown();
                          if (!allowFirstClose.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Timed out waiting to release a query");
                          }
                        }
                        completedOperations.add("close:" + closedQuery);
                        return success();
                      case "closeSession":
                      case "deallocatePreparedStatement":
                        return success();
                      case "toString":
                        return "IoTDBR2dbcMultiResultTestNode";
                      default:
                        throw new UnsupportedOperationException(method.getName());
                    }
                  });
      executor =
          Executors.newCachedThreadPool(
              runnable -> {
                Thread worker = new Thread(runnable, "r2dbc-multi-result-test-worker");
                worker.setDaemon(true);
                return worker;
              });
      server =
          new TThreadPoolServer(
              new TThreadPoolServer.Args(new TServerSocket(socket))
                  .processor(new IClientRPCService.Processor<>(handler))
                  .transportFactory(new TFramedTransport.Factory())
                  .protocolFactory(new TBinaryProtocol.Factory())
                  .executorService(executor));
      thread = new Thread(server::serve, "r2dbc-multi-result-test-server");
      thread.setDaemon(true);
      thread.start();
      for (int attempts = 0; attempts < 200 && !server.isServing(); attempts++) {
        Thread.sleep(5);
      }
      if (!server.isServing()) {
        throw new IllegalStateException("Stub server failed to start");
      }
    }

    @Override
    public void close() throws Exception {
      allowFirstFetch.countDown();
      allowFirstClose.countDown();
      server.stop();
      socket.close();
      executor.shutdownNow();
      thread.join(1_000);
    }
  }
}
