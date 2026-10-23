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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;
import org.apache.iotdb.service.rpc.thrift.ServerProperties;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareResp;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ValidationDepth;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Execution responses may carry post-success routing metadata and must never replay a statement.
 */
public class IoTDBR2dbcRedirectTest {

  @Test
  public void tableWriteWithOneRecommendationDoesNotRequireRedirectBudget() throws Exception {
    TSStatus recommendation = tableRecommendation(false);

    assertExecutedOnce(
        "table", 0, "INSERT INTO probe(time,device,v) VALUES (1,'d',1)", false, recommendation);
  }

  @Test
  public void tableWriteWithSeveralRecommendationsIsNotReportedAsPartialFailure() throws Exception {
    TSStatus recommendation = tableRecommendation(true);

    assertExecutedOnce(
        "table", 0, "INSERT INTO probe(time,device,v) VALUES (1,'d',1)", false, recommendation);
  }

  @Test
  public void treeSuccessWithARecommendedNodeDoesNotReplayTheWrite() throws Exception {
    try (Node target = new Node();
        Node entry = new Node()) {
      entry.statementResponse =
          () -> success().setRedirectNode(new TEndPoint("127.0.0.1", target.port));

      executeAndAssertOnce(
          entry, target, "tree", 3, "INSERT INTO root.probe.d(time,v) VALUES (1,1)", false);
    }
  }

  @Test
  public void anUnprovenDirectRecommendationIsReportedWithoutReplay() throws Exception {
    try (Node target = new Node();
        Node entry = new Node()) {
      entry.statementResponse =
          () ->
              new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
                  .setRedirectNode(target.endpoint());

      RuntimeException failure =
          execute(entry, "table", 3, "INSERT INTO probe(time,device,v) VALUES (1,'d',1)", false);

      assertEquals(1, entry.statementExecutions.get());
      assertEquals(0, target.statementExecutions.get());
      assertEquals(1, entry.sessions.get());
      assertEquals(0, target.sessions.get());
      org.junit.Assert.assertNotNull(failure);
      assertEquals(
          TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(),
          ((io.r2dbc.spi.R2dbcException) failure).getErrorCode());
    }
  }

  @Test
  public void preparedExecutionWithARecommendationIsNotPreparedOrExecutedAgain() throws Exception {
    try (Node target = new Node();
        Node entry = new Node()) {
      entry.preparedResponse = () -> tableRecommendation(true);

      executeAndAssertOnce(entry, target, "table", 3, "SELECT v FROM probe WHERE v = ?", true);
      assertEquals(1, entry.prepares.get());
      assertEquals(0, target.prepares.get());
    }
  }

  @Test
  public void anActualExecutionFailureIsStillReportedWithoutRetry() throws Exception {
    try (Node target = new Node();
        Node entry = new Node()) {
      entry.statementResponse =
          () -> new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode()).setMessage("bad SQL");

      RuntimeException failure =
          execute(entry, "table", 3, "INSERT INTO probe(time,device,v) VALUES (1,'d',1)", false);

      assertEquals(1, entry.statementExecutions.get());
      assertEquals(0, target.statementExecutions.get());
      assertEquals(1, entry.sessions.get());
      assertEquals(0, target.sessions.get());
      org.junit.Assert.assertNotNull(failure);
      org.junit.Assert.assertTrue(failure.getMessage().contains("bad SQL"));
    }
  }

  @Test
  public void cancelingAnExecutionWithUnknownOutcomeAbortsTheConnection() throws Exception {
    try (Node entry = new Node()) {
      entry.statementReceived = new CountDownLatch(1);
      entry.allowStatementResponse = new CountDownLatch(1);
      IoTDBConnectionConfiguration configuration =
          IoTDBConnectionConfiguration.from(
              ConnectionFactoryOptions.builder()
                  .option(ConnectionFactoryOptions.HOST, "127.0.0.1")
                  .option(ConnectionFactoryOptions.PORT, entry.port)
                  .option(ConnectionFactoryOptions.USER, "test")
                  .option(ConnectionFactoryOptions.PASSWORD, "test")
                  .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table")
                  .option(IoTDBConnectionFactoryOptions.RPC_TIMEOUT, Duration.ofSeconds(3))
                  .build());
      try (IoTDBR2dbcConnectionFactory factory = new IoTDBR2dbcConnectionFactory(configuration)) {
        Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(5));
        try {
          Disposable execution =
              Mono.from(
                      connection
                          .createStatement("INSERT INTO probe(time,device,v) VALUES (1,'d',1)")
                          .execute())
                  .subscribe();
          assertTrue(entry.statementReceived.await(5, java.util.concurrent.TimeUnit.SECONDS));

          execution.dispose();

          assertFalse(
              Mono.from(connection.validate(ValidationDepth.LOCAL)).block(Duration.ofSeconds(5)));
          assertThrows(IllegalStateException.class, () -> connection.createStatement("SELECT 1"));
        } finally {
          entry.allowStatementResponse.countDown();
          Mono.from(connection.close()).block(Duration.ofSeconds(5));
        }
      }
    }
  }

  private static TSStatus tableRecommendation(boolean multipleTargets) {
    List<TSStatus> subStatuses =
        multipleTargets
            ? Arrays.asList(
                success().setRedirectNode(new TEndPoint("127.0.0.1", 6668)),
                success().setRedirectNode(new TEndPoint("127.0.0.1", 6669)))
            : List.of(success().setRedirectNode(new TEndPoint("127.0.0.1", 6668)));
    return new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
        .setSubStatus(subStatuses);
  }

  private static void assertExecutedOnce(
      String dialect, int budget, String sql, boolean bind, TSStatus response) throws Exception {
    try (Node target = new Node();
        Node entry = new Node()) {
      if (response.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        response.getSubStatus().get(0).setRedirectNode(target.endpoint());
      } else {
        response.setRedirectNode(target.endpoint());
      }
      entry.statementResponse = () -> response;
      executeAndAssertOnce(entry, target, dialect, budget, sql, bind);
    }
  }

  private static void executeAndAssertOnce(
      Node entry, Node target, String dialect, int budget, String sql, boolean bind) {
    RuntimeException failure = execute(entry, dialect, budget, sql, bind);

    assertNull("the successful execution response must be returned", failure);
    assertEquals(1, entry.statementExecutions.get() + entry.preparedExecutions.get());
    assertEquals(0, target.statementExecutions.get() + target.preparedExecutions.get());
    assertEquals(1, entry.sessions.get());
    assertEquals(0, target.sessions.get());
  }

  private static RuntimeException execute(
      Node entry, String dialect, int budget, String sql, boolean bind) {
    IoTDBConnectionConfiguration configuration =
        IoTDBConnectionConfiguration.from(
            ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.HOST, "127.0.0.1")
                .option(ConnectionFactoryOptions.PORT, entry.port)
                .option(ConnectionFactoryOptions.USER, "test")
                .option(ConnectionFactoryOptions.PASSWORD, "test")
                .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, dialect)
                .option(IoTDBConnectionFactoryOptions.MAX_REDIRECTS, budget)
                .option(IoTDBConnectionFactoryOptions.RPC_TIMEOUT, Duration.ofSeconds(3))
                .build());
    try (IoTDBR2dbcConnectionFactory factory = new IoTDBR2dbcConnectionFactory(configuration)) {
      Connection connection = Mono.from(factory.create()).block(Duration.ofSeconds(5));
      try {
        io.r2dbc.spi.Statement statement = connection.createStatement(sql);
        if (bind) {
          statement.bind(0, 1);
        }
        try {
          Mono.from(statement.execute()).block(Duration.ofSeconds(5));
          assertTrue(
              "taking the first completed result must keep the connection usable",
              Mono.from(connection.validate(ValidationDepth.LOCAL)).block(Duration.ofSeconds(5)));
          return null;
        } catch (RuntimeException failure) {
          return failure;
        }
      } finally {
        Mono.from(connection.close()).block(Duration.ofSeconds(5));
      }
    }
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static final class Node implements AutoCloseable {

    private final ServerSocket socket;
    private final TThreadPoolServer server;
    private final ExecutorService executor;
    private final Thread thread;
    private final int port;
    private final AtomicInteger sessions = new AtomicInteger();
    private final AtomicInteger statementExecutions = new AtomicInteger();
    private final AtomicInteger preparedExecutions = new AtomicInteger();
    private final AtomicInteger prepares = new AtomicInteger();
    private volatile CountDownLatch statementReceived;
    private volatile CountDownLatch allowStatementResponse;
    private volatile Supplier<TSStatus> statementResponse = IoTDBR2dbcRedirectTest::success;
    private volatile Supplier<TSStatus> preparedResponse = IoTDBR2dbcRedirectTest::success;

    private Node() throws Exception {
      socket = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      port = socket.getLocalPort();
      IClientRPCService.Iface handler =
          (IClientRPCService.Iface)
              java.lang.reflect.Proxy.newProxyInstance(
                  IClientRPCService.Iface.class.getClassLoader(),
                  new Class<?>[] {IClientRPCService.Iface.class},
                  (proxy, method, args) -> {
                    switch (method.getName()) {
                      case "openSession":
                        sessions.incrementAndGet();
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
                      case "executeStatementV2":
                        statementExecutions.incrementAndGet();
                        CountDownLatch received = statementReceived;
                        if (received != null) {
                          received.countDown();
                        }
                        CountDownLatch responseGate = allowStatementResponse;
                        if (responseGate != null) {
                          responseGate.await();
                        }
                        return new TSExecuteStatementResp().setStatus(statementResponse.get());
                      case "prepareStatement":
                        prepares.incrementAndGet();
                        return new TSPrepareResp().setStatus(success()).setParameterCount(1);
                      case "executePreparedStatement":
                        preparedExecutions.incrementAndGet();
                        return new TSExecuteStatementResp().setStatus(preparedResponse.get());
                      case "closeSession":
                      case "closeOperation":
                      case "deallocatePreparedStatement":
                        return success();
                      case "toString":
                        return "IoTDBR2dbcRedirectTestNode";
                      default:
                        throw new UnsupportedOperationException(method.getName());
                    }
                  });
      executor =
          Executors.newCachedThreadPool(
              runnable -> {
                Thread worker = new Thread(runnable, "r2dbc-redirect-test-worker");
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
      thread = new Thread(server::serve, "r2dbc-redirect-test-server");
      thread.setDaemon(true);
      thread.start();
      for (int attempts = 0; attempts < 200 && !server.isServing(); attempts++) {
        Thread.sleep(5);
      }
      if (!server.isServing()) {
        throw new IllegalStateException("Stub server failed to start");
      }
    }

    private TEndPoint endpoint() {
      return new TEndPoint("127.0.0.1", port);
    }

    @Override
    public void close() throws Exception {
      server.stop();
      socket.close();
      executor.shutdownNow();
      thread.join(1_000);
    }
  }
}
