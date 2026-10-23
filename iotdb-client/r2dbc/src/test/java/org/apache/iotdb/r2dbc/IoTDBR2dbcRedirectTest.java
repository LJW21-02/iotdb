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
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.ValidationDepth;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
  public void parameterValidationFailureDoesNotKeepTheConnectionBusy() throws Exception {
    try (Node node = new Node();
        ConnectionContext context = connect(node, "tree", Duration.ZERO)) {
      Connection connection = context.connection;
      try {
        assertThrows(
            RuntimeException.class,
            () ->
                Mono.from(
                        connection
                            .createStatement("SELECT ?")
                            .bind(0, Instant.ofEpochSecond(0, 1))
                            .execute())
                    .block(Duration.ofSeconds(5)));

        Mono.from(connection.createStatement("SELECT 1").execute()).block(Duration.ofSeconds(5));
        assertEquals(1, node.statementExecutions.get());
      } finally {
        Mono.from(connection.close()).block(Duration.ofSeconds(5));
      }
    }
  }

  @Test
  public void configuredStatementTimeoutAbortsTheInFlightExecution() throws Exception {
    try (Node node = new Node();
        ConnectionContext context = connect(node, "table", Duration.ofMillis(250))) {
      node.statementReceived = new CountDownLatch(1);
      node.allowStatementResponse = new CountDownLatch(1);
      Connection connection = context.connection;
      try {
        assertThrows(
            R2dbcTimeoutException.class,
            () ->
                Mono.from(
                        connection
                            .createStatement("INSERT INTO probe(time,device,v) VALUES (1,'d',1)")
                            .execute())
                    .block(Duration.ofSeconds(3)));
        assertTrue(node.statementReceived.await(5, java.util.concurrent.TimeUnit.SECONDS));
        assertFalse(Mono.from(connection.validate(ValidationDepth.LOCAL)).block());
      } finally {
        node.allowStatementResponse.countDown();
        Mono.from(connection.close()).block(Duration.ofSeconds(5));
      }
    }
  }

  @Test
  public void prepareValidationFailureDoesNotSendExecuteRpc() throws Exception {
    try (Node node = new Node();
        ConnectionContext context = connect(node, "table", Duration.ZERO)) {
      node.prepareParameterCount = 0;
      Connection connection = context.connection;
      try {
        assertThrows(
            RuntimeException.class,
            () ->
                Mono.from(
                        connection
                            .createStatement("SELECT v FROM probe WHERE v = ?")
                            .bind(0, 1)
                            .execute())
                    .block(Duration.ofSeconds(5)));

        assertEquals(1, node.prepares.get());
        assertEquals(0, node.preparedExecutions.get());
        Mono.from(connection.createStatement("SELECT 1").execute()).block(Duration.ofSeconds(5));
        assertEquals(1, node.statementExecutions.get());
      } finally {
        Mono.from(connection.close()).block(Duration.ofSeconds(5));
      }
    }
  }

  @Test
  public void initialResultIsDeliveredOnTheCallbackScheduler() throws Exception {
    try (Node node = new Node();
        ConnectionContext context = connect(node, "table", Duration.ZERO)) {
      Connection connection = context.connection;
      try {
        java.util.concurrent.atomic.AtomicReference<String> callbackThread =
            new java.util.concurrent.atomic.AtomicReference<>();
        Mono.from(
                connection
                    .createStatement("INSERT INTO probe(time,device,v) VALUES (1,'d',1)")
                    .execute())
            .doOnNext(ignored -> callbackThread.set(Thread.currentThread().getName()))
            .block(Duration.ofSeconds(5));

        org.junit.Assert.assertNotNull(callbackThread.get());
        assertTrue(callbackThread.get().startsWith("iotdb-r2dbc-callbacks"));
      } finally {
        Mono.from(connection.close()).block(Duration.ofSeconds(5));
      }
    }
  }

  @Test
  public void blockedResultCallbackDoesNotBlockAnotherConnectionsRpc() throws Exception {
    try (Node node = new Node()) {
      IoTDBConnectionConfiguration configuration = configuration(node, 3);
      try (IoTDBR2dbcConnectionFactory factory = new IoTDBR2dbcConnectionFactory(configuration)) {
        Connection first = Mono.from(factory.create()).block(Duration.ofSeconds(5));
        Connection second = Mono.from(factory.create()).block(Duration.ofSeconds(5));
        CountDownLatch callbackStarted = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        CountDownLatch callbackFinished = new CountDownLatch(1);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        Disposable blockedCallback =
            Mono.from(
                    first
                        .createStatement("INSERT INTO probe(time,device,v) VALUES (1,'first',1)")
                        .execute())
                .doOnNext(
                    ignored -> {
                      callbackStarted.countDown();
                      try {
                        if (!releaseCallback.await(5, java.util.concurrent.TimeUnit.SECONDS)) {
                          throw new IllegalStateException("The test did not release its callback");
                        }
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                      }
                    })
                .doOnError(callbackFailure::set)
                .doFinally(signal -> callbackFinished.countDown())
                .subscribe();
        try {
          assertTrue(callbackStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));
          Mono.from(
                  second
                      .createStatement("INSERT INTO probe(time,device,v) VALUES (2,'second',2)")
                      .execute())
              .block(Duration.ofSeconds(2));
          assertEquals(2, node.statementExecutions.get());
        } finally {
          releaseCallback.countDown();
          assertTrue(callbackFinished.await(5, java.util.concurrent.TimeUnit.SECONDS));
          blockedCallback.dispose();
          Mono.from(first.close()).block(Duration.ofSeconds(5));
          Mono.from(second.close()).block(Duration.ofSeconds(5));
        }
        assertEquals(null, callbackFailure.get());
      }
    }
  }

  private static ConnectionContext connect(Node node, String dialect, Duration statementTimeout) {
    IoTDBConnectionConfiguration configuration =
        IoTDBConnectionConfiguration.from(
            ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.HOST, "127.0.0.1")
                .option(ConnectionFactoryOptions.PORT, node.port)
                .option(ConnectionFactoryOptions.USER, "test")
                .option(ConnectionFactoryOptions.PASSWORD, "test")
                .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, dialect)
                .option(ConnectionFactoryOptions.STATEMENT_TIMEOUT, statementTimeout)
                .option(IoTDBConnectionFactoryOptions.RPC_TIMEOUT, Duration.ofSeconds(5))
                .build());
    IoTDBR2dbcConnectionFactory factory = new IoTDBR2dbcConnectionFactory(configuration);
    try {
      return new ConnectionContext(
          factory, Mono.from(factory.create()).block(Duration.ofSeconds(5)));
    } catch (RuntimeException error) {
      factory.close();
      throw error;
    }
  }

  private static final class ConnectionContext implements AutoCloseable {

    private final IoTDBR2dbcConnectionFactory factory;
    private final Connection connection;

    private ConnectionContext(IoTDBR2dbcConnectionFactory factory, Connection connection) {
      this.factory = factory;
      this.connection = connection;
    }

    @Override
    public void close() {
      try {
        Mono.from(connection.close()).block(Duration.ofSeconds(5));
      } finally {
        factory.close();
      }
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

  @Test
  public void cancelingAnUndeliveredConnectionClosesItsSocketBeforeTheHandshakeResponds()
      throws Exception {
    try (Node node = new Node()) {
      node.openSessionReceived = new CountDownLatch(1);
      node.allowOpenSessionResponse = new CountDownLatch(1);
      IoTDBConnectionConfiguration configuration = configuration(node, 3);
      try (IoTDBR2dbcConnectionFactory factory = new IoTDBR2dbcConnectionFactory(configuration)) {
        AtomicInteger delivered = new AtomicInteger();
        Disposable connection =
            Mono.from(factory.create())
                .doOnNext(ignored -> delivered.incrementAndGet())
                .subscribe();
        assertTrue(node.openSessionReceived.await(5, java.util.concurrent.TimeUnit.SECONDS));
        Socket peer = node.awaitAcceptedSocket();
        try {
          connection.dispose();
          assertPeerClosed(peer);
        } finally {
          node.allowOpenSessionResponse.countDown();
        }

        assertTrue(node.openSessionReturned.await(5, java.util.concurrent.TimeUnit.SECONDS));
        assertEquals(0, delivered.get());
        assertEquals(0, node.statementIdRequests.get());
        assertEquals(0, node.propertiesRequests.get());
      }
    }
  }

  @Test
  public void cancelingFromConnectionDeliveryKeepsTheDeliveredConnectionOpen() throws Exception {
    try (Node node = new Node()) {
      IoTDBConnectionConfiguration configuration = configuration(node, 3);
      try (IoTDBR2dbcConnectionFactory factory = new IoTDBR2dbcConnectionFactory(configuration)) {
        AtomicReference<Connection> delivered = new AtomicReference<>();
        CountDownLatch receivedConnection = new CountDownLatch(1);
        Mono.from(factory.create())
            .subscribe(
                new BaseSubscriber<Connection>() {
                  @Override
                  protected void hookOnNext(Connection connection) {
                    delivered.set(connection);
                    cancel();
                    receivedConnection.countDown();
                  }
                });

        assertTrue(receivedConnection.await(5, java.util.concurrent.TimeUnit.SECONDS));
        Connection connection = delivered.get();
        try {
          assertTrue(
              "cancellation from onNext must not close a connection already handed to the caller",
              Mono.from(connection.validate(ValidationDepth.LOCAL)).block(Duration.ofSeconds(5)));
          Mono.from(connection.createStatement("SELECT 1").execute()).block(Duration.ofSeconds(5));
          assertEquals(1, node.statementExecutions.get());
        } finally {
          Mono.from(connection.close()).block(Duration.ofSeconds(5));
        }
      }
    }
  }

  private static IoTDBConnectionConfiguration configuration(Node node, int redirects) {
    return IoTDBConnectionConfiguration.from(
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.HOST, "127.0.0.1")
            .option(ConnectionFactoryOptions.PORT, node.port)
            .option(ConnectionFactoryOptions.USER, "test")
            .option(ConnectionFactoryOptions.PASSWORD, "test")
            .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "tree")
            .option(IoTDBConnectionFactoryOptions.MAX_REDIRECTS, redirects)
            .option(IoTDBConnectionFactoryOptions.RPC_TIMEOUT, Duration.ofSeconds(3))
            .build());
  }

  private static void assertPeerClosed(Socket peer) throws Exception {
    peer.setSoTimeout(2_000);
    assertEquals(
        "cancellation must close the transport before a late RPC response",
        -1,
        peer.getInputStream().read());
  }

  private static final class ObservedServerSocket extends TServerSocket {
    private final BlockingQueue<Socket> acceptedSockets;

    private ObservedServerSocket(ServerSocket serverSocket, BlockingQueue<Socket> acceptedSockets)
        throws org.apache.thrift.transport.TTransportException {
      super(serverSocket);
      this.acceptedSockets = acceptedSockets;
    }

    @Override
    public TSocket accept() throws org.apache.thrift.transport.TTransportException {
      TSocket accepted = super.accept();
      acceptedSockets.add(accepted.getSocket());
      return accepted;
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
    private final BlockingQueue<Socket> acceptedSockets = new LinkedBlockingQueue<>();
    private final AtomicInteger sessions = new AtomicInteger();
    private final AtomicInteger statementIdRequests = new AtomicInteger();
    private final AtomicInteger propertiesRequests = new AtomicInteger();
    private final AtomicInteger statementExecutions = new AtomicInteger();
    private final AtomicInteger preparedExecutions = new AtomicInteger();
    private final AtomicInteger prepares = new AtomicInteger();
    private volatile CountDownLatch statementReceived;
    private volatile CountDownLatch allowStatementResponse;
    private volatile CountDownLatch openSessionReceived;
    private volatile CountDownLatch allowOpenSessionResponse;
    private final CountDownLatch openSessionReturned = new CountDownLatch(1);
    private volatile int prepareParameterCount = 1;
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
                        CountDownLatch openReceived = openSessionReceived;
                        if (openReceived != null) {
                          openReceived.countDown();
                        }
                        CountDownLatch openGate = allowOpenSessionResponse;
                        if (openGate != null) {
                          openGate.await();
                        }
                        openSessionReturned.countDown();
                        return new TSOpenSessionResp()
                            .setStatus(success())
                            .setSessionId(1)
                            .setConfiguration(Map.of("timestamp_precision", "ms"));
                      case "requestStatementId":
                        statementIdRequests.incrementAndGet();
                        return 1L;
                      case "getProperties":
                        propertiesRequests.incrementAndGet();
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
                        return new TSPrepareResp()
                            .setStatus(success())
                            .setParameterCount(prepareParameterCount);
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
              new TThreadPoolServer.Args(new ObservedServerSocket(socket, acceptedSockets))
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

    private Socket awaitAcceptedSocket() throws InterruptedException {
      Socket accepted = acceptedSockets.poll(5, java.util.concurrent.TimeUnit.SECONDS);
      if (accepted == null) {
        throw new AssertionError("client socket was not accepted");
      }
      return accepted;
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
