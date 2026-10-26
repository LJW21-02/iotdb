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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Soak checks against a live instance: repeated prepared executions, many pages, a demand driven
 * consumer, parallel connections sharing one runtime and repeated connect/close cycles. Skipped
 * unless {@code -Diotdb.it.host} is given, like {@link IoTDBR2dbcIT}.
 */
public class IoTDBR2dbcSoakIT {

  private static final String DATABASE = "r2dbc_soak";
  private static final int ROWS = 300;

  /** Far more rows than one server side page holds, for the abandoned result case. */
  private static final int ABANDON_ROWS = 20_000;

  private static final int ABANDON_BATCH = 1_000;

  @BeforeClass
  public static void requireServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @Test
  public void repeatedPreparedExecutionsDoNotLeakServerHandles() {
    ConnectionFactory factory = factory();
    Connection connection = Mono.from(factory.create()).block();
    try {
      provision(connection);
      // Every iteration prepares, executes and deallocates; a leaked handle would make the next
      // prepare fail with "Prepared statement ... already exists".
      for (int i = 1; i <= 150; i++) {
        Statement bound = connection.createStatement(selectValueText());
        bound.bind(0, (i % ROWS) + 1);
        List<Row> rows = query(bound);
        assertEquals("iteration " + i, 1, rows.size());
        assertEquals(Integer.valueOf((i % ROWS) + 1), rows.get(0).get("v", Integer.class));
      }
      // A discarded result must still release its cursor and keep the connection usable.
      for (int i = 0; i < 10; i++) {
        assertTrue(discardedResultIsEmpty(connection));
      }
      assertEquals(ROWS, query(connection.createStatement(selectAllText())).size());
      assertTrue(Mono.from(connection.validate(ValidationDepth.LOCAL)).block());
    } finally {
      Mono.from(connection.close()).block();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void pagesThroughManyFetchesWithoutLosingOrRepeatingRows() {
    ConnectionFactory factory = factory();
    Connection connection = Mono.from(factory.create()).block();
    try {
      provision(connection);
      List<Row> rows = query(connection.createStatement(selectAllText()).fetchSize(7));
      assertEquals(ROWS, rows.size());
      long sum = 0;
      long previousTime = Long.MIN_VALUE;
      for (Row row : rows) {
        sum += row.get("v", Integer.class);
        long time = row.get("time", Long.class);
        assertTrue("rows must stay ordered across page boundaries", time > previousTime);
        previousTime = time;
      }
      assertEquals((long) ROWS * (ROWS + 1) / 2, sum);
    } finally {
      Mono.from(connection.close()).block();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void slowConsumerReceivesRowsOnDemandAcrossPages() {
    ConnectionFactory factory = factory();
    Connection connection = Mono.from(factory.create()).block();
    try {
      provision(connection);
      Flux<Row> rows =
          Flux.from(connection.createStatement(selectAllText()).fetchSize(5).execute())
              .flatMap(result -> result.map((row, metadata) -> row));
      StepVerifier.create(rows, 0)
          .thenRequest(1)
          .expectNextCount(1)
          .thenRequest(10)
          .expectNextCount(10)
          .thenRequest(ROWS)
          .expectNextCount(ROWS - 11)
          .verifyComplete();
    } finally {
      Mono.from(connection.close()).block();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void parallelConnectionsShareOneSelectorRuntime() {
    ConnectionFactory factory = factory();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Connection> connections = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        connections.add(Mono.from(factory.create()).block());
      }
      provision(connections.get(0));
      List<Callable<Integer>> tasks = new ArrayList<>();
      for (Connection connection : connections) {
        tasks.add(
            () -> {
              int rows = 0;
              for (int i = 0; i < 25; i++) {
                rows += query(connection.createStatement(selectAllText())).size();
              }
              return rows;
            });
      }
      List<Future<Integer>> results = executor.invokeAll(tasks);
      for (Future<Integer> result : results) {
        assertEquals(25 * ROWS, result.get().intValue());
      }
      for (Connection connection : connections) {
        assertTrue(Mono.from(connection.validate(ValidationDepth.LOCAL)).block());
        Mono.from(connection.close()).block();
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    } finally {
      executor.shutdownNow();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void connectCloseCyclesReuseOneSelectorThread() throws InterruptedException {
    long selectorsBefore = selectorThreadCount();
    for (int i = 0; i < 20; i++) {
      ConnectionFactory factory = factory();
      Connection connection = Mono.from(factory.create()).block();
      try {
        assertEquals(ROWS, query(connection.createStatement(selectAllText())).size());
      } finally {
        Mono.from(connection.close()).block();
        ((IoTDBR2dbcConnectionFactory) factory).close();
      }
    }
    // Closing a factory stops its selector thread asynchronously.
    long deadline = System.currentTimeMillis() + 10_000;
    while (selectorThreadCount() > selectorsBefore && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
    assertEquals(
        "closed factories must release their selector thread",
        selectorsBefore,
        selectorThreadCount());
  }

  /**
   * A server that stops answering without closing its socket is the case the RPC timeout exists
   * for. The driver must fail the statement with a timeout instead of waiting forever, and
   * afterwards the connection must be reported as broken rather than being reused silently.
   */
  @Test
  public void unresponsiveServerFailsWithinTheRpcTimeout() throws Exception {
    Path stallFile = Files.createTempFile("r2dbc-stall", ".flag");
    Files.delete(stallFile);
    int proxyPort = freePort();
    Process proxy = startStallProxy(stallFile, proxyPort);
    ConnectionFactory factory = factory(proxyPort, Duration.ofSeconds(3));
    try {
      awaitPort(proxyPort);
      Connection connection = Mono.from(factory.create()).block();
      try {
        provision(connection);
        assertEquals(ROWS, query(connection.createStatement(selectAllText())).size());

        // The socket stays open but nothing is answered from here on.
        Files.createFile(stallFile);
        long start = System.nanoTime();
        Throwable failure =
            assertThrows(
                Throwable.class,
                () ->
                    Mono.from(connection.createStatement(selectAllText()).execute())
                        .timeout(Duration.ofSeconds(30))
                        .block());
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
        assertTrue(
            "the driver must fail on its own timeout, took " + elapsedMillis + " ms",
            elapsedMillis < 20_000);
        assertTrue(
            "a timeout must be reported as R2dbcTimeoutException, was " + failure,
            failure instanceof R2dbcTimeoutException);
        assertTrue(!Mono.from(connection.validate(ValidationDepth.LOCAL)).block());

        // A broken connection must not be reused for the next statement either.
        assertThrows(
            Throwable.class,
            () ->
                Mono.from(connection.createStatement(selectAllText()).execute())
                    .timeout(Duration.ofSeconds(30))
                    .block());
      } finally {
        Mono.from(connection.close()).block();
      }
    } finally {
      ((IoTDBR2dbcConnectionFactory) factory).close();
      proxy.destroyForcibly();
      proxy.waitFor();
      Files.deleteIfExists(stallFile);
    }
  }

  /**
   * A silent server must not make close() hang. The cleanup budget bounds the closeSession round
   * trip while the per RPC budget stays out of the way, the socket is closed either way, and the
   * caller sees a normal completion because closing is best effort.
   */
  @Test
  public void closingAConnectionWithASilentServerStaysWithinTheCleanupTimeout() throws Exception {
    Path stallFile = Files.createTempFile("r2dbc-stall", ".flag");
    Files.delete(stallFile);
    int proxyPort = freePort();
    Process proxy = startStallProxy(stallFile, proxyPort);
    ConnectionFactory factory = factory(proxyPort, Duration.ofMinutes(10), Duration.ofSeconds(1));
    try {
      awaitPort(proxyPort);
      Connection connection = Mono.from(factory.create()).block();
      provision(connection);
      Files.createFile(stallFile);

      long start = System.nanoTime();
      Mono.from(connection.close()).block(Duration.ofSeconds(5));
      long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

      assertTrue(
          "close must return on the cleanup budget, not on the ten minute RPC budget, took "
              + elapsedMillis
              + " ms",
          elapsedMillis < 5_000);
    } finally {
      ((IoTDBR2dbcConnectionFactory) factory).close();
      proxy.destroyForcibly();
      proxy.waitFor();
      Files.deleteIfExists(stallFile);
    }
  }

  /**
   * Abandoning a result releases the query handle with a request that queues behind the stalled
   * fetch, so the cleanup budget rather than the per RPC budget decides. A connection whose cleanup
   * did not finish must be destroyed instead of being handed back with an unknown protocol state.
   */
  @Test
  public void abandoningAResultWithASilentServerDestroysTheConnection() throws Exception {
    Path stallFile = Files.createTempFile("r2dbc-stall", ".flag");
    Files.delete(stallFile);
    int proxyPort = freePort();
    Process proxy = startStallProxy(stallFile, proxyPort);
    ConnectionFactory factory = factory(proxyPort, Duration.ofMinutes(10), Duration.ofSeconds(1));
    try {
      awaitPort(proxyPort);
      String table = DATABASE + ".abandon_probe";
      // Calibrate on the endpoint the helper forwards to: provisioning a device that this endpoint
      // does not own would move the session to another DataNode and bypass the silent helper, which
      // this case cannot observe. A single node endpoint owns its own writes.
      ConnectionFactory local =
          factory(proxyPort, Duration.ofMinutes(10), Duration.ofSeconds(1), 0);
      try {
        Connection calibration = Mono.from(local.create()).block();
        try {
          execute(calibration, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
          execute(
              calibration,
              "CREATE TABLE IF NOT EXISTS " + table + "(device STRING TAG, v INT32 FIELD)");
          try {
            execute(calibration, insertAbandonRow(table, 1));
          } catch (RuntimeException notLocal) {
            Assume.assumeTrue(
                "this case needs an endpoint that owns the fixture, but it recommends another DataNode"
                    + " for it: "
                    + notLocal.getMessage(),
                false);
          }
          provisionAbandonRows(calibration, table);
        } finally {
          Mono.from(calibration.close()).block();
        }
      } finally {
        ((IoTDBR2dbcConnectionFactory) local).close();
      }

      Connection connection = Mono.from(factory.create()).block();
      provision(connection);
      Statement statement = connection.createStatement("SELECT time, v FROM " + table);
      statement.fetchSize(1);
      // Demand one row only: the query is open on the server (a query id exists) but no fetch is in
      // flight, so cancelling afterwards releases the handle with a single request that the silent
      // server never answers.
      CountDownLatch firstRow = new CountDownLatch(1);
      CountDownLatch completed = new CountDownLatch(1);
      BaseSubscriber<Row> rows =
          new BaseSubscriber<Row>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
              request(1);
            }

            @Override
            protected void hookOnNext(Row row) {
              firstRow.countDown();
            }

            @Override
            protected void hookOnComplete() {
              completed.countDown();
            }
          };
      Flux.from(statement.execute())
          .flatMap(result -> result.map((row, metadata) -> row), 1, 1)
          .subscribe(rows);
      try {
        assertTrue(
            "the query must deliver a row before the server goes silent",
            firstRow.await(30, TimeUnit.SECONDS));
        assertTrue(
            "the fixture fits into one page, so there is nothing left to abandon",
            completed.getCount() > 0);
        Files.createFile(stallFile);
        rows.cancel();

        long elapsedMillis = awaitClosed(connection, Duration.ofSeconds(5));

        assertTrue(
            "the cleanup budget must decide, not the ten minute RPC budget, took "
                + elapsedMillis
                + " ms",
            elapsedMillis < 5_000);
        assertThrows(
            "a connection whose cleanup timed out must not be reused",
            Throwable.class,
            () -> Mono.from(connection.createStatement(selectAllText()).execute()).block());
      } finally {
        rows.cancel();
      }
    } finally {
      ((IoTDBR2dbcConnectionFactory) factory).close();
      proxy.destroyForcibly();
      proxy.waitFor();
      Files.deleteIfExists(stallFile);
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void awaitPort(int port) throws Exception {
    long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      try (Socket socket = new Socket("127.0.0.1", port)) {
        return;
      } catch (IOException e) {
        Thread.sleep(50);
      }
    }
    throw new AssertionError("proxy did not start on port " + port);
  }

  /**
   * Starts the silent but connected server helper; creating the flag file switches it to stalling.
   * Skipped when the helper is not configured, so the cases that need it do not fail with a null
   * path.
   */
  private static Process startStallProxy(Path stallFile, int proxyPort) throws IOException {
    Assume.assumeTrue(
        "set -Diotdb.it.stallProxy=<path to stall_proxy.py> to run it",
        System.getProperty("iotdb.it.stallProxy") != null);
    return new ProcessBuilder(
            System.getProperty("iotdb.it.python", "python"),
            Path.of(System.getProperty("iotdb.it.stallProxy")).toString(),
            "--listen-port",
            String.valueOf(proxyPort),
            "--target-port",
            String.valueOf(Integer.getInteger("iotdb.it.port", 6667)),
            "--stall-file",
            stallFile.toString())
        .redirectErrorStream(true)
        .start();
  }

  private static String insertAbandonRow(String table, int time) {
    return "INSERT INTO " + table + "(time,device,v) VALUES (" + time + ",'abandon'," + time + ")";
  }

  /** Writes more rows than one page holds, so an abandoned result still has a live handle. */
  private static void provisionAbandonRows(Connection connection, String table) {
    for (int start = 1; start <= ABANDON_ROWS; start += ABANDON_BATCH) {
      int end = Math.min(start + ABANDON_BATCH, ABANDON_ROWS);
      StringBuilder insert = new StringBuilder("INSERT INTO " + table + "(time,device,v) VALUES ");
      for (int row = start; row <= end; row++) {
        if (row > start) {
          insert.append(',');
        }
        insert.append('(').append(row).append(",'abandon',").append(row).append(')');
      }
      execute(connection, insert.toString());
    }
  }

  /** Waits until the driver reports the connection as destroyed and returns the elapsed time. */
  private static long awaitClosed(Connection connection, Duration budget)
      throws InterruptedException {
    long start = System.nanoTime();
    long deadline = start + budget.toNanos();
    while (System.nanoTime() < deadline) {
      if (!Boolean.TRUE.equals(Mono.from(connection.validate(ValidationDepth.LOCAL)).block())) {
        return (System.nanoTime() - start) / 1_000_000;
      }
      Thread.sleep(50);
    }
    throw new AssertionError("the connection was not destroyed within " + budget);
  }

  /**
   * Sustained mixed workload, gated by {@code -Diotdb.it.soak.seconds=N}: paged queries, prepared
   * statements and discarded results on one connection for as long as the budget allows. Every
   * iteration asserts what it read, and the run reports rows, statements, heap and thread deltas
   * with a {@code SOAK} prefix so a long run leaves evidence behind.
   *
   * <p>Skipped by default: the short rounds above are the suite, and a build should not depend on
   * one machine being free for minutes.
   */
  @Test
  public void sustainedMixedWorkloadKeepsResourcesBounded() throws Exception {
    int seconds = Integer.getInteger("iotdb.it.soak.seconds", 0);
    Assume.assumeTrue("set -Diotdb.it.soak.seconds=N to run it", seconds > 0);
    ConnectionFactory factory = factory();
    Connection connection = Mono.from(factory.create()).block();
    try {
      provision(connection);
      long baselineHeap = usedHeapAfterCollection();
      long selectorsBefore = selectorThreadCount();
      long driverThreadsBefore = driverThreadCount();
      long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
      long statements = 0;
      long rows = 0;
      long nextReport = System.nanoTime() + Duration.ofSeconds(30).toNanos();
      while (System.nanoTime() < deadline) {
        rows += query(connection.createStatement(selectAllText()).fetchSize(5)).size();
        Statement bound = connection.createStatement(selectValueText());
        bound.bind(0, (int) (statements % ROWS) + 1);
        List<Row> boundRows = query(bound);
        assertEquals("bound statement " + statements, 1, boundRows.size());
        rows += boundRows.size();
        // A result that is thrown away still has to release its cursor.
        assertTrue(discardedResultIsEmpty(connection));
        statements += 3;
        if (System.nanoTime() > nextReport) {
          report(seconds, statements, rows, baselineHeap, driverThreadsBefore);
          nextReport = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        }
      }
      report(seconds, statements, rows, baselineHeap, driverThreadsBefore);
      assertTrue(
          "the connection must stay usable",
          Mono.from(connection.validate(ValidationDepth.LOCAL)).block());
      assertEquals(
          "the sustained run must not add a selector thread",
          selectorsBefore,
          selectorThreadCount());
      // Callback workers expire after a minute of inactivity, so a couple of extra ones are normal;
      // the runtime caps them, so unbounded growth would be the defect.
      assertTrue(
          "the sustained run must not accumulate driver threads",
          driverThreadCount() - driverThreadsBefore <= 4);
      assertTrue(
          "the sustained run must not accumulate heap",
          usedHeapAfterCollection() - baselineHeap < 64L * 1024 * 1024);
    } finally {
      Mono.from(connection.close()).block();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  private static void report(
      int seconds, long statements, long rows, long baselineHeap, long baselineThreads) {
    System.out.printf(
        Locale.ROOT,
        "SOAK seconds=%d statements=%d rows=%d heapDeltaMiB=%.1f selectorThreads=%d driverThreads=%d"
            + " driverThreadDelta=%d%n",
        seconds,
        statements,
        rows,
        (usedHeapAfterCollection() - baselineHeap) / (1024.0 * 1024.0),
        selectorThreadCount(),
        driverThreadCount(),
        driverThreadCount() - baselineThreads);
  }

  /** Selector and callback threads of the driver factories this test created. */
  private static long driverThreadCount() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(
            thread ->
                thread.getName().startsWith("TAsyncClientManager#SelectorThread")
                    || thread.getName().startsWith("iotdb-r2dbc-callbacks"))
        .count();
  }

  /** Heap in use after a collection, so the number is comparable between samples. */
  private static long usedHeapAfterCollection() {
    System.gc();
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private static long selectorThreadCount() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread.getName().startsWith("TAsyncClientManager#SelectorThread"))
        .count();
  }

  private static boolean discardedResultIsEmpty(Connection connection) {
    return Flux.from(connection.createStatement(selectAllText()).execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .block()
        .isEmpty();
  }

  private static String selectValueText() {
    return "SELECT v FROM " + DATABASE + ".samples WHERE v = ? ORDER BY time";
  }

  private static String selectAllText() {
    return "SELECT time, v FROM " + DATABASE + ".samples ORDER BY time";
  }

  private static List<Row> query(Statement statement) {
    return Flux.from(statement.execute())
        .flatMap(result -> result.map((row, metadata) -> row))
        .collectList()
        .block();
  }

  private static void provision(Connection connection) {
    execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
    execute(
        connection,
        "CREATE TABLE IF NOT EXISTS " + DATABASE + ".samples(device STRING TAG, v INT32 FIELD)");
    execute(connection, "DELETE FROM " + DATABASE + ".samples");
    long base = System.currentTimeMillis();
    for (int start = 1; start <= ROWS; start += 50) {
      StringBuilder insert =
          new StringBuilder("INSERT INTO " + DATABASE + ".samples(time,device,v) VALUES ");
      for (int row = start; row < start + 50 && row <= ROWS; row++) {
        if (row > start) {
          insert.append(',');
        }
        insert.append('(').append(base + row).append(",'d1',").append(row).append(')');
      }
      execute(connection, insert.toString());
    }
    assertEquals(ROWS, query(connection.createStatement(selectAllText())).size());
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .block();
  }

  private static ConnectionFactory factory() {
    return ConnectionFactories.get(options().build());
  }

  /** A factory on another port, with a per RPC budget short enough for a test. */
  private static ConnectionFactory factory(int port, Duration rpcTimeout) {
    return factory(port, rpcTimeout, null);
  }

  /**
   * Same, with a cleanup budget short enough to observe it: the per RPC budget stays long so the
   * measured time can only come from the cleanup budget.
   */
  private static ConnectionFactory factory(int port, Duration rpcTimeout, Duration cleanupTimeout) {
    return factory(port, rpcTimeout, cleanupTimeout, -1);
  }

  /** A budget of {@code -1} keeps the driver default, {@code 0} refuses any recommendation. */
  private static ConnectionFactory factory(
      int port, Duration rpcTimeout, Duration cleanupTimeout, int maxRedirects) {
    ConnectionFactoryOptions.Builder builder =
        options()
            .option(ConnectionFactoryOptions.PORT, port)
            .option(IoTDBConnectionFactoryOptions.RPC_TIMEOUT, rpcTimeout);
    if (cleanupTimeout != null) {
      builder.option(IoTDBConnectionFactoryOptions.CLEANUP_TIMEOUT, cleanupTimeout);
    }
    if (maxRedirects >= 0) {
      builder.option(IoTDBConnectionFactoryOptions.MAX_REDIRECTS, maxRedirects);
    }
    return ConnectionFactories.get(builder.build());
  }

  private static ConnectionFactoryOptions.Builder options() {
    return ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "iotdb")
        .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
        .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
        .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
        .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
        .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table");
  }
}
