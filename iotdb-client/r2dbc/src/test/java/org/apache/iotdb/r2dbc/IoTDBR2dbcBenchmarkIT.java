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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.session.Session;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Measures the non-blocking driver under sustained load and compares it with the blocking session
 * client, which is the ecosystem baseline; it is not a pure transport comparison because both arms
 * use their own client stack.
 *
 * <p>Correctness is asserted (row checksum and bounded thread usage); the latency and throughput
 * numbers are printed with a {@code BENCH} prefix and recorded in {@code PERFORMANCE.md} instead of
 * being turned into a pass/fail threshold. Skipped unless {@code -Diotdb.it.host} is given.
 */
public class IoTDBR2dbcBenchmarkIT {

  private static final String DATABASE = "r2dbc_bench";
  private static final int ROWS = 500;
  private static final int QUERIES = 200;
  private static final int PARALLEL_CONNECTIONS = 16;
  private static final int PARALLEL_QUERIES = 10;
  private static final int FETCH_SIZE = 50;
  private static final long EXPECTED_SUM = (long) ROWS * (ROWS + 1) / 2;

  @BeforeClass
  public static void requireServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @Test
  public void nonBlockingDriverSustainsLoadWithBoundedThreads() throws Exception {
    ConnectionFactory factory = factory();
    Connection connection = Mono.from(factory.create()).block();
    try {
      provision(connection);
      for (int i = 0; i < 20; i++) {
        assertEquals(EXPECTED_SUM, checksum(connection));
      }

      int threadsBefore = Thread.activeCount();
      List<Long> sequentialLatencies = new ArrayList<>(QUERIES);
      long start = System.nanoTime();
      for (int i = 0; i < QUERIES; i++) {
        long queryStart = System.nanoTime();
        assertEquals(EXPECTED_SUM, checksum(connection));
        sequentialLatencies.add(System.nanoTime() - queryStart);
      }
      double sequentialSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
      report(
          "r2dbc" + transportLabel() + " sequential",
          QUERIES,
          sequentialSeconds,
          sequentialLatencies,
          0);

      // Sixteen connections with an active result each: a thread per waiting request would need at
      // least sixteen extra threads, the shared selector runtime does not.
      List<Connection> connections = new ArrayList<>();
      for (int i = 0; i < PARALLEL_CONNECTIONS; i++) {
        connections.add(Mono.from(factory.create()).block());
      }
      int threadsWithConnections = Thread.activeCount();
      ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_CONNECTIONS);
      try {
        List<Callable<Long>> tasks = new ArrayList<>();
        for (Connection parallel : connections) {
          tasks.add(
              () -> {
                long rows = 0;
                for (int i = 0; i < PARALLEL_QUERIES; i++) {
                  rows += checksum(parallel);
                }
                return rows;
              });
        }
        start = System.nanoTime();
        List<Future<Long>> results = executor.invokeAll(tasks);
        double parallelSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
        for (Future<Long> result : results) {
          assertEquals(EXPECTED_SUM * PARALLEL_QUERIES, result.get().longValue());
        }
        report(
            "r2dbc" + transportLabel() + " parallel",
            PARALLEL_CONNECTIONS * PARALLEL_QUERIES,
            parallelSeconds,
            List.of(),
            threadsWithConnections - threadsBefore);
      } finally {
        executor.shutdownNow();
        for (Connection parallel : connections) {
          Mono.from(parallel.close()).block();
        }
      }
      // Only the connection related delta is asserted: sixteen connections each holding an active
      // result must not need a thread per connection, which a blocking model would require.
      assertTrue(
          "sixteen connections must not require a thread per connection",
          threadsWithConnections - threadsBefore < PARALLEL_CONNECTIONS);
    } finally {
      Mono.from(connection.close()).block();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void blockingSessionBaseline() throws Exception {
    Session.Builder sessionBuilder =
        new Session.Builder()
            .host(System.getProperty("iotdb.it.host"))
            .port(
                tlsEnabled()
                    ? Integer.getInteger("iotdb.it.tls.port", 6667)
                    : Integer.getInteger("iotdb.it.port", 6667))
            .username(System.getProperty("iotdb.it.user"))
            .password(System.getProperty("iotdb.it.password"))
            .sqlDialect("table")
            .fetchSize(FETCH_SIZE)
            .enableRedirection(false)
            .enableAutoFetch(false);
    if (tlsEnabled()) {
      sessionBuilder
          .useSSL(true)
          .trustStore(System.getProperty("iotdb.it.tls.trustStore"))
          .trustStorePwd(System.getProperty("iotdb.it.tls.trustStorePassword"));
    }
    Session session = sessionBuilder.build();
    session.open(false, 10_000);
    try {
      for (int i = 0; i < 20; i++) {
        assertEquals(EXPECTED_SUM, sessionChecksum(session));
      }
      List<Long> latencies = new ArrayList<>(QUERIES);
      long start = System.nanoTime();
      for (int i = 0; i < QUERIES; i++) {
        long queryStart = System.nanoTime();
        assertEquals(EXPECTED_SUM, sessionChecksum(session));
        latencies.add(System.nanoTime() - queryStart);
      }
      double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
      report("session" + transportLabel() + " sequential", QUERIES, seconds, latencies, 0);
    } finally {
      session.close();
    }
  }

  private static void report(
      String label, int queries, double seconds, List<Long> latencies, int threadDelta) {
    double qps = queries / seconds;
    String percentiles = "p50ms=- p95ms=-";
    if (!latencies.isEmpty()) {
      List<Long> sorted = new ArrayList<>(latencies);
      sorted.sort(null);
      percentiles =
          String.format(
              Locale.ROOT,
              "p50ms=%.2f p95ms=%.2f",
              sorted.get(sorted.size() / 2) / 1_000_000.0,
              sorted.get((int) (sorted.size() * 0.95)) / 1_000_000.0);
    }
    System.out.printf(
        Locale.ROOT,
        "BENCH %s queries=%d rowsPerQuery=%d seconds=%.3f qps=%.1f %s threadDelta=%d%n",
        label,
        queries,
        ROWS,
        seconds,
        qps,
        percentiles,
        threadDelta);
  }

  private static long checksum(Connection connection) {
    List<Row> rows =
        Flux.from(
                connection
                    .createStatement("SELECT v FROM " + DATABASE + ".samples ORDER BY time")
                    .fetchSize(FETCH_SIZE)
                    .execute())
            .flatMap(result -> result.map((row, metadata) -> row))
            .collectList()
            .block();
    assertEquals(ROWS, rows.size());
    long sum = 0;
    for (Row row : rows) {
      sum += row.get("v", Integer.class);
    }
    return sum;
  }

  private static long sessionChecksum(Session session) throws Exception {
    long sum = 0;
    int rows = 0;
    try (SessionDataSet dataSet =
        session.executeQueryStatement(
            "SELECT v FROM " + DATABASE + ".samples ORDER BY time", 30_000)) {
      SessionDataSet.DataIterator iterator = dataSet.iterator();
      while (iterator.next()) {
        sum += iterator.getInt(1);
        rows++;
      }
    }
    assertEquals(ROWS, rows);
    return sum;
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
    assertEquals(EXPECTED_SUM, checksum(connection));
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .block();
  }

  private static boolean tlsEnabled() {
    return System.getProperty("iotdb.it.tls.trustStore") != null;
  }

  private static String transportLabel() {
    return tlsEnabled() ? " tls" : "";
  }

  private static ConnectionFactoryOptions.Builder optionsBuilder() {
    ConnectionFactoryOptions.Builder builder =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table");
    if (tlsEnabled()) {
      builder
          .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.tls.port", 6667))
          .option(ConnectionFactoryOptions.SSL, Boolean.TRUE)
          .option(
              IoTDBConnectionFactoryOptions.TRUST_STORE,
              System.getProperty("iotdb.it.tls.trustStore"))
          .option(
              IoTDBConnectionFactoryOptions.TRUST_STORE_PASSWORD,
              System.getProperty("iotdb.it.tls.trustStorePassword"));
    } else {
      builder.option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667));
    }
    return builder;
  }

  private static ConnectionFactory factory() {
    return ConnectionFactories.get(optionsBuilder().build());
  }
}
