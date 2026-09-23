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
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Soak checks against a live instance: repeated prepared executions, many pages, a demand driven
 * consumer, parallel connections sharing one runtime and repeated connect/close cycles. Skipped
 * unless {@code -Diotdb.it.host} is given, like {@link IoTDBR2dbcIT}.
 */
public class IoTDBR2dbcSoakIT {

  private static final String DATABASE = "r2dbc_soak";
  private static final int ROWS = 300;

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
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table")
            .build();
    return ConnectionFactories.get(options);
  }
}
