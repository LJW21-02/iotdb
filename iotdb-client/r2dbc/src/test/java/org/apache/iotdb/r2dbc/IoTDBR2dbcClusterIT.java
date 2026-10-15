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
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Runs against a 1C3D cluster (one ConfigNode, three DataNodes) started by {@code
 * .r2dbc-validation/Start-Cluster.ps1}. It checks that data written through one DataNode is
 * readable from the others and that a broken connection surfaces as a transport error instead of
 * being silently reused or replayed.
 *
 * <p>Skipped unless {@code -Diotdb.it.cluster.ports=16667,16668,16669} is given.
 */
public class IoTDBR2dbcClusterIT {

  private static final String DATABASE = "r2dbc_cluster";
  private static final String PROBE_DEVICE = "redirect_probe";
  private static final int ROWS = 30;

  @BeforeClass
  public static void requireCluster() {
    Assume.assumeTrue(System.getProperty("iotdb.it.cluster.ports") != null);
  }

  @Test
  public void dataWrittenThroughOneDataNodeIsReadableFromTheOthers() {
    List<Integer> ports = clusterPorts();
    List<ConnectionFactory> factories = new ArrayList<>();
    try {
      ConnectionFactory writer = factory(ports.get(0));
      factories.add(writer);
      Connection connection = Mono.from(writer.create()).block();
      List<Row> written;
      try {
        execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
        execute(
            connection,
            "CREATE TABLE IF NOT EXISTS "
                + DATABASE
                + ".samples(device STRING TAG, v INT32 FIELD)");
        execute(connection, "DELETE FROM " + DATABASE + ".samples");
        for (int start = 1; start <= ROWS; start += 10) {
          StringBuilder insert =
              new StringBuilder("INSERT INTO " + DATABASE + ".samples(time,device,v) VALUES ");
          for (int row = start; row < start + 10 && row <= ROWS; row++) {
            if (row > start) {
              insert.append(',');
            }
            insert.append('(').append(row).append(",'d1',").append(row).append(')');
          }
          execute(connection, insert.toString());
        }
        written = query(connection);
        assertEquals(ROWS, written.size());
      } finally {
        Mono.from(connection.close()).block();
      }

      // Reading through the other DataNodes exercises the cluster routing path for regions that are
      // not hosted locally.
      for (int index = 1; index < ports.size(); index++) {
        ConnectionFactory reader = factory(ports.get(index));
        factories.add(reader);
        Connection readConnection = Mono.from(reader.create()).block();
        try {
          List<Row> rows = query(readConnection);
          assertEquals("reader on port " + ports.get(index), ROWS, rows.size());
          assertEquals(Integer.valueOf(1), rows.get(0).get("v", Integer.class));
          assertEquals(Integer.valueOf(ROWS), rows.get(ROWS - 1).get("v", Integer.class));
        } finally {
          Mono.from(readConnection.close()).block();
        }
      }
    } finally {
      for (ConnectionFactory factory : factories) {
        ((IoTDBR2dbcConnectionFactory) factory).close();
      }
    }
  }

  /**
   * A typed parameter query uses the prepare RPC, so the server side handle is created on the
   * session that serves it; the same statement must still return the rows when the request lands on
   * a DataNode that does not host the fixture.
   */
  @Test
  public void typedParameterQueryRunsFromAnyDataNode() {
    List<Integer> ports = clusterPorts();
    List<ConnectionFactory> factories = new ArrayList<>();
    try {
      provision();
      for (int port : ports) {
        ConnectionFactory factory = factory(port);
        factories.add(factory);
        Connection connection = Mono.from(factory.create()).block();
        try {
          List<Row> rows =
              Flux.from(
                      connection
                          .createStatement(
                              "SELECT v FROM " + DATABASE + ".samples WHERE device = ?")
                          .bind(0, "d1")
                          .execute())
                  .flatMap(result -> result.map((row, metadata) -> row))
                  .collectList()
                  .block();
          assertEquals("reader on port " + port, ROWS, rows.size());
          assertEquals(Integer.valueOf(1), rows.get(0).get("v", Integer.class));
        } finally {
          Mono.from(connection.close()).block();
        }
      }
    } finally {
      for (ConnectionFactory factory : factories) {
        ((IoTDBR2dbcConnectionFactory) factory).close();
      }
    }
  }

  /**
   * A write that lands on a DataNode that does not own the region is answered with a recommendation
   * instead of a failure. With no budget left the driver must report the exhausted budget rather
   * than write somewhere else; with a budget it must re-establish the session on the recommended
   * node, and the row must then be visible from another node.
   *
   * <p>Which DataNode owns a device is decided by the cluster, so the test calibrates itself: it
   * writes a distinct device from every DataNode with {@code iotdb.maxRedirects=0} and requires at
   * least one refusal and one acceptance, then repeats a refused write with the default budget.
   */
  @Test
  public void aWriteOnANonOwningDataNodeIsRedirectedWithinTheBudget() {
    List<Integer> ports = clusterPorts();
    ConnectionFactory provisioner = factory(ports.get(0));
    try {
      Connection connection = Mono.from(provisioner.create()).block();
      try {
        execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
        execute(
            connection,
            "CREATE TABLE IF NOT EXISTS "
                + DATABASE
                + ".redirect_probe(device STRING TAG, v INT32 FIELD)");
      } finally {
        Mono.from(connection.close()).block();
      }
    } finally {
      ((IoTDBR2dbcConnectionFactory) provisioner).close();
    }

    // Calibration: the same device is written from every DataNode, so exactly one DataNode owns the
    // region and only that one may accept a write while the budget refuses every recommendation.
    List<Integer> refused = new ArrayList<>();
    for (int index = 0; index < ports.size(); index++) {
      ConnectionFactory factory = factory(ports.get(index), 0);
      try {
        Connection connection = Mono.from(factory.create()).block();
        try {
          execute(connection, insertProbeRow(PROBE_DEVICE, 1_000 + index, 10 + index));
        } catch (RuntimeException failure) {
          assertTrue(
              "a refused redirect must report the exhausted budget, was " + failure,
              failure.getMessage() != null
                  && failure
                      .getMessage()
                      .contains("The redirection budget of 0 hops is exhausted"));
          refused.add(ports.get(index));
        } finally {
          Mono.from(connection.close()).block();
        }
      } finally {
        ((IoTDBR2dbcConnectionFactory) factory).close();
      }
    }
    assertTrue(
        "no DataNode refused the write: no recommendation was exercised", !refused.isEmpty());
    assertTrue(
        "every DataNode refused the write: the fixture is unusable", refused.size() < ports.size());

    // The default budget must follow the recommendation for the refused write.
    int writerPort = refused.get(0);
    ConnectionFactory writer = factory(writerPort);
    try {
      Connection connection = Mono.from(writer.create()).block();
      try {
        execute(connection, insertProbeRow(PROBE_DEVICE, 2_000, 77));
      } finally {
        Mono.from(connection.close()).block();
      }
    } finally {
      ((IoTDBR2dbcConnectionFactory) writer).close();
    }

    int readerPort = ports.get(0) == writerPort ? ports.get(1) : ports.get(0);
    ConnectionFactory reader = factory(readerPort);
    try {
      Connection connection = Mono.from(reader.create()).block();
      try {
        boolean found = false;
        for (Row row : probeRows(connection)) {
          if (PROBE_DEVICE.equals(row.get("device", String.class))
              && Integer.valueOf(77).equals(row.get("v", Integer.class))) {
            found = true;
          }
        }
        assertTrue("the redirected write is not visible from port " + readerPort, found);
      } finally {
        Mono.from(connection.close()).block();
      }
    } finally {
      ((IoTDBR2dbcConnectionFactory) reader).close();
    }
  }

  @Test
  public void brokenConnectionFailsFastAndIsNotReused() throws Exception {
    Assume.assumeTrue(
        "set -Diotdb.it.cluster.proxy=<path to rtt_proxy.py> to run it",
        System.getProperty("iotdb.it.cluster.proxy") != null);
    int dataNodePort = clusterPorts().get(0);
    int proxyPort = freePort();
    Path proxyScript = Path.of(System.getProperty("iotdb.it.cluster.proxy"));
    Process proxy =
        new ProcessBuilder(
                "python",
                proxyScript.toString(),
                "--listen-port",
                String.valueOf(proxyPort),
                "--target-port",
                String.valueOf(dataNodePort),
                "--delay-ms",
                "0")
            .redirectErrorStream(true)
            .start();
    ConnectionFactory factory = factory(proxyPort);
    try {
      awaitProxy(proxyPort);
      provision();
      Connection connection = Mono.from(factory.create()).block();
      try {
        assertEquals(
            ROWS,
            Flux.from(
                    connection
                        .createStatement("SELECT v FROM " + DATABASE + ".samples ORDER BY time")
                        .execute())
                .flatMap(result -> result.map((row, metadata) -> row))
                .collectList()
                .block()
                .size());

        proxy.destroyForcibly();
        proxy.waitFor();

        // The next request must fail with a transport error in bounded time instead of hanging.
        assertThrows(
            RuntimeException.class,
            () ->
                Mono.from(
                        connection
                            .createStatement("SELECT v FROM " + DATABASE + ".samples")
                            .execute())
                    .timeout(Duration.ofSeconds(15))
                    .block());

        // The connection is broken, so it must not silently succeed afterwards.
        assertThrows(
            RuntimeException.class,
            () ->
                Mono.from(
                        connection
                            .createStatement("SELECT v FROM " + DATABASE + ".samples")
                            .execute())
                    .timeout(Duration.ofSeconds(15))
                    .block());
        assertTrue(!Mono.from(connection.validate(io.r2dbc.spi.ValidationDepth.LOCAL)).block());
      } finally {
        Mono.from(connection.close()).block();
      }
    } finally {
      ((IoTDBR2dbcConnectionFactory) factory).close();
      proxy.destroyForcibly();
    }
  }

  private static ConnectionFactory factory(int port) {
    return factory(port, -1);
  }

  /** A budget of {@code -1} keeps the driver default, {@code 0} refuses any recommendation. */
  private static ConnectionFactory factory(int port, int maxRedirects) {
    ConnectionFactoryOptions.Builder builder =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, port)
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table");
    if (maxRedirects >= 0) {
      builder.option(IoTDBConnectionFactoryOptions.MAX_REDIRECTS, maxRedirects);
    }
    return ConnectionFactories.get(builder.build());
  }

  private static String insertProbeRow(String device, long time, int value) {
    return "INSERT INTO "
        + DATABASE
        + ".redirect_probe(time,device,v) VALUES ("
        + time
        + ",'"
        + device
        + "',"
        + value
        + ")";
  }

  private static List<Row> probeRows(Connection connection) {
    return Flux.from(
            connection
                .createStatement("SELECT time, device, v FROM " + DATABASE + ".redirect_probe")
                .execute())
        .flatMap(result -> result.map((row, metadata) -> row))
        .collectList()
        .block();
  }

  /** Creates the fixture through the first DataNode so both tests are order independent. */
  private static void provision() {
    ConnectionFactory factory = factory(clusterPorts().get(0));
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS " + DATABASE + ".samples(device STRING TAG, v INT32 FIELD)");
      execute(connection, "DELETE FROM " + DATABASE + ".samples");
      StringBuilder insert =
          new StringBuilder("INSERT INTO " + DATABASE + ".samples(time,device,v) VALUES ");
      for (int row = 1; row <= ROWS; row++) {
        if (row > 1) {
          insert.append(',');
        }
        insert.append('(').append(row).append(",'d1',").append(row).append(')');
      }
      execute(connection, insert.toString());
      assertEquals(ROWS, query(connection).size());
    } finally {
      Mono.from(connection.close()).block();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  private static void awaitProxy(int port) throws Exception {
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

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static List<Integer> clusterPorts() {
    List<Integer> ports = new ArrayList<>();
    for (String port : System.getProperty("iotdb.it.cluster.ports").split(",")) {
      ports.add(Integer.parseInt(port.trim()));
    }
    return ports;
  }

  private static List<Row> query(Connection connection) {
    return Flux.from(
            connection
                .createStatement("SELECT v FROM " + DATABASE + ".samples ORDER BY time")
                .execute())
        .flatMap(result -> result.map((row, metadata) -> row))
        .collectList()
        .block();
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .block();
  }
}
