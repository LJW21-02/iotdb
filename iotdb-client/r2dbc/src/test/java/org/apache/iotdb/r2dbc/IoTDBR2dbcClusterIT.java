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
import io.r2dbc.spi.ValidationDepth;
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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Runs against a 1C3D cluster (one ConfigNode, three DataNodes) started by {@code
 * src/test/scripts/fixture.py}. It checks that data written through one DataNode is readable from
 * the others and that a broken connection surfaces as a transport error instead of being silently
 * reused or replayed.
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

  /** A successful write recommendation must not be mistaken for a refused request. */
  @Test
  public void writesOnEveryDataNodeCompleteWithNoRedirectBudget() {
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

    // The owning DataNode is cluster-dependent. Every successful execution must still be reported
    // as successful, even if its response includes a routing recommendation and the budget is 0.
    for (int index = 0; index < ports.size(); index++) {
      ConnectionFactory factory = factory(ports.get(index), 0);
      try {
        Connection connection = Mono.from(factory.create()).block();
        try {
          execute(connection, insertProbeRow(PROBE_DEVICE, 1_000 + index, 10 + index));
        } finally {
          Mono.from(connection.close()).block();
        }
      } finally {
        ((IoTDBR2dbcConnectionFactory) factory).close();
      }
    }

    int readerPort = ports.get(0);
    ConnectionFactory reader = factory(readerPort);
    try {
      Connection connection = Mono.from(reader.create()).block();
      try {
        List<Integer> visibleValues = new ArrayList<>();
        for (Row row : probeRows(connection)) {
          if (PROBE_DEVICE.equals(row.get("device", String.class))) {
            visibleValues.add(row.get("v", Integer.class));
          }
        }
        for (int index = 0; index < ports.size(); index++) {
          assertTrue(
              "write from port " + ports.get(index) + " is not visible from port " + readerPort,
              visibleValues.contains(10 + index));
        }
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
                System.getProperty("iotdb.it.cluster.python", "python"),
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

  /**
   * Stops a real fixture DataNode, checks the old connection, then restarts it. The fixture uses
   * replication factor one, so this checks connection invalidation and surviving RPC availability,
   * not continuous access to a region hosted only on the stopped node.
   */
  @Test
  public void stoppedDataNodeInvalidatesItsConnectionWhileAnotherNodeStaysHealthy()
      throws Exception {
    Assume.assumeTrue(System.getProperty("iotdb.it.cluster.fixture") != null);
    Assume.assumeTrue(System.getProperty("iotdb.it.cluster.control") != null);
    List<Integer> ports = clusterPorts();
    assertEquals(3, ports.size());
    IoTDBR2dbcConnectionFactory victimFactory = (IoTDBR2dbcConnectionFactory) factory(ports.get(2));
    IoTDBR2dbcConnectionFactory survivorFactory =
        (IoTDBR2dbcConnectionFactory) factory(ports.get(0));
    Connection victim = null;
    Connection survivor = null;
    boolean stopAttempted = false;
    try {
      victim = Mono.from(victimFactory.create()).block(Duration.ofSeconds(15));
      survivor = Mono.from(survivorFactory.create()).block(Duration.ofSeconds(15));
      assertTrue(serverVersion(victim) > 0);
      assertTrue(serverVersion(survivor) > 0);
      stopAttempted = true;
      controlFixtureNode("kill-node");

      Connection stoppedConnection = victim;
      assertThrows(RuntimeException.class, () -> serverVersion(stoppedConnection));
      assertTrue(!Mono.from(victim.validate(ValidationDepth.LOCAL)).block());
      // A second request must fail too, rather than reconnecting or replaying it elsewhere.
      assertThrows(RuntimeException.class, () -> serverVersion(stoppedConnection));
      assertTrue(serverVersion(survivor) > 0);
      assertTrue(Mono.from(survivor.validate(ValidationDepth.REMOTE)).block());
    } finally {
      try {
        Mono.whenDelayError(
                victim == null ? Mono.empty() : Mono.from(victim.close()),
                survivor == null ? Mono.empty() : Mono.from(survivor.close()))
            .block(Duration.ofSeconds(15));
      } finally {
        try {
          victimFactory.close();
        } finally {
          try {
            survivorFactory.close();
          } finally {
            if (stopAttempted) {
              controlFixtureNode("restart-node");
            }
          }
        }
      }
    }
  }

  private static int serverVersion(Connection connection) {
    return Flux.from(connection.createStatement("SHOW VERSION").execute())
        .concatMap(result -> result.map((row, metadata) -> row.get(0, String.class)))
        .collectList()
        .block(Duration.ofSeconds(15))
        .size();
  }

  private static void controlFixtureNode(String action) throws Exception {
    Process controller =
        new ProcessBuilder(
                System.getProperty("iotdb.it.cluster.python", "python"),
                System.getProperty("iotdb.it.cluster.control"),
                action,
                "--state",
                System.getProperty("iotdb.it.cluster.fixture"),
                "--node",
                "dn3")
            .redirectErrorStream(true)
            .start();
    if (!controller.waitFor(120, TimeUnit.SECONDS)) {
      controller.destroyForcibly();
      throw new AssertionError("Fixture node controller exceeded its deadline");
    }
    assertEquals("Fixture node controller failed", 0, controller.exitValue());
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
