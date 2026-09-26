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

  @Test
  public void brokenConnectionFailsFastAndIsNotReused() throws Exception {
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
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, port)
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table")
            .build();
    return ConnectionFactories.get(options);
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
