/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.r2dbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
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
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Representative real-server protocol, timestamp precision, TLS and cluster combinations. */
public class IoTDBR2dbcCompatibilityIT {
  private static final Duration BUDGET = Duration.ofSeconds(90);
  private static final String TABLE = "r2dbc_compatibility.samples";

  @BeforeClass
  public static void requireFixture() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @Test
  public void configuredTransportAndTemporalValuesRoundTrip() {
    String precision = System.getProperty("iotdb.it.precision", "ms");
    int factor =
        precision.equals("ns") ? 1_000_000_000 : precision.equals("us") ? 1_000_000 : 1_000;
    int nanos =
        precision.equals("ns") ? 123_456_789 : precision.equals("us") ? 123_456_000 : 123_000_000;
    Instant first = Instant.ofEpochSecond(1_700_000_000L, nanos);
    Instant second = first.plusNanos(1_000_000_000L / factor);
    List<Integer> ports = ports();
    try (IoTDBR2dbcConnectionFactory factory = factory(ports.get(0), true)) {
      Connection connection = open(factory);
      try {
        execute(connection.createStatement("CREATE DATABASE IF NOT EXISTS r2dbc_compatibility"));
        execute(
            connection.createStatement(
                "CREATE TABLE IF NOT EXISTS " + TABLE + "(device STRING TAG, v INT32 FIELD)"));
        execute(connection.createStatement("DELETE FROM " + TABLE));
        execute(
            connection
                .createStatement("INSERT INTO " + TABLE + "(time,device,v) VALUES (?,?,?)")
                .bind(0, first)
                .bind(1, "d1")
                .bind(2, 101));
        execute(
            connection
                .createStatement("INSERT INTO " + TABLE + "(time,device,v) VALUES (?,?,?)")
                .bind(0, LocalDateTime.ofInstant(second, ZoneOffset.UTC))
                .bind(1, "d1")
                .bind(2, 202));
      } finally {
        Mono.from(connection.close()).block(BUDGET);
      }
    }
    for (int port : ports) {
      try (IoTDBR2dbcConnectionFactory factory = factory(port, true)) {
        Connection connection = open(factory);
        try {
          assertTrue(Mono.from(connection.validate(ValidationDepth.REMOTE)).block(BUDGET));
          List<Row> rows =
              rows(
                  connection
                      .createStatement(
                          "SELECT time,v FROM "
                              + TABLE
                              + " WHERE time >= ? AND time <= ? ORDER BY time")
                      .bind(0, first)
                      .bind(1, LocalDateTime.ofInstant(second, ZoneOffset.UTC))
                      .fetchSize(1));
          assertEquals(2, rows.size());
          assertEquals(first, rows.get(0).get("time", Instant.class));
          assertEquals(second, rows.get(1).get("time", Instant.class));
          assertEquals(
              Long.valueOf(first.getEpochSecond() * factor + nanos / (1_000_000_000L / factor)),
              rows.get(0).get("time", Long.class));
          assertEquals(Integer.valueOf(101), rows.get(0).get("v", Integer.class));
          assertEquals(Integer.valueOf(202), rows.get(1).get("v", Integer.class));
        } finally {
          Mono.from(connection.close()).block(BUDGET);
        }
      }
    }
  }

  @Test
  public void serverVersionAndOptionalClientCertificatePolicyAreVerified() {
    int port = ports().get(0);
    try (IoTDBR2dbcConnectionFactory factory = factory(port, true)) {
      Connection connection = open(factory);
      try {
        assertTrue(Mono.from(connection.validate(ValidationDepth.REMOTE)).block(BUDGET));
        List<Row> versions = rows(connection.createStatement("SHOW VERSION"));
        assertTrue(!versions.isEmpty());
        System.out.printf(
            "COMPATIBILITY protocol=%s precision=%s ssl=%s mutualTls=%s dataNodes=%d serverVersion=%s%n",
            System.getProperty("iotdb.it.protocol", "binary"),
            System.getProperty("iotdb.it.precision", "ms"),
            System.getProperty("iotdb.it.ssl", "false"),
            System.getProperty("iotdb.it.clientAuth", "false"),
            ports().size(),
            versions.get(0).get(0, String.class));
        if (ports().size() > 1) {
          List<String> running =
              rows(connection.createStatement("SHOW CLUSTER DETAILS")).stream()
                  .filter(
                      row ->
                          "DataNode".equals(row.get("NodeType", String.class))
                              && "Running".equals(row.get("Status", String.class)))
                  .map(row -> row.get("RpcPort", String.class))
                  .collect(Collectors.toList());
          for (int expected : ports()) {
            assertTrue(
                "DataNode must be registered Running: " + expected,
                running.contains(Integer.toString(expected)));
          }
        }
      } finally {
        Mono.from(connection.close()).block(BUDGET);
      }
    }
    if (Boolean.getBoolean("iotdb.it.clientAuth")) {
      try (IoTDBR2dbcConnectionFactory missingCertificate = factory(port, false)) {
        assertThrows(
            RuntimeException.class,
            () -> Mono.from(missingCertificate.create()).block(Duration.ofSeconds(15)));
      }
    }
  }

  private static List<Integer> ports() {
    return Arrays.stream(
            System.getProperty("iotdb.it.cluster.ports", System.getProperty("iotdb.it.port"))
                .split(","))
        .map(Integer::valueOf)
        .collect(Collectors.toList());
  }

  private static IoTDBR2dbcConnectionFactory factory(int port, boolean withClientCertificate) {
    ConnectionFactoryOptions.Builder options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, port)
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.ofSeconds(5))
            .option(IoTDBConnectionFactoryOptions.RPC_TIMEOUT, Duration.ofSeconds(10))
            .option(
                IoTDBConnectionFactoryOptions.PROTOCOL,
                System.getProperty("iotdb.it.protocol", "binary"));
    if (Boolean.getBoolean("iotdb.it.ssl")) {
      options
          .option(ConnectionFactoryOptions.SSL, true)
          .option(
              IoTDBConnectionFactoryOptions.TRUST_STORE,
              System.getProperty("iotdb.it.tls.trustStore"))
          .option(
              IoTDBConnectionFactoryOptions.TRUST_STORE_PASSWORD,
              System.getProperty("iotdb.it.tls.trustStorePassword"));
    }
    if (withClientCertificate && Boolean.getBoolean("iotdb.it.clientAuth")) {
      options
          .option(
              IoTDBConnectionFactoryOptions.KEY_STORE, System.getProperty("iotdb.it.tls.keyStore"))
          .option(
              IoTDBConnectionFactoryOptions.KEY_STORE_PASSWORD,
              System.getProperty("iotdb.it.tls.keyStorePassword"));
    }
    return (IoTDBR2dbcConnectionFactory) ConnectionFactories.get(options.build());
  }

  private static Connection open(IoTDBR2dbcConnectionFactory factory) {
    return Mono.from(factory.create())
        .retryWhen(Retry.fixedDelay(89, Duration.ofSeconds(1)))
        .block(BUDGET);
  }

  private static void execute(Statement statement) {
    Flux.from(statement.execute()).concatMap(Result::getRowsUpdated).then().block(BUDGET);
  }

  private static List<Row> rows(Statement statement) {
    return Flux.from(statement.execute())
        .concatMap(result -> result.map((row, metadata) -> row))
        .collectList()
        .block(BUDGET);
  }
}
