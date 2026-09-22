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
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.ValidationDepth;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * End to end test against a real IoTDB instance.
 *
 * <p>It is skipped unless {@code -Diotdb.it.host} is given, for example: {@code mvn test
 * -Dtest=IoTDBR2dbcIT -DfailIfNoTests=false -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=16667
 * -Diotdb.it.user=<user> -Diotdb.it.password=<password>}.
 */
public class IoTDBR2dbcIT {

  private static final String DATABASE = "r2dbc_e2e";
  private static final List<ConnectionFactory> FACTORIES = new ArrayList<>();

  @BeforeClass
  public static void requireServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @AfterClass
  public static void closeFactories() {
    for (ConnectionFactory factory : FACTORIES) {
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void tableModelRoundTripWithPagingAndTypedParameters() {
    ConnectionFactory factory = factory(IoTDBConnectionConfiguration.SqlDialect.TABLE);
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS "
              + DATABASE
              + ".samples(device STRING TAG, v INT32 FIELD, d DOUBLE FIELD, s STRING FIELD,"
              + " b BOOLEAN FIELD)");
      insertSampleRows(connection, 7);

      // Literal binding: the table dialect prepares queries only, writes use the literal path.
      Statement insert =
          connection.createStatement(
              "INSERT INTO " + DATABASE + ".samples(time,device,v,d,s,b) VALUES (?,?,?,?,?,?)");
      for (int i = 0; i < 3; i++) {
        int offset = 100 + i;
        if (i > 0) {
          insert.add();
        }
        insert
            .bind(1, 1_700_000_000_000L + offset)
            .bind(2, "d" + offset)
            .bind(3, offset)
            .bind(4, offset + 0.5D)
            .bind(5, "text-" + offset)
            .bind(6, offset % 2 == 0);
      }
      List<Result> insertResults =
          Flux.from(insert.execute()).cast(Result.class).collectList().block();
      assertEquals(3, insertResults.size());
      for (Result result : insertResults) {
        // IoTDB does not report affected row counts, so the count stream stays empty.
        assertTrue(Flux.from(result.getRowsUpdated()).collectList().block().isEmpty());
      }

      // Typed parameters: table dialect queries with supported scalar types.
      List<Row> rows =
          Flux.from(
                  connection
                      .createStatement(
                          "SELECT time, device, v, d, s, b FROM "
                              + DATABASE
                              + ".samples WHERE v >= ? ORDER BY time")
                      .bind(1, 100)
                      .execute())
              .flatMap(result -> result.map((row, metadata) -> row))
              .collectList()
              .block();
      assertEquals(3, rows.size());
      Row first = rows.get(0);
      assertEquals("d100", first.get("device", String.class));
      assertEquals(Integer.valueOf(100), first.get("v", Integer.class));
      assertEquals(Double.valueOf(100.5D), first.get("d", Double.class));
      assertEquals("text-100", first.get("s", String.class));
      assertEquals(Boolean.TRUE, first.get("b", Boolean.class));
      assertEquals(Long.valueOf(1_700_000_000_100L), first.get("time", Long.class));

      // Paging is demand driven: a fetch size of two forces several pages for seven rows.
      Statement select =
          connection
              .createStatement("SELECT v FROM " + DATABASE + ".samples WHERE v < ? ORDER BY time")
              .bind(1, 100)
              .fetchSize(2);
      List<Integer> values =
          Flux.from(select.execute())
              .flatMap(result -> result.map((row, metadata) -> row.get("v", Integer.class)))
              .collectList()
              .block();
      assertEquals(7, values.size());

      assertTrue(Mono.from(connection.validate(ValidationDepth.LOCAL)).block());

      // IoTDB has no cross statement transactions; the driver must report that instead of faking
      // it.
      assertThrows(R2dbcException.class, () -> Mono.from(connection.beginTransaction()).block());
      assertThrows(R2dbcException.class, () -> Mono.from(connection.setAutoCommit(false)).block());

      // Server errors are mapped to standard R2DBC exceptions with the server code preserved.
      R2dbcException error =
          assertThrows(
              R2dbcException.class,
              () ->
                  Mono.from(
                          connection
                              .createStatement("SELECT * FROM " + DATABASE + ".missing")
                              .execute())
                      .block());
      assertTrue(error.getErrorCode() != 0);

      assertTrue(connection.isAutoCommit());
      Mono.from(connection.close()).block();
    } finally {
      Mono.from(connection.close()).block();
    }
  }

  @Test
  public void treeDialectKeepsTheTimeColumnAndUsesLiteralBinding() {
    ConnectionFactory factory = factory(IoTDBConnectionConfiguration.SqlDialect.TREE);
    Connection connection = Mono.from(factory.create()).block();
    String device = "d" + System.nanoTime();
    String measurement = "root.r2dbc_e2e." + device + ".v";
    try {
      execute(
          connection, "CREATE TIMESERIES " + measurement + " WITH DATATYPE=INT32, ENCODING=RLE");
      execute(
          connection, "INSERT INTO root.r2dbc_e2e." + device + "(time, v) VALUES (1, 11), (2, 22)");

      List<Row> rows =
          Flux.from(connection.createStatement("SELECT v FROM root.r2dbc_e2e." + device).execute())
              .flatMap(result -> result.map((row, metadata) -> row))
              .collectList()
              .block();
      assertEquals(2, rows.size());
      // The tree model time column is exposed as the first column and typed as TIMESTAMP.
      assertEquals("Time", rows.get(0).getMetadata().getColumnMetadata(0).getName());
      // Tree model responses use the full measurement path as the column name.
      assertEquals(Integer.valueOf(11), rows.get(0).get(measurement, Integer.class));
      assertEquals(Instant.ofEpochMilli(1L), rows.get(0).get("Time", Instant.class));
      assertEquals(Long.valueOf(2L), rows.get(1).get("Time", Long.class));
    } finally {
      Mono.from(connection.close()).block();
    }
  }

  private static void insertSampleRows(Connection connection, int count) {
    for (int i = 1; i <= count; i++) {
      execute(
          connection,
          "INSERT INTO "
              + DATABASE
              + ".samples(time,device,v,d,s,b) VALUES ("
              + (1_700_000_000_000L + i)
              + ",'d"
              + i
              + "',"
              + i
              + ","
              + i
              + ".5,'text-"
              + i
              + "',"
              + (i % 2 == 0)
              + ")");
    }
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .block();
  }

  private static ConnectionFactory factory(IoTDBConnectionConfiguration.SqlDialect dialect) {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(
                IoTDBConnectionFactoryOptions.SQL_DIALECT,
                dialect == IoTDBConnectionConfiguration.SqlDialect.TREE ? "tree" : "table")
            .option(IoTDBConnectionFactoryOptions.FETCH_SIZE, 2)
            .build();
    ConnectionFactory factory = ConnectionFactories.get(options);
    FACTORIES.add(factory);
    return factory;
  }
}
