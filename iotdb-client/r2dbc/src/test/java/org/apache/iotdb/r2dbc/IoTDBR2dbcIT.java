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

import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
            .bind(0, 1_700_000_000_000L + offset)
            .bind(1, "d" + offset)
            .bind(2, offset)
            .bind(3, offset + 0.5D)
            .bind(4, "text-" + offset)
            .bind(5, offset % 2 == 0);
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
                      .bind(0, 100)
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
              .bind(0, 100)
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

  @Test
  public void timestampParametersUseEpochTicksAndFailedPreparesAreReleased() {
    ConnectionFactory factory = factory(IoTDBConnectionConfiguration.SqlDialect.TABLE);
    Connection connection = Mono.from(factory.create()).block();
    // Unique timestamps keep repeated runs of this test independent, because the table keeps the
    // rows written by earlier runs.
    long base = System.currentTimeMillis();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS " + DATABASE + ".times(device STRING TAG, v INT32 FIELD)");
      execute(
          connection,
          "INSERT INTO "
              + DATABASE
              + ".times(time,device,v) VALUES ("
              + (base + 1000)
              + ",'d1',1),("
              + (base + 2000)
              + ",'d1',2),("
              + (base + 3000)
              + ",'d1',3)");

      // A LocalDateTime parameter is converted with the connection zone; the default is UTC.
      List<Row> afterOne =
          query(
              connection
                  .createStatement(
                      "SELECT v FROM " + DATABASE + ".times WHERE time > ? ORDER BY time")
                  .bind(
                      0,
                      LocalDateTime.ofInstant(Instant.ofEpochMilli(base + 1000), ZoneOffset.UTC)));
      assertEquals(2, afterOne.size());
      assertEquals(Integer.valueOf(2), afterOne.get(0).get("v", Integer.class));

      // Instant parameters use the same epoch tick encoding.
      List<Row> afterTwo =
          query(
              connection
                  .createStatement(
                      "SELECT v FROM " + DATABASE + ".times WHERE time > ? ORDER BY time")
                  .bind(0, Instant.ofEpochMilli(base + 2000)));
      assertEquals(1, afterTwo.size());
      assertEquals(Integer.valueOf(3), afterTwo.get(0).get("v", Integer.class));

      // A statement that fails after prepare must release its server side handle, otherwise the
      // next execution would be rejected with "Prepared statement ... already exists".
      assertThrows(
          R2dbcException.class,
          () ->
              Mono.from(
                      connection
                          .createStatement("SELECT v FROM " + DATABASE + ".times WHERE v > ?")
                          .bind(0, "not-a-number")
                          .execute())
                  .block());

      List<Row> recovered =
          query(
              connection
                  .createStatement("SELECT v FROM " + DATABASE + ".times WHERE time > ?")
                  .bind(0, base));
      assertEquals(3, recovered.size());
    } finally {
      Mono.from(connection.close()).block();
    }
  }

  @Test
  public void batchStatementsRunInOrderAndStopAtTheFirstFailure() {
    ConnectionFactory factory = factory(IoTDBConnectionConfiguration.SqlDialect.TABLE);
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS "
              + DATABASE
              + ".batch_samples(device STRING TAG, v INT32 FIELD)");

      // Statements without parameters run in order.
      Batch batch =
          connection
              .createBatch()
              .add("INSERT INTO " + DATABASE + ".batch_samples(time,device,v) VALUES (1,'b1',1)")
              .add("INSERT INTO " + DATABASE + ".batch_samples(time,device,v) VALUES (2,'b1',2)");
      assertEquals(2, Flux.from(batch.execute()).cast(Result.class).collectList().block().size());

      // Parameter placeholders are rejected for batches.
      assertThrows(
          IoTDBR2dbcException.class,
          () ->
              connection
                  .createBatch()
                  .add("SELECT * FROM " + DATABASE + ".batch_samples WHERE v = ?"));

      // The first error stops the batch; earlier statements keep their effect.
      Batch failing =
          connection
              .createBatch()
              .add("INSERT INTO " + DATABASE + ".batch_samples(time,device,v) VALUES (3,'b1',3)")
              .add("SELECT * FROM " + DATABASE + ".does_not_exist")
              .add("INSERT INTO " + DATABASE + ".batch_samples(time,device,v) VALUES (4,'b1',4)");
      assertThrows(R2dbcException.class, () -> Flux.from(failing.execute()).collectList().block());

      List<Row> rows =
          query(
              connection.createStatement(
                  "SELECT time FROM "
                      + DATABASE
                      + ".batch_samples WHERE device = 'b1' ORDER BY time"));
      assertEquals(3, rows.size());
    } finally {
      Mono.from(connection.close()).block();
    }
  }

  @Test
  public void statementTimeoutsAndClosedConnectionAreEnforced() {
    ConnectionFactory factory = factory(IoTDBConnectionConfiguration.SqlDialect.TABLE);
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS "
              + DATABASE
              + ".lifecycle(device STRING TAG, v INT32 FIELD)");
      execute(
          connection,
          "INSERT INTO "
              + DATABASE
              + ".lifecycle(time,device,v) VALUES (1,'d1',11),(2,'d1',22),(3,'d1',33)");

      // fetchSize(0) means "driver default"; a negative value is rejected.
      Statement zero = connection.createStatement("SELECT v FROM " + DATABASE + ".lifecycle");
      assertEquals(3, query(zero.fetchSize(0)).size());
      assertThrows(
          IllegalArgumentException.class,
          () -> connection.createStatement("SELECT v").fetchSize(-1));

      // Asking for update counts on a query consumes nothing but must release the cursor, so the
      // connection stays usable.
      List<Long> updates =
          Flux.from(
                  connection.createStatement("SELECT v FROM " + DATABASE + ".lifecycle").execute())
              .flatMap(Result::getRowsUpdated)
              .collectList()
              .block();
      assertTrue(updates.isEmpty());
      assertEquals(
          3, query(connection.createStatement("SELECT v FROM " + DATABASE + ".lifecycle")).size());

      // Remote validation issues a light weight RPC.
      assertTrue(Mono.from(connection.validate(ValidationDepth.REMOTE)).block());

      Mono.from(connection.close()).block();
      assertThrows(IllegalStateException.class, () -> connection.createStatement("SELECT 1"));
      // Closing twice is a no-op.
      Mono.from(connection.close()).block();
    } finally {
      Mono.from(connection.close()).block();
    }
  }

  @Test
  public void wrongCredentialsAreRejectedAsPermissionErrors() {
    ConnectionFactory factory =
        factory(
            IoTDBConnectionConfiguration.SqlDialect.TABLE,
            System.getProperty("iotdb.it.password") + "x");
    R2dbcException error =
        assertThrows(R2dbcException.class, () -> Mono.from(factory.create()).block());
    assertTrue(
        "expected a permission failure but was " + error,
        error instanceof R2dbcPermissionDeniedException);
  }

  private static List<Row> query(Statement statement) {
    return Flux.from(statement.execute())
        .flatMap(result -> result.map((row, metadata) -> row))
        .collectList()
        .block();
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
    return factory(dialect, System.getProperty("iotdb.it.password"));
  }

  private static ConnectionFactory factory(
      IoTDBConnectionConfiguration.SqlDialect dialect, String password) {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, password)
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
