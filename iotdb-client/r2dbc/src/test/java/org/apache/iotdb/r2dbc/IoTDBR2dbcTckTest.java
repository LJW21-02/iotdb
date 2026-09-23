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

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.test.TestKit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Runs the R2DBC TCK ({@code r2dbc-spi-test}) against a live IoTDB instance.
 *
 * <p>Requires the {@code with-r2dbc-tck} profile and an instance description:
 *
 * <pre>
 * mvn test -pl iotdb-client/r2dbc -P with-r2dbc-tck -Dtest=IoTDBR2dbcTckTest -DfailIfNoTests=false \
 *     -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=16667 -Diotdb.it.user=&lt;user&gt; -Diotdb.it.password=&lt;password&gt;
 * </pre>
 *
 * <p>The fixture tables are created through the IoTDB JDBC driver, which is the same database the
 * driver under test talks to. Statements are translated to the IoTDB table model because the TCK
 * uses ANSI SQL. Tests for capabilities outside the first release scope (streaming Blob/Clob,
 * generated keys) are disabled with a reason; differences such as update counts and transactions
 * stay visible and are listed in {@code TCK.md}.
 */
@EnabledIfSystemProperty(named = "iotdb.it.host", matches = ".+")
class IoTDBR2dbcTckTest implements TestKit<Integer> {

  private static final String DATABASE = "r2dbc_tck";
  private static final ConnectionFactory CONNECTION_FACTORY = createConnectionFactory();
  private static final JdbcOperations JDBC_OPERATIONS = createJdbcOperations();

  @AfterAll
  static void closeFactory() {
    ((IoTDBR2dbcConnectionFactory) CONNECTION_FACTORY).close();
  }

  @Override
  public ConnectionFactory getConnectionFactory() {
    return CONNECTION_FACTORY;
  }

  @Override
  public String getPlaceholder(int index) {
    return "?";
  }

  @Override
  public Integer getIdentifier(int index) {
    return index;
  }

  @Override
  public JdbcOperations getJdbcOperations() {
    return JDBC_OPERATIONS;
  }

  @Override
  public String blobType() {
    return "BLOB";
  }

  @Override
  public String clobType() {
    return "TEXT";
  }

  @Override
  public String expand(TestStatement statement, Object... args) {
    switch (statement) {
      case CREATE_TABLE:
        return "CREATE TABLE test (test_value INT32 FIELD)";
      case DROP_TABLE:
        return "DROP TABLE test";
      case INSERT_VALUE100:
        return "INSERT INTO test(time, test_value) VALUES (100, 100)";
      case INSERT_VALUE200:
        return "INSERT INTO test(time, test_value) VALUES (200, 200)";
      case INSERT_VALUE_PLACEHOLDER:
        return String.format("INSERT INTO test(time, test_value) VALUES (1, %s)", args[0]);
      case INSERT_TWO_VALUES_PLACEHOLDER:
        return String.format(
            "INSERT INTO test(time, test_value) VALUES (%s, %s)", args[0], args[1]);
      case SELECT_VALUE:
        return "SELECT test_value FROM test";
      case SELECT_VALUE_BATCH:
        return "SELECT test_value FROM test; SELECT test_value FROM test";
      case CREATE_TABLE_TWO_COLUMNS:
        return "CREATE TABLE test_two_column (col1 INT32 FIELD, col2 STRING FIELD)";
      case DROP_TABLE_TWO_COLUMNS:
        return "DROP TABLE test_two_column";
      case INSERT_TWO_COLUMNS:
        return "INSERT INTO test_two_column(time, col1, col2) VALUES (1, 100, 'hello')";
      case SELECT_VALUE_TWO_COLUMNS:
        return "SELECT col1 AS test_value, col2 AS test_value FROM test_two_column";
      case SELECT_VALUE_ALIASED_COLUMNS:
        return "SELECT col1 AS b, col1 AS c, col1 AS a FROM test_two_column";
      case CREATE_BLOB_TABLE:
        return "CREATE TABLE blob_test (test_value BLOB FIELD)";
      case DROP_BLOB_TABLE:
        return "DROP TABLE blob_test";
      case INSERT_BLOB_VALUE_PLACEHOLDER:
        return String.format("INSERT INTO blob_test(time, test_value) VALUES (1, %s)", args[0]);
      case SELECT_BLOB_VALUE:
        return "SELECT test_value FROM blob_test";
      case CREATE_CLOB_TABLE:
        return "CREATE TABLE clob_test (test_value TEXT FIELD)";
      case DROP_CLOB_TABLE:
        return "DROP TABLE clob_test";
      case INSERT_CLOB_VALUE_PLACEHOLDER:
        return String.format("INSERT INTO clob_test(time, test_value) VALUES (1, %s)", args[0]);
      case SELECT_CLOB_VALUE:
        return "SELECT test_value FROM clob_test";
      default:
        return TestKit.super.expand(statement, args);
    }
  }

  @Override
  @Disabled("Streaming Blob values are outside the first release scope of the driver")
  public void blobInsert() {}

  @Override
  @Disabled("Streaming Blob values are outside the first release scope of the driver")
  public void blobSelect() {}

  @Override
  @Disabled("IoTDB has no CLOB type; streaming character LOBs are outside the first release scope")
  public void clobInsert() {}

  @Override
  @Disabled("IoTDB has no CLOB type; streaming character LOBs are outside the first release scope")
  public void clobSelect() {}

  @Override
  @Disabled("IoTDB does not support generated keys")
  public void returnGeneratedValues() {}

  @Override
  @Disabled("IoTDB does not support generated keys")
  public void returnGeneratedValuesFails() {}

  private static ConnectionFactory createConnectionFactory() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(ConnectionFactoryOptions.DATABASE, DATABASE)
            .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table")
            .option(IoTDBConnectionFactoryOptions.FETCH_SIZE, 10)
            .build();
    ConnectionFactory factory = ConnectionFactories.get(options);
    createDatabase();
    return factory;
  }

  private static void createDatabase() {
    Properties properties = new Properties();
    properties.setProperty("user", System.getProperty("iotdb.it.user"));
    properties.setProperty("password", System.getProperty("iotdb.it.password"));
    properties.setProperty("sql_dialect", "table");
    String url =
        "jdbc:iotdb://"
            + System.getProperty("iotdb.it.host")
            + ":"
            + Integer.getInteger("iotdb.it.port", 6667)
            + "/?sql_dialect=table";
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      try (Connection connection = DriverManager.getConnection(url, properties)) {
        try (java.sql.Statement statement = connection.createStatement()) {
          statement.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
        }
      }
    } catch (ClassNotFoundException | SQLException e) {
      throw new IllegalStateException("could not prepare the TCK database", e);
    }
  }

  private static JdbcOperations createJdbcOperations() {
    Properties properties = new Properties();
    properties.setProperty("user", System.getProperty("iotdb.it.user"));
    properties.setProperty("password", System.getProperty("iotdb.it.password"));
    properties.setProperty("sql_dialect", "table");
    String url =
        "jdbc:iotdb://"
            + System.getProperty("iotdb.it.host")
            + ":"
            + Integer.getInteger("iotdb.it.port", 6667)
            + "/"
            + DATABASE
            + "?sql_dialect=table";
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      Connection connection = DriverManager.getConnection(url, properties);
      // The TCK fixture reuses one connection; it must not be closed by every statement.
      return new JdbcTemplate(new SingleConnectionDataSource(connection, true));
    } catch (ClassNotFoundException | SQLException e) {
      throw new IllegalStateException("could not create the TCK JDBC fixture", e);
    }
  }
}
