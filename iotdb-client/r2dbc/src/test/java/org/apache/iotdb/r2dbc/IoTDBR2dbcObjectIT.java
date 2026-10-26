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

import org.apache.iotdb.rpc.TSStatusCode;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.ValidationDepth;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the community server's OBJECT boundary through real RPCs.
 *
 * <p>This server rejects stored OBJECT columns, so this suite does not claim to upload or read a
 * stored object. A typed NULL expression still exercises OBJECT result metadata and row decoding
 * alongside an ordinary stored value. Enabled with the same {@code iotdb.it.*} options as {@link
 * IoTDBR2dbcIT}.
 */
public class IoTDBR2dbcObjectIT {

  private static final String DATABASE = "r2dbc_object_it";

  @BeforeClass
  public static void requireServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @Test
  public void communityServerRejectsStoredObjectColumnsWithoutInvalidatingTheConnection() {
    IoTDBR2dbcConnectionFactory factory = factory();
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      R2dbcException failure =
          assertThrows(
              R2dbcException.class,
              () ->
                  execute(
                      connection,
                      "CREATE TABLE "
                          + DATABASE
                          + ".stored_object(device STRING TAG, payload OBJECT FIELD, v INT32 FIELD)"));
      // The DDL visitor wraps MetadataException as SemanticException without retaining its cause;
      // the RPC error handler therefore exposes SEMANTIC_ERROR, not the inner metadata code.
      assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), failure.getErrorCode());
      assertTrue(failure instanceof R2dbcBadGrammarException);
      assertTrue(failure.getMessage().toLowerCase(Locale.ROOT).contains("object"));
      assertTrue(Mono.from(connection.validate(ValidationDepth.REMOTE)).block());
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS " + DATABASE + ".usable_after_rejection(v INT32 FIELD)");
      execute(connection, "DROP TABLE " + DATABASE + ".usable_after_rejection");
    } finally {
      Mono.from(connection.close()).block();
      factory.close();
    }
  }

  @Test
  public void nullObjectExpressionKeepsItsMetadataAndOtherStoredColumnsReadable() {
    IoTDBR2dbcConnectionFactory factory = factory();
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS " + DATABASE + ".samples(device STRING TAG, v INT32 FIELD)");
      execute(connection, "DELETE FROM " + DATABASE + ".samples");
      execute(
          connection,
          "INSERT INTO " + DATABASE + ".samples(time,device,v) VALUES (1,'d1',7),(2,'d1',11)");

      List<Integer> values =
          Flux.from(
                  connection
                      .createStatement(
                          "SELECT CAST(NULL AS OBJECT) AS payload, v FROM "
                              + DATABASE
                              + ".samples ORDER BY time")
                      .execute())
              .concatMap(
                  result ->
                      result.map(
                          (row, metadata) -> {
                            assertEquals(
                                R2dbcType.VARCHAR, metadata.getColumnMetadata("payload").getType());
                            assertEquals(
                                String.class, metadata.getColumnMetadata("payload").getJavaType());
                            assertEquals(
                                TSDataType.OBJECT,
                                metadata.getColumnMetadata("payload").getNativeTypeMetadata());
                            assertNull(row.get("payload"));
                            assertNull(row.get("payload", String.class));
                            return row.get("v", Integer.class);
                          }))
              .collectList()
              .block();
      assertEquals(List.of(7, 11), values);
      assertTrue(Mono.from(connection.validate(ValidationDepth.REMOTE)).block());
    } finally {
      Mono.from(connection.close()).block();
      factory.close();
    }
  }

  private static IoTDBR2dbcConnectionFactory factory() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table")
            .option(IoTDBConnectionFactoryOptions.FETCH_SIZE, 1)
            .build();
    return (IoTDBR2dbcConnectionFactory) ConnectionFactories.get(options);
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .concatMap(Result::getRowsUpdated)
        .then()
        .block();
  }
}
