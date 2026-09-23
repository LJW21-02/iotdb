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

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
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

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the {@code r2dbc-pool} integration: borrow, validate, release and reuse.
 *
 * <p>Skipped unless {@code -Diotdb.it.host} is given, like {@link IoTDBR2dbcIT}.
 */
public class IoTDBR2dbcPoolIT {

  private static final String DATABASE = "r2dbc_pool_e2e";

  @BeforeClass
  public static void requireServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @Test
  public void pooledConnectionsAreValidatedAndReused() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(IoTDBConnectionFactoryOptions.FETCH_SIZE, 2)
            .build();
    ConnectionFactory factory = ConnectionFactories.get(options);
    ConnectionPool pool =
        new ConnectionPool(
            ConnectionPoolConfiguration.builder(factory).initialSize(1).maxSize(2).build());
    try {
      Connection connection = Mono.from(pool.create()).block();
      try {
        execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
        execute(
            connection,
            "CREATE TABLE IF NOT EXISTS "
                + DATABASE
                + ".samples(device STRING TAG, v INT32 FIELD)");
        execute(
            connection,
            "INSERT INTO "
                + DATABASE
                + ".samples(time,device,v) VALUES (1,'d1',11),(2,'d1',22),(3,'d1',33)");
      } finally {
        Mono.from(connection.close()).block();
      }

      long allocatedSize =
          pool.getMetrics()
              .orElseThrow(() -> new AssertionError("pool metrics unavailable"))
              .allocatedSize();
      for (int round = 0; round < 3; round++) {
        Connection borrowed = Mono.from(pool.create()).block();
        try {
          assertTrue(Mono.from(borrowed.validate(ValidationDepth.LOCAL)).block());
          List<Row> rows =
              Flux.from(
                      borrowed
                          .createStatement("SELECT v FROM " + DATABASE + ".samples ORDER BY time")
                          .execute())
                  .flatMap(result -> result.map((row, metadata) -> row))
                  .collectList()
                  .block();
          assertEquals(3, rows.size());
          assertEquals(Integer.valueOf(11), rows.get(0).get("v", Integer.class));
        } finally {
          Mono.from(borrowed.close()).block();
        }
      }
      assertEquals(
          "the pool must reuse the connection instead of creating new ones",
          allocatedSize,
          pool.getMetrics()
              .orElseThrow(() -> new AssertionError("pool metrics unavailable"))
              .allocatedSize());
      assertEquals(
          0,
          pool.getMetrics()
              .orElseThrow(() -> new AssertionError("pool metrics unavailable"))
              .acquiredSize());
    } finally {
      pool.dispose();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .block();
  }
}
