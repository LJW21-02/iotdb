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
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Minimal Spring Boot sample: the driver is discovered through the ServiceLoader, Boot creates the
 * {@code ConnectionFactory} bean (through this module's {@code Lifecycle} when a pool is present)
 * and the application runs explicit SQL through a {@code DatabaseClient}.
 *
 * <p>Needs the {@code with-r2dbc-spring} profile and a live instance:
 *
 * <pre>
 * mvn test -pl iotdb-client/r2dbc -P with-r2dbc-spring -Dtest=IoTDBR2dbcSpringBootSampleIT \
 *     -DfailIfNoTests=false -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667 \
 *     -Diotdb.it.user=&lt;user&gt; -Diotdb.it.password=&lt;password&gt;
 * </pre>
 *
 * <p>Spring's default {@code DatabaseClient} auto configuration cannot build a client for a driver
 * that publishes no Spring types, so the sample provides it explicitly with named parameters
 * disabled; the failing default path is asserted as well.
 */
public class IoTDBR2dbcSpringBootSampleIT {

  private static final String DATABASE = "r2dbc_spring";

  @BeforeClass
  public static void requireServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @Test
  public void bootStartsAndRunsExplicitSqlThroughDatabaseClient() {
    createDatabase();
    try (ConfigurableApplicationContext context = boot(SampleConfiguration.class)) {
      DatabaseClient client = context.getBean(DatabaseClient.class);
      // The explicit client runs DDL/DML and a parameterised query.
      client
          .sql(
              "CREATE TABLE IF NOT EXISTS "
                  + DATABASE
                  + ".samples(device STRING TAG, v INT32 FIELD)")
          .fetch()
          .rowsUpdated()
          .block();
      client.sql("DELETE FROM " + DATABASE + ".samples").fetch().rowsUpdated().block();
      client
          .sql(
              "INSERT INTO "
                  + DATABASE
                  + ".samples(time,device,v) VALUES (1,'s1',11),(2,'s1',22),(3,'s1',33)")
          .fetch()
          .rowsUpdated()
          .block();
      Long rows =
          client
              .sql("SELECT v FROM " + DATABASE + ".samples ORDER BY time")
              .map((row, metadata) -> row.get("v", Integer.class))
              .all()
              .count()
              .block();
      assertEquals(Long.valueOf(3), rows);
      assertEquals(
          Integer.valueOf(22),
          client
              .sql("SELECT v FROM " + DATABASE + ".samples WHERE v > ? ORDER BY time")
              .bind(0, 11)
              .map((row, metadata) -> row.get("v", Integer.class))
              .first()
              .block());
    }
  }

  @Test
  public void defaultDatabaseClientAutoConfigurationFailsAsDocumented() {
    createDatabase();
    RuntimeException error =
        assertThrows(RuntimeException.class, () -> boot(DefaultConfiguration.class));
    assertTrueContains(error, "BindMarkersFactory");
  }

  private static ConfigurableApplicationContext boot(Class<?> configuration) {
    return new SpringApplicationBuilder(configuration)
        .web(WebApplicationType.NONE)
        .properties(
            "spring.r2dbc.url=r2dbc:iotdb://"
                + System.getProperty("iotdb.it.host")
                + ":"
                + Integer.getInteger("iotdb.it.port", 6667)
                + "/"
                + DATABASE,
            "spring.r2dbc.username=" + System.getProperty("iotdb.it.user"),
            "spring.r2dbc.password=" + System.getProperty("iotdb.it.password"),
            "spring.main.banner-mode=off",
            "logging.level.root=ERROR")
        .run();
  }

  private static void assertTrueContains(Throwable error, String expected) {
    Throwable current = error;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) {
        return;
      }
      current = current.getCause();
    }
    throw new AssertionError("expected a failure containing '" + expected + "', but was " + error);
  }

  /** Sample application: an explicit client because the driver publishes no Spring types. */
  @Configuration
  @EnableAutoConfiguration
  static class SampleConfiguration {

    @Bean
    DatabaseClient databaseClient(ConnectionFactory connectionFactory) {
      return DatabaseClient.builder()
          .connectionFactory(connectionFactory)
          .namedParameters(false)
          .build();
    }
  }

  /**
   * Application without an explicit client; used to assert the documented auto configuration gap.
   */
  @Configuration
  @EnableAutoConfiguration
  static class DefaultConfiguration {}

  private static void createDatabase() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .build();
    ConnectionFactory factory = ConnectionFactories.get(options);
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS " + DATABASE + ".samples(device STRING TAG, v INT32 FIELD)");
      execute(connection, "DELETE FROM " + DATABASE + ".samples");
    } finally {
      Mono.from(connection.close()).block();
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
