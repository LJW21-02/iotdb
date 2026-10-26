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
package org.apache.iotdb.r2dbc.spring;

import org.apache.iotdb.r2dbc.IoTDBR2dbcConnectionFactory;
import org.apache.iotdb.r2dbc.spring.sample.Reading;
import org.apache.iotdb.r2dbc.spring.sample.ReadingRepository;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/** Runs Boot starter-data-r2dbc automatic wiring and real Spring Data repository queries. */
public class IoTDBSpringDataRepositoryIT {
  private static final String DATABASE = "r2dbc_spring_data";
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  @BeforeClass
  public static void requireServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.host") != null);
  }

  @Test
  public void bootDiscoversTheDialectAndExecutesRepositoriesWithoutFakeCounts() {
    provision();
    try (ConfigurableApplicationContext context =
        new SpringApplicationBuilder(Application.class)
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
                "spring.r2dbc.pool.initial-size=0",
                "spring.main.banner-mode=off",
                "logging.level.root=ERROR")
            .run()) {
      ConnectionFactory factory = context.getBean(ConnectionFactory.class);
      assertSame(IoTDBDialect.INSTANCE, DialectResolver.getDialect(factory));
      assertNotNull(context.getBean(R2dbcEntityTemplate.class));
      DatabaseClient client = context.getBean(DatabaseClient.class);
      ReadingRepository repository = context.getBean(ReadingRepository.class);
      repository.insert(time(1), 10).block(TIMEOUT);
      repository.insert(time(2), 20).block(TIMEOUT);
      repository.insert(time(3), 30).block(TIMEOUT);
      repository.insert(time(4), 40).block(TIMEOUT);

      assertEquals(new Reading(time(2), 20), repository.findByTime(time(2)).block(TIMEOUT));
      assertEquals(Long.valueOf(3), repository.countByValueGreaterThan(10).block(TIMEOUT));
      assertEquals(
          List.of(new Reading(time(2), 20), new Reading(time(3), 30)),
          repository
              .findByValueGreaterThanOrderByTimeAsc(10, PageRequest.of(0, 2))
              .collectList()
              .block(TIMEOUT));
      assertEquals(
          List.of(new Reading(time(4), 40)),
          repository
              .findByValueGreaterThanOrderByTimeAsc(10, PageRequest.of(1, 2))
              .collectList()
              .block(TIMEOUT));
      assertEquals(
          List.of(new Reading(time(2), 20)),
          repository.findWithRepeatedParameter(20).collectList().block(TIMEOUT));
      assertEquals(
          Integer.valueOf(30),
          client
              .sql("SELECT v FROM readings WHERE time = :time")
              .bind("time", time(3))
              .map(row -> row.get("v", Integer.class))
              .one()
              .block(TIMEOUT));

      // Inspect the raw driver counts; Spring's rowsUpdated() aggregates an empty stream to zero.
      assertEquals(
          List.of(),
          client
              .inConnectionMany(
                  connection ->
                      Flux.from(
                              connection
                                  .createStatement("INSERT INTO readings(time,v) VALUES (5,50)")
                                  .execute())
                          .concatMap(Result::getRowsUpdated))
              .collectList()
              .block(TIMEOUT));
      repository.deleteThrough(time(2)).block(TIMEOUT);
      assertEquals(Long.valueOf(3), repository.countByValueGreaterThan(0).block(TIMEOUT));
      assertThrows(
          RuntimeException.class, () -> client.sql("THIS IS NOT SQL").then().block(TIMEOUT));
    }
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @AutoConfigurationPackage(basePackageClasses = ReadingRepository.class)
  static class Application {}

  private static LocalDateTime time(long millis) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
  }

  private static void provision() {
    ConnectionFactory factory =
        ConnectionFactories.get(
            ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "iotdb")
                .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
                .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.port", 6667))
                .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
                .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
                .build());
    try {
      Connection connection = Mono.from(factory.create()).block(TIMEOUT);
      try {
        execute(connection, "CREATE DATABASE IF NOT EXISTS " + DATABASE);
        execute(connection, "CREATE TABLE IF NOT EXISTS " + DATABASE + ".readings(v INT32 FIELD)");
        execute(connection, "DELETE FROM " + DATABASE + ".readings");
      } finally {
        Mono.from(connection.close()).block(TIMEOUT);
      }
    } finally {
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .concatMap(Result::getRowsUpdated)
        .then()
        .block(TIMEOUT);
  }
}
