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
import org.apache.thrift.transport.TTransportException;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Checks the frame guard against a TLS enabled instance: the endpoint must really speak TLS, and a
 * plaintext driver connection to it must fail fast with a bounded frame error instead of allocating
 * a buffer for whatever length the TLS handshake bytes happen to encode (the prototype OOM case).
 *
 * <p>Needs a TLS enabled instance and is skipped unless the TLS options are given:
 *
 * <pre>
 * mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcTlsIT -DfailIfNoTests=false \
 *     -Diotdb.it.host=127.0.0.1 -Diotdb.it.tls.port=16667 -Diotdb.it.user=&lt;user&gt; \
 *     -Diotdb.it.password=&lt;password&gt; -Diotdb.it.tls.trustStore=/path/trust.p12 \
 *     -Diotdb.it.tls.trustStorePassword=&lt;password&gt;
 * </pre>
 */
public class IoTDBR2dbcTlsIT {

  @BeforeClass
  public static void requireTlsServer() {
    Assume.assumeTrue(System.getProperty("iotdb.it.tls.port") != null);
    Assume.assumeTrue(System.getProperty("iotdb.it.tls.trustStore") != null);
  }

  @Test
  public void driverConnectsOverTlsAndPagesResults() {
    ConnectionFactory factory = tlsFactory(System.getProperty("iotdb.it.tls.trustStore"));
    Connection connection = Mono.from(factory.create()).block();
    try {
      execute(connection, "CREATE DATABASE IF NOT EXISTS r2dbc_tls");
      execute(
          connection,
          "CREATE TABLE IF NOT EXISTS r2dbc_tls.samples(device STRING TAG, v INT32 FIELD)");
      execute(connection, "DELETE FROM r2dbc_tls.samples");
      long base = System.currentTimeMillis();
      for (int start = 1; start <= 120; start += 40) {
        StringBuilder insert =
            new StringBuilder("INSERT INTO r2dbc_tls.samples(time,device,v) VALUES ");
        for (int row = start; row < start + 40 && row <= 120; row++) {
          if (row > start) {
            insert.append(',');
          }
          insert.append('(').append(base + row).append(",'d1',").append(row).append(')');
        }
        execute(connection, insert.toString());
      }
      List<Row> rows =
          Flux.from(
                  connection
                      .createStatement("SELECT time, v FROM r2dbc_tls.samples ORDER BY time")
                      .fetchSize(7)
                      .execute())
              .flatMap(result -> result.map((row, metadata) -> row))
              .collectList()
              .block();
      assertEquals(120, rows.size());
      long sum = 0;
      for (Row row : rows) {
        sum += row.get("v", Integer.class);
      }
      assertEquals(120L * 121 / 2, sum);

      // Repeated frames and concurrent TLS connections exercise the record handling and the
      // partial write accounting.
      for (int i = 0; i < 25; i++) {
        assertEquals(
            120,
            Flux.from(
                    connection
                        .createStatement("SELECT v FROM r2dbc_tls.samples ORDER BY time")
                        .execute())
                .flatMap(result -> result.map((row, metadata) -> row))
                .collectList()
                .block()
                .size());
      }
      List<Connection> concurrent = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        concurrent.add(Mono.from(factory.create()).block());
      }
      try {
        for (Connection parallel : concurrent) {
          assertEquals(
              120,
              Flux.from(
                      parallel
                          .createStatement("SELECT v FROM r2dbc_tls.samples ORDER BY time")
                          .execute())
                  .flatMap(result -> result.map((row, metadata) -> row))
                  .collectList()
                  .block()
                  .size());
        }
      } finally {
        for (Connection parallel : concurrent) {
          Mono.from(parallel.close()).block();
        }
      }
      assertTrue(Mono.from(connection.validate(ValidationDepth.REMOTE)).block());
    } finally {
      Mono.from(connection.close()).block();
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void untrustedCertificateFailsWithoutDowngrading() {
    // Without a trust store the JVM default one is used, which does not trust the test certificate.
    ConnectionFactory factory = tlsFactory(null);
    assertThrows(
        RuntimeException.class,
        () -> Mono.from(factory.create()).timeout(Duration.ofSeconds(15)).block());
    ((IoTDBR2dbcConnectionFactory) factory).close();
  }

  private static ConnectionFactory tlsFactory(String trustStore) {
    ConnectionFactoryOptions.Builder builder =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.tls.port"))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .option(ConnectionFactoryOptions.SSL, Boolean.TRUE);
    if (trustStore != null) {
      builder
          .option(IoTDBConnectionFactoryOptions.TRUST_STORE, trustStore)
          .option(
              IoTDBConnectionFactoryOptions.TRUST_STORE_PASSWORD,
              System.getProperty("iotdb.it.tls.trustStorePassword"));
    }
    return ConnectionFactories.get(builder.build());
  }

  private static void execute(Connection connection, String sql) {
    Flux.from(connection.createStatement(sql).execute())
        .flatMap(Result::getRowsUpdated)
        .collectList()
        .block();
  }

  @Test
  public void tlsEndpointRejectsAPlaintextConnectionWithABoundedFrameError() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, System.getProperty("iotdb.it.host"))
            .option(ConnectionFactoryOptions.PORT, Integer.getInteger("iotdb.it.tls.port"))
            .option(ConnectionFactoryOptions.USER, System.getProperty("iotdb.it.user"))
            .option(ConnectionFactoryOptions.PASSWORD, System.getProperty("iotdb.it.password"))
            .build();
    ConnectionFactory factory = ConnectionFactories.get(options);
    try {
      RuntimeException error =
          assertThrows(
              RuntimeException.class,
              () -> Mono.from(factory.create()).timeout(Duration.ofSeconds(10)).block());
      Throwable root = error;
      while (root.getCause() != null) {
        root = root.getCause();
      }
      assertTrue(
          "the connection must fail with a controlled transport error, but was " + root,
          root instanceof TTransportException);
      assertTrue(
          "the frame guard must reject the oversized length, message was: " + root.getMessage(),
          root.getMessage().contains("frame"));
    } finally {
      ((IoTDBR2dbcConnectionFactory) factory).close();
    }
  }

  @Test
  public void tlsEndpointReallySpeaksTls() throws Exception {
    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    try (InputStream input =
        Files.newInputStream(Path.of(System.getProperty("iotdb.it.tls.trustStore")))) {
      trustStore.load(input, System.getProperty("iotdb.it.tls.trustStorePassword").toCharArray());
    }
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagerFactory.init(trustStore);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, trustManagerFactory.getTrustManagers(), null);
    try (SSLSocket socket =
        (SSLSocket)
            context
                .getSocketFactory()
                .createSocket(
                    System.getProperty("iotdb.it.host"), Integer.getInteger("iotdb.it.tls.port"))) {
      // Throws when the endpoint is not TLS or the certificate is not trusted.
      socket.startHandshake();
      assertTrue(socket.getSession().isValid());
    }
  }
}
