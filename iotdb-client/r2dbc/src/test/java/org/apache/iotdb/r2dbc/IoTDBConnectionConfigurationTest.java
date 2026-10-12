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

import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration.Protocol;
import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration.SqlDialect;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IoTDBConnectionConfigurationTest {

  private static IoTDBConnectionConfiguration from(String url) {
    return IoTDBConnectionConfiguration.from(ConnectionFactoryOptions.parse(url));
  }

  @Test
  public void defaultsMatchTheDocumentedContract() {
    IoTDBConnectionConfiguration configuration = from("r2dbc:iotdb://db.example:6667/demo");
    assertEquals("db.example", configuration.host());
    assertEquals(6667, configuration.port());
    assertEquals("demo", configuration.database());
    assertEquals(SqlDialect.TABLE, configuration.sqlDialect());
    assertEquals(ZoneId.of("UTC"), configuration.zoneId());
    assertEquals(Protocol.BINARY, configuration.protocol());
    assertEquals(Duration.ofSeconds(10), configuration.connectTimeout());
    assertEquals(Duration.ZERO, configuration.statementTimeout());
    assertEquals(Duration.ofSeconds(60), configuration.rpcTimeout());
    assertNull(configuration.serverQueryTimeout());
    assertEquals(1024, configuration.fetchSize());
  }

  @Test
  public void urlQueryValuesAreConvertedDefensively() {
    IoTDBConnectionConfiguration configuration =
        from(
            "r2dbc:iotdb://127.0.0.1:6667/demo?iotdb.sqlDialect=tree&iotdb.fetchSize=512"
                + "&iotdb.protocol=compact&iotdb.timeZone=Asia/Shanghai&connectTimeout=PT3S"
                + "&iotdb.rpcTimeout=1500");
    assertEquals(SqlDialect.TREE, configuration.sqlDialect());
    assertEquals(512, configuration.fetchSize());
    assertEquals(Protocol.COMPACT, configuration.protocol());
    assertEquals(ZoneId.of("Asia/Shanghai"), configuration.zoneId());
    assertEquals(Duration.ofSeconds(3), configuration.connectTimeout());
    assertEquals(Duration.ofMillis(1500), configuration.rpcTimeout());
  }

  @Test
  public void programmaticValuesAreAccepted() {
    ConnectionFactoryOptions options =
        ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "iotdb")
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.PORT, 6667)
            .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.ofSeconds(2))
            .option(IoTDBConnectionFactoryOptions.FETCH_SIZE, 100)
            .option(IoTDBConnectionFactoryOptions.SERVER_QUERY_TIMEOUT, Duration.ofSeconds(5))
            .build();
    IoTDBConnectionConfiguration configuration = IoTDBConnectionConfiguration.from(options);
    assertEquals(Duration.ofSeconds(2), configuration.connectTimeout());
    assertEquals(100, configuration.fetchSize());
    assertEquals(Duration.ofSeconds(5), configuration.serverQueryTimeout());
  }

  @Test
  public void tlsIsEnabledForBothEntryPoints() {
    assertTrue(from("r2dbcs:iotdb://127.0.0.1:6667/demo").tls().enabled());
    assertTrue(from("r2dbc:iotdb://127.0.0.1:6667/demo?ssl=true").tls().enabled());
    assertFalse(from("r2dbc:iotdb://127.0.0.1:6667/demo").tls().enabled());

    IoTDBConnectionConfiguration configured =
        from(
            "r2dbc:iotdb://127.0.0.1:6667/demo?ssl=true&iotdb.trustStore=/tmp/trust.p12"
                + "&iotdb.trustStorePassword=secret&iotdb.keyStore=/tmp/key.p12"
                + "&iotdb.keyStorePassword=keysecret");
    assertEquals("/tmp/trust.p12", configured.tls().trustStore());
    assertEquals("secret", configured.tls().trustStorePassword());
    assertEquals("/tmp/key.p12", configured.tls().keyStore());
  }

  @Test
  public void explicitSslFalseIsRejectedAsRequiredByTheSpiContract() {
    // The SPI turns r2dbcs into Boolean.TRUE but keeps an explicit ssl=false string, so the raw
    // value must be whitelisted instead of being coerced to a boolean.
    assertThrows(
        IoTDBR2dbcException.class, () -> from("r2dbcs:iotdb://127.0.0.1:6667/demo?ssl=false"));
    assertThrows(
        IoTDBR2dbcException.class, () -> from("r2dbc:iotdb://127.0.0.1:6667/demo?ssl=FALSE"));
    assertThrows(
        IoTDBR2dbcException.class, () -> from("r2dbc:iotdb://127.0.0.1:6667/demo?ssl=maybe"));
  }

  @Test
  public void invalidOptionsAreRejected() {
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?iotdb.sqlDialect=x"));
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?iotdb.protocol=x"));
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?iotdb.frameCompression=snappy"));
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?iotdb.timeZone=Mars/Olympus"));
    // An integer option must not be reported as a Duration, and the other way round. Both
    // expectations are built from the message constants, so they hold in the zh build as well.
    IoTDBR2dbcException nonPositiveFetchSize =
        assertThrows(
            IoTDBR2dbcException.class,
            () -> from("r2dbc:iotdb://localhost:6667/demo?iotdb.fetchSize=0"));
    assertEquals(
        String.format(
            R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_BE_A_POSITIVE_INTEGER_BUT_WAS_ARG_389AAC64,
            "iotdb.fetchSize",
            "0"),
        nonPositiveFetchSize.getMessage());
    IoTDBR2dbcException zeroConnectTimeout =
        assertThrows(
            IoTDBR2dbcException.class,
            () -> from("r2dbc:iotdb://localhost:6667/demo?connectTimeout=0"));
    assertEquals(
        String.format(
            R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_BE_A_POSITIVE_DURATION_BUT_WAS_ARG_9DD4E8F1,
            "connectTimeout",
            "0"),
        zeroConnectTimeout.getMessage());
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?iotdb.fetchSize=many"));
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?connectTimeout=-PT1S"));
    assertThrows(IoTDBR2dbcException.class, () -> from("r2dbc:iotdb://localhost:0/demo"));
  }

  @Test
  public void positiveTimeoutIsAcceptedAndZeroIsRejectedForRpcTimeout() {
    assertEquals(
        Duration.ofSeconds(1),
        from("r2dbc:iotdb://localhost:6667/demo?iotdb.rpcTimeout=1000").rpcTimeout());
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?iotdb.rpcTimeout=0"));
  }

  @Test
  public void statementTimeoutMayBeZeroButNotNegative() {
    assertEquals(Duration.ZERO, from("r2dbc:iotdb://localhost:6667/demo").statementTimeout());
    assertThrows(
        IoTDBR2dbcException.class,
        () -> from("r2dbc:iotdb://localhost:6667/demo?statementTimeout=-PT1S"));
  }

  @Test
  public void credentialsNeverAppearInToString() {
    IoTDBConnectionConfiguration configuration =
        from("r2dbc:iotdb://user:secret@localhost:6667/demo");
    assertFalse(configuration.toString().contains("secret"));
    assertTrue(configuration.toString().contains("REDACTED"));
  }
}
