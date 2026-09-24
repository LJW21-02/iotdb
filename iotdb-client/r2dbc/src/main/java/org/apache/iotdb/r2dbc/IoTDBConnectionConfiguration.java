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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;

import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Locale;

/**
 * Validated, immutable view of the options a connection was created with.
 *
 * <p>Values coming from a connection URL are strings, values coming from the programmatic API may
 * be instances of the documented type. All values are therefore read as {@link Object} and
 * converted defensively.
 */
public record IoTDBConnectionConfiguration(
    String host,
    int port,
    String database,
    String user,
    String password,
    TlsConfiguration tls,
    SqlDialect sqlDialect,
    ZoneId zoneId,
    Protocol protocol,
    Duration connectTimeout,
    Duration statementTimeout,
    Duration rpcTimeout,
    Duration serverQueryTimeout,
    Duration cleanupTimeout,
    int fetchSize,
    int maxFrameSize) {

  public static final String DRIVER = "iotdb";
  private static final String DEFAULT_HOST = "localhost";
  private static final int DEFAULT_PORT = 6667;
  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_RPC_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration DEFAULT_CLEANUP_TIMEOUT = Duration.ofSeconds(10);
  private static final int DEFAULT_FETCH_SIZE = 1024;
  private static final int DEFAULT_MAX_FRAME_SIZE = 24 * 1024 * 1024;

  /** Server side SQL dialect. */
  public enum SqlDialect {
    TABLE,
    TREE
  }

  /** Thrift protocol used on the wire. */
  public enum Protocol {
    BINARY,
    COMPACT
  }

  /**
   * TLS settings: whether TLS is enabled and, optionally, the trust store that verifies the server
   * certificate plus a key store for mutual TLS. Without an explicit trust store the JVM default
   * one is used.
   */
  public record TlsConfiguration(
      boolean enabled,
      String trustStore,
      String trustStorePassword,
      String keyStore,
      String keyStorePassword) {

    public static TlsConfiguration disabled() {
      return new TlsConfiguration(false, null, null, null, null);
    }
  }

  public static IoTDBConnectionConfiguration from(ConnectionFactoryOptions options) {
    String user = stringValue(options, ConnectionFactoryOptions.USER, null);
    String password = stringValue(options, ConnectionFactoryOptions.PASSWORD, user);
    requireFrameCompressionSupported(options);
    return new IoTDBConnectionConfiguration(
        stringValue(options, ConnectionFactoryOptions.HOST, DEFAULT_HOST),
        port(options),
        stringValue(options, ConnectionFactoryOptions.DATABASE, null),
        user,
        password,
        tlsConfiguration(options),
        sqlDialect(options),
        zoneId(options),
        protocol(options),
        duration(options, ConnectionFactoryOptions.CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT, false),
        duration(options, ConnectionFactoryOptions.STATEMENT_TIMEOUT, Duration.ZERO, true),
        duration(options, IoTDBConnectionFactoryOptions.RPC_TIMEOUT, DEFAULT_RPC_TIMEOUT, false),
        duration(options, IoTDBConnectionFactoryOptions.SERVER_QUERY_TIMEOUT, null, false),
        duration(
            options, IoTDBConnectionFactoryOptions.CLEANUP_TIMEOUT, DEFAULT_CLEANUP_TIMEOUT, false),
        positiveInt(options, IoTDBConnectionFactoryOptions.FETCH_SIZE, DEFAULT_FETCH_SIZE),
        positiveInt(options, IoTDBConnectionFactoryOptions.MAX_FRAME_SIZE, DEFAULT_MAX_FRAME_SIZE));
  }

  /**
   * Reads the raw SSL value and the TLS material.
   *
   * <p>The R2DBC SPI turns {@code r2dbcs:} into {@code Boolean.TRUE} but turns an explicit {@code
   * ssl=false} query parameter into the string {@code "false"}; the provider cannot tell them apart
   * afterwards, so the raw value is checked here instead of being coerced to a boolean.
   */
  private static TlsConfiguration tlsConfiguration(ConnectionFactoryOptions options) {
    Object value = options.getValue(ConnectionFactoryOptions.SSL);
    if (value == null || Boolean.FALSE.equals(value)) {
      return TlsConfiguration.disabled();
    }
    if (!Boolean.TRUE.equals(value)
        && !(value instanceof String && "true".equalsIgnoreCase(((String) value).trim()))) {
      throw new IoTDBR2dbcException(
          R2dbcMessages
              .EXCEPTION_SSL_MUST_BE_ENABLED_WITH_BOOLEAN_TRUE_THE_STRING_TRUE_CASE_INSENSITIVE_TRIMMED_OR_THE_R2DBCS_SCHEME_THE_STRING_FALSE_AND_ALL_OTHER_VALUES_ARE_REJECTED_3BD83E9E);
    }
    return new TlsConfiguration(
        true,
        optionalStringValue(options, IoTDBConnectionFactoryOptions.TRUST_STORE),
        optionalStringValue(options, IoTDBConnectionFactoryOptions.TRUST_STORE_PASSWORD),
        optionalStringValue(options, IoTDBConnectionFactoryOptions.KEY_STORE),
        optionalStringValue(options, IoTDBConnectionFactoryOptions.KEY_STORE_PASSWORD));
  }

  private static String optionalStringValue(ConnectionFactoryOptions options, Option<?> option) {
    Object value = options.getValue(option);
    if (value == null) {
      return null;
    }
    if (!(value instanceof String)) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_BE_A_STRING_BUT_WAS_ARG_817A77EE,
              option.name(),
              value));
    }
    return (String) value;
  }

  /** Only uncompressed frames are verified; Snappy must not be silently downgraded or accepted. */
  private static void requireFrameCompressionSupported(ConnectionFactoryOptions options) {
    String value = stringValue(options, IoTDBConnectionFactoryOptions.FRAME_COMPRESSION, "none");
    if (!"none".equalsIgnoreCase(value)) {
      throw new IoTDBR2dbcException(
          R2dbcMessages
              .EXCEPTION_OPTION_IOTDB_FRAMECOMPRESSION_ONLY_ACCEPTS_NONE_SNAPPY_HAS_NOT_BEEN_VERIFIED_AND_IS_REJECTED_EA6F4787);
    }
  }

  private static int port(ConnectionFactoryOptions options) {
    Object value = options.getValue(ConnectionFactoryOptions.PORT);
    int port = value == null ? DEFAULT_PORT : intValue(ConnectionFactoryOptions.PORT, value);
    if (port < 1 || port > 65535) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_PORT_MUST_BE_BETWEEN_1_AND_65535_BUT_WAS_ARG_348A6B97, port));
    }
    return port;
  }

  private static SqlDialect sqlDialect(ConnectionFactoryOptions options) {
    String value = stringValue(options, IoTDBConnectionFactoryOptions.SQL_DIALECT, "table");
    switch (value.toLowerCase(Locale.ROOT)) {
      case "table":
        return SqlDialect.TABLE;
      case "tree":
        return SqlDialect.TREE;
      default:
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages
                    .EXCEPTION_OPTION_IOTDB_SQLDIALECT_MUST_BE_TABLE_OR_TREE_BUT_WAS_ARG_BE870BD5,
                value));
    }
  }

  private static ZoneId zoneId(ConnectionFactoryOptions options) {
    String value = stringValue(options, IoTDBConnectionFactoryOptions.TIME_ZONE, "UTC");
    try {
      return ZoneId.of(value);
    } catch (RuntimeException e) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_OPTION_IOTDB_TIMEZONE_IS_NOT_A_VALID_TIME_ZONE_ARG_6C010057,
              value),
          e);
    }
  }

  private static Protocol protocol(ConnectionFactoryOptions options) {
    String value = stringValue(options, IoTDBConnectionFactoryOptions.PROTOCOL, "binary");
    switch (value.toLowerCase(Locale.ROOT)) {
      case "binary":
        return Protocol.BINARY;
      case "compact":
        return Protocol.COMPACT;
      default:
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages
                    .EXCEPTION_OPTION_IOTDB_PROTOCOL_MUST_BE_BINARY_OR_COMPACT_BUT_WAS_ARG_53E9FC70,
                value));
    }
  }

  private static String stringValue(
      ConnectionFactoryOptions options, Option<?> option, String defaultValue) {
    Object value = options.getValue(option);
    if (value == null) {
      return defaultValue;
    }
    if (!(value instanceof String)) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_BE_A_STRING_BUT_WAS_ARG_817A77EE,
              option.name(),
              value));
    }
    String text = ((String) value).trim();
    if (text.isEmpty()) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_NOT_BE_EMPTY_C5532E9C, option.name()));
    }
    return text;
  }

  private static int intValue(Option<?> option, Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    if (value instanceof String) {
      try {
        return Integer.parseInt(((String) value).trim());
      } catch (NumberFormatException ignored) {
        // fall through to the uniform error below
      }
    }
    throw new IoTDBR2dbcException(
        String.format(
            R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_BE_AN_INTEGER_BUT_WAS_ARG_1CAFFF31,
            option.name(),
            value));
  }

  private static int positiveInt(
      ConnectionFactoryOptions options, Option<Integer> option, int defaultValue) {
    Object value = options.getValue(option);
    if (value == null) {
      return defaultValue;
    }
    int parsed = intValue(option, value);
    if (parsed <= 0) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_BE_A_POSITIVE_DURATION_BUT_WAS_ARG_9DD4E8F1,
              option.name(),
              value));
    }
    return parsed;
  }

  private static Duration duration(
      ConnectionFactoryOptions options,
      Option<Duration> option,
      Duration defaultValue,
      boolean allowZero) {
    Object value = options.getValue(option);
    if (value == null) {
      return defaultValue;
    }
    Duration parsed = parseDuration(value);
    if (parsed == null || parsed.isNegative() || (!allowZero && parsed.isZero())) {
      throw new IoTDBR2dbcException(
          String.format(
              allowZero
                  ? R2dbcMessages.EXCEPTION_OPTION_ARG_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_28DA7D3A
                  : R2dbcMessages
                      .EXCEPTION_OPTION_ARG_MUST_BE_A_POSITIVE_DURATION_BUT_WAS_ARG_9DD4E8F1,
              option.name(),
              value));
    }
    return parsed;
  }

  private static Duration parseDuration(Object value) {
    if (value instanceof Duration) {
      return (Duration) value;
    }
    if (value instanceof Number) {
      try {
        return Duration.ofMillis(((Number) value).longValue());
      } catch (ArithmeticException e) {
        return null;
      }
    }
    if (value instanceof String) {
      String text = ((String) value).trim();
      try {
        return Duration.parse(text);
      } catch (DateTimeParseException parseException) {
        try {
          return Duration.ofMillis(Long.parseLong(text));
        } catch (NumberFormatException | ArithmeticException e) {
          return null;
        }
      }
    }
    return null;
  }

  @Override
  public String toString() {
    return "IoTDBConnectionConfiguration{"
        + "host='"
        + host
        + '\''
        + ", port="
        + port
        + ", database='"
        + database
        + '\''
        + ", user='"
        + user
        + '\''
        + ", password=REDACTED"
        + ", tls="
        + (tls.enabled() ? "enabled" : "disabled")
        + ", sqlDialect="
        + sqlDialect
        + ", zoneId="
        + zoneId
        + ", protocol="
        + protocol
        + ", connectTimeout="
        + connectTimeout
        + ", statementTimeout="
        + statementTimeout
        + ", rpcTimeout="
        + rpcTimeout
        + ", serverQueryTimeout="
        + serverQueryTimeout
        + ", cleanupTimeout="
        + cleanupTimeout
        + ", fetchSize="
        + fetchSize
        + ", maxFrameSize="
        + maxFrameSize
        + '}';
  }
}
