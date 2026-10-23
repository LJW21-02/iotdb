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

import io.r2dbc.spi.Option;

import java.time.Duration;

/** Vendor specific options accepted by the IoTDB R2DBC driver. */
public final class IoTDBConnectionFactoryOptions {

  /** SQL dialect used for the session, {@code table} (default) or {@code tree}. */
  public static final Option<String> SQL_DIALECT = Option.valueOf("iotdb.sqlDialect");

  /** Time zone used to convert {@code LocalDateTime} values, defaults to UTC. */
  public static final Option<String> TIME_ZONE = Option.valueOf("iotdb.timeZone");

  /** Thrift protocol, {@code binary} (default) or {@code compact}. */
  public static final Option<String> PROTOCOL = Option.valueOf("iotdb.protocol");

  /** Thrift frame compression, only {@code none} is verified. */
  public static final Option<String> FRAME_COMPRESSION = Option.valueOf("iotdb.frameCompression");

  /** Hint for the number of rows the server returns per page, defaults to 1024. */
  public static final Option<Integer> FETCH_SIZE = Option.valueOf("iotdb.fetchSize");

  /** Upper bound for an accepted response frame, defaults to 24 MiB. */
  public static final Option<Integer> MAX_FRAME_SIZE = Option.valueOf("iotdb.maxFrameSize");

  /** Budget for a single RPC, defaults to 60 seconds. */
  public static final Option<Duration> RPC_TIMEOUT = Option.valueOf("iotdb.rpcTimeout");

  /** Server side query budget, omitted by default. */
  public static final Option<Duration> SERVER_QUERY_TIMEOUT =
      Option.valueOf("iotdb.serverQueryTimeout");

  /** Budget for closing a query handle, defaults to 10 seconds. */
  public static final Option<Duration> CLEANUP_TIMEOUT = Option.valueOf("iotdb.cleanupTimeout");

  /**
   * Retained for configuration compatibility; execution responses are not automatically replayed.
   */
  public static final Option<Integer> MAX_REDIRECTS = Option.valueOf("iotdb.maxRedirects");

  /** PKCS#12/JKS trust store used to verify the server certificate; defaults to the JVM store. */
  public static final Option<String> TRUST_STORE = Option.valueOf("iotdb.trustStore");

  /** Password of {@link #TRUST_STORE}. */
  public static final Option<String> TRUST_STORE_PASSWORD =
      Option.sensitiveValueOf("iotdb.trustStorePassword");

  /** Key store with the client certificate for mutual TLS. */
  public static final Option<String> KEY_STORE = Option.valueOf("iotdb.keyStore");

  /** Password of {@link #KEY_STORE}. */
  public static final Option<String> KEY_STORE_PASSWORD =
      Option.sensitiveValueOf("iotdb.keyStorePassword");

  private IoTDBConnectionFactoryOptions() {}
}
