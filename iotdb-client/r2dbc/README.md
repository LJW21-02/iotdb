<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# IoTDB R2DBC driver

A non-blocking [R2DBC SPI 1.0](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/) driver for Apache
IoTDB (module `iotdb-client/r2dbc`, artifact `iotdb-r2dbc`).

The driver uses the generated Thrift protocol and the Thrift async client on top of a dedicated
selector thread, so no waiting thread is consumed per request. Response frames are validated against
a size limit before Thrift allocates memory for them, and user callbacks plus TsBlock decoding run on
a bounded scheduler instead of the shared selector thread.

Design and rollout plan: <https://timechor.feishu.cn/docx/ESSBdGXymoJ1OlxQUjQcC5egnFf>.

## Usage

```java
ConnectionFactoryOptions options =
    ConnectionFactoryOptions.builder()
        .option(ConnectionFactoryOptions.DRIVER, "iotdb")
        .option(ConnectionFactoryOptions.HOST, "127.0.0.1")
        .option(ConnectionFactoryOptions.PORT, 6667)
        .option(ConnectionFactoryOptions.DATABASE, "demo")
        .option(ConnectionFactoryOptions.USER, "root")
        .option(ConnectionFactoryOptions.PASSWORD, "root")
        .option(IoTDBConnectionFactoryOptions.SQL_DIALECT, "table")
        .build();

ConnectionFactory factory = ConnectionFactories.get(options);
Mono.from(factory.create())
    .flatMapMany(
        connection ->
            Flux.from(connection.createStatement("SELECT v FROM demo.samples WHERE v > ?").bind(1, 100).execute())
                .flatMap(result -> result.map((row, metadata) -> row.get("v", Integer.class)))
                .doFinally(signal -> Mono.from(connection.close()).subscribe()))
    .subscribe(System.out::println);
```

The connection URL form is also supported: `r2dbc:iotdb://user:password@host:6667/database`. TLS
URLs (`r2dbcs:`) are rejected at configuration time, see the capability matrix.

## Options

| Option | Type | Default | Notes |
|-|-|-|-|
| `host` / `port` | String / Integer | `localhost` / `6667` | Standard SPI options. |
| `database` | String | none | Sent as the session database (`db`). |
| `user` / `password` | String | none | Never logged; `toString()` redacts the password. |
| `iotdb.sqlDialect` | String | `table` | `table` or `tree`; the SQL is never rewritten. |
| `iotdb.timeZone` | String | `UTC` | `LocalDateTime` conversion zone. |
| `iotdb.protocol` | String | `binary` | `binary` or `compact`. |
| `iotdb.frameCompression` | String | `none` | Only `none`; `snappy` is rejected. |
| `iotdb.fetchSize` | Integer | `1024` | Page size hint, not a memory bound. |
| `iotdb.maxFrameSize` | Integer | `25165824` (24 MiB) | Stricter of this and the server announced limit wins. |
| `connectTimeout` | Duration | `10s` | Total budget for connect + session setup. |
| `statementTimeout` | Duration | `ZERO` | Until the first execution response; `ZERO` disables it. |
| `iotdb.rpcTimeout` | Duration | `60s` | Per RPC budget; running out of it is reported as `R2dbcTimeoutException` and marks the connection as broken. |
| `iotdb.serverQueryTimeout` | Duration | none | Server side budget; absent follows the server default. |
| `iotdb.cleanupTimeout` | Duration | `10s` | Cursor/handle cleanup budget. |
| `iotdb.maxRedirects` | Integer | `3` | How many server recommended redirections one statement may follow. |
| `ssl` / `r2dbcs:` | Boolean or string `true` | disabled | Enables TLS; the string `false` and any other value are rejected. |
| `iotdb.trustStore` / `iotdb.trustStorePassword` | String | JVM default trust store | Certificate chain verification; the host name is always verified against the certificate. |
| `iotdb.keyStore` / `iotdb.keyStorePassword` | String | none | Client certificate for mutual TLS. |

Durations accept an ISO-8601 value or an integer number of milliseconds. Values from a URL are
strings and are converted defensively; invalid, negative or conflicting values fail before any
connection is opened.

## Capability matrix

| Capability | State |
|-|-|
| Table model, `SELECT` / `INSERT` / DDL | Supported, verified end to end. |
| Tree model queries and writes | Supported; the time column is exposed as `Time` (`TIMESTAMP`), the default `LocalDateTime`, with `get(..., Long.class)`/`get(..., Instant.class)` for raw ticks and instants. |
| Typed parameters | Table model queries with `String`, `Boolean`, `Byte`/`Short`/`Integer`, `Long`, `Float`, `Double` and `Instant`/`LocalDateTime`, using the server side prepare RPC. |
| Time parameters | `Instant` and `LocalDateTime` are converted to epoch ticks in the session precision (`iotdb.timeZone` resolves `LocalDateTime`); ambiguous DST times and precision loss are rejected. An ISO-8601 string is *not* accepted by the server for a `TIMESTAMP` comparison (`Cannot apply operator: TIMESTAMP < STRING`), so ticks are the only verified form. |
| Literal parameters | All statements and dialects for the types above; placeholders inside strings, identifiers and comments are never rewritten. |
| `DATE` parameters | Not supported: this server version rejects both `DATE < STRING` and `DATE < INT32` comparisons, so binding a `LocalDate` fails at bind time instead of being sent. Rows of a `DATE` column are decoded with the TsFile date encoding. |
| Connection pool | Verified with `r2dbc-pool` 1.0.2: borrow, `LOCAL` validation, release, lifecycle reset and reuse of the same connection. |
| Prepared statements | One server side handle per execution; a statement that fails after prepare is deallocated, so the next execution cannot collide with a leaked handle. |
| Parameters | Zero-based positional indexes as required by the SPI; `io.r2dbc.spi.Parameter` wrappers (used by frameworks) are unwrapped and typed nulls become `bindNull`. |
| Batch | Parameter free statements run in order and stop at the first failure; effects of already executed statements are kept. |
| Validation | `LOCAL` checks the local session state, `REMOTE` issues a light weight `getProperties` RPC. |
| Stability | Verified against a live instance: 150 prepared executions in a row without leaking a server side handle, 300 rows paged with `fetchSize=7` without loss or duplication, demand driven consumption, 8 parallel connections sharing one selector runtime and 20 connect/close cycles that release their selector thread. |
| Backpressure | Pages are fetched only when the current page is exhausted and more rows are demanded. |
| Single result per connection | One active result; a second statement fails or waits for cleanup, never interleaves. |
| Result consumption | Once per result; `map`/`filter`/`flatMap`/`getRowsUpdated` share one consumption state. |
| Transactions | Not supported (strict): `beginTransaction`, `commitTransaction`, `rollbackTransaction`, `setAutoCommit(false)` and savepoints report an error instead of succeeding silently. |
| Update counts | Not reported by IoTDB; the count stream completes empty, `0`/`1` is never fabricated. |
| TLS (`r2dbcs:`, `ssl=true`) | Supported with a non-blocking `SSLEngine` transport: the handshake, record layer and partial writes never block the selector thread, certificate chain and host name are verified, and a TLS failure is reported as an error without falling back to plaintext. Verified against a TLS enabled instance by `IoTDBR2dbcTlsIT` (paged queries, repeated frames, concurrent connections, untrusted certificate rejection) as well as a plaintext misconnect that fails in about 0.2 s with a bounded frame error. |
| Multi node redirection | A DataNode that does not host the data recommends another node; the driver re-establishes the session there (dialect, database, time zone) and re-sends the statement, at most `iotdb.maxRedirects` hops. A response that recommends several different nodes for one request is rejected instead of replaying part of it. Verified against a 1C3D cluster by `IoTDBR2dbcClusterIT`, which also runs a typed parameter query against every DataNode. |
| Snappy, time and binary parameters, generated keys, named parameters, streaming LOB | Rejected with a clear error; none of them can silently fall back to a different path. |
| Server side cancellation | Not implemented by the server; the driver stops consuming and releases the handle. |

## Build and test

```bash
# Unit tests (no server required)
mvn test -pl iotdb-client/r2dbc -am

# End to end test against a running instance (skipped when no host is given)
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcIT -DfailIfNoTests=false \
    -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667 \
    -Diotdb.it.user=<user> -Diotdb.it.password=<password>

# Soak and timeout suites (the unresponsive server case needs the stalling proxy helper)
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcSoakIT -DfailIfNoTests=false     -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667     -Diotdb.it.user=<user> -Diotdb.it.password=<password>     -Diotdb.it.stallProxy=<path to the validation helper>/stall_proxy.py

# Frame guard against a TLS endpoint (skipped without the TLS options)
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcTlsIT -DfailIfNoTests=false     -Diotdb.it.host=127.0.0.1 -Diotdb.it.tls.port=6667     -Diotdb.it.user=<user> -Diotdb.it.password=<password>     -Diotdb.it.tls.trustStore=<trust.p12> -Diotdb.it.tls.trustStorePassword=<password>

# Formatting / style
mvn spotless:apply -pl iotdb-client/r2dbc
```

## Spring `DatabaseClient`

Spring cannot derive bind markers from a driver that publishes no Spring types, so the default
builder fails with `NoBindMarkersFactoryException`. Build the client with named parameters disabled
(or with an explicit `BindMarkersFactory`):

```java
DatabaseClient client =
    DatabaseClient.builder().connectionFactory(factory).namedParameters(false).build();

Flux<Integer> values =
    client
        .sql("SELECT v FROM demo.samples WHERE v > ? ORDER BY time")
        .bind(0, 100) // DatabaseClient with anonymous markers is zero based
        .map((row, metadata) -> row.get("v", Integer.class))
        .all();
```

### Spring Boot sample

`IoTDBR2dbcSpringBootSampleIT` (profile `with-r2dbc-spring`) starts a real Boot application context
that discovers the driver through the ServiceLoader, lets Boot build the `ConnectionFactory` bean
(including the `r2dbc-pool` wrapper) and runs explicit SQL through a `DatabaseClient` bean:

```bash
mvn test -pl iotdb-client/r2dbc -P with-r2dbc-spring -Dtest=IoTDBR2dbcSpringBootSampleIT     -DfailIfNoTests=false -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667     -Diotdb.it.user=<user> -Diotdb.it.password=<password>
```

The same test asserts the documented limitation: without the explicit bean, Boot's default
`DatabaseClient` auto configuration fails with `NoBindMarkersFactoryException`, because the driver
publishes no Spring types. Verified versions: Boot 3.3.8, Spring 6.1.13, R2DBC SPI 1.0.0.RELEASE,
r2dbc-pool 1.0.2.RELEASE, Reactor 3.7.19.

Values bound by frameworks arrive as `io.r2dbc.spi.Parameter`; the driver unwraps them, and a typed
null becomes a `bindNull` for the declared type.

## R2DBC TCK

The driver runs the official R2DBC TCK against a live instance; the per-test outcome and the
documented differences (transactions and update counts) are recorded in [TCK.md](TCK.md):

```bash
mvn test -pl iotdb-client/r2dbc -P with-r2dbc-tck -Dtest=IoTDBR2dbcTckTest -DfailIfNoTests=false     -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667     -Diotdb.it.user=<user> -Diotdb.it.password=<password>
```

## Decision record

[ADR.md](ADR.md) records why the driver uses the Thrift async client with a driver owned TLS
transport, the alternatives that were rejected, the blocking/frame/TLS evidence and the trigger for
re-evaluating the Netty option.

## Transport notes

The plaintext transport is Thrift's `TNonblockingSocket`. For TLS the driver drives an `SSLEngine`
itself (`TlsNonblockingSocket`) instead of using Thrift's non-blocking SSL socket, whose handshake
busy waits on the shared selector thread and whose read contract does not match the async client. The
TLS transport only acknowledges plaintext once the matching ciphertext reached the socket, and it
keeps the async client reading buffered plaintext by registering writability for exactly one extra
transition, which is what Thrift's two-step frame read needs.

## Performance

Median numbers, the measurement method and its limitations are recorded in
[PERFORMANCE.md](PERFORMANCE.md); they are evidence for the non-blocking design, not a throughput
promise.

## Follow-up work

Open items, tracked by the milestones of the design document above:

- A longer stability run than the recorded ten minutes (`PERFORMANCE.md`, "Long run"), ideally on a
  machine that is not shared with other work.
- A cluster run in which one DataNode is taken down on purpose. The driver side of that case is
  covered (a dead or silent server has to fail within its budget and the connection is not reused,
  `IoTDBR2dbcSoakIT`), but taking a single node out of a running cluster needs a helper that can
  restart it, which the validation scripts do not have yet.
- A write coalescing layer for the non-blocking transport. Thrift's async client writes the frame
  size and payload as two socket writes, which costs one extra transport delay per request on a high
  latency path (`PERFORMANCE.md`, "RTT arm"); on a LAN it is not visible.
- Repository level packaging checks. Verified here: the module builds to
  `iotdb-r2dbc-<version>.jar` with `META-INF/services/io.r2dbc.spi.ConnectionFactoryProvider`
  pointing at `IoTDBR2dbcConnectionFactoryProvider`, its dependencies are listed in
  `dependencies.json`, and it builds with the RAT and spotless checks enabled. The aggregate
  CycloneDX report (`-P enable-sbom-check`) is a whole reactor run and was not part of this work.
