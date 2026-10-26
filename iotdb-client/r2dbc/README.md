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

try (IoTDBR2dbcConnectionFactory factory =
    (IoTDBR2dbcConnectionFactory) ConnectionFactories.get(options)) {
  Flux.usingWhen(
          Mono.from(factory.create()),
          connection ->
              Flux.from(
                      connection
                          .createStatement("SELECT v FROM demo.samples WHERE v > ?")
                          .bind(0, 100)
                          .execute())
                  .flatMap(result -> result.map((row, metadata) -> row.get("v", Integer.class))),
          connection -> Mono.from(connection.close()))
      .doOnNext(System.out::println)
      .blockLast(); // Wait only at this standalone application's boundary.
}
```

Close all connections before closing their `IoTDBR2dbcConnectionFactory`, which owns the shared
selector and callback runtime. A long-lived reactive application should own that factory for its
lifetime and close it at shutdown after connection cleanup completes.

The connection URL form is also supported: `r2dbc:iotdb://user:password@host:6667/database`
(credentials belong to the user info part; the SPI rejects `?user=...&password=...` in the query string,
while driver options such as `iotdb.sqlDialect`/`iotdb.fetchSize` pass through it). TLS
URLs (`r2dbcs:`) enable TLS, see the capability matrix.

## Options

| Option | Type | Default | Notes |
|-|-|-|-|
| `host` / `port` | String / Integer | `localhost` / `6667` | Standard SPI options. |
| `database` | String | none | Sent as the session database (`db`). |
| `user` | String | none | Sent as the session user. |
| `password` | CharSequence | none | Snapshotted as a String. An omitted password stays absent; an explicitly empty password is sent unchanged. Never inferred from the user name; `toString()` redacts it. |
| `iotdb.sqlDialect` | String | `table` | `table` or `tree`; the SQL is never rewritten. |
| `iotdb.timeZone` | String | `UTC` | `LocalDateTime` conversion zone. |
| `iotdb.protocol` | String | `binary` | Thrift wire protocol: `binary` (`TBinaryProtocol`) or `compact` (`TCompactProtocol`). The server decides which one its client facing RPC service speaks through `dn_rpc_thrift_compression_enable` (ConfigNode: `cn_rpc_thrift_compression_enable`), so client and server must agree: a mismatch fails during the call with `TTransportException: Read call frame size failed`. Verified against a server started with that flag: the same workload (connect, DDL, insert, select, close) needed 1316 bytes with `compact` against 1900 bytes with `binary`, with the same number of round trips. |
| `iotdb.frameCompression` | String | `none` | Only `none`; `snappy` is rejected. |
| `iotdb.fetchSize` | Integer | `1024` | Page size hint, not a memory bound. |
| `iotdb.maxFrameSize` | Integer | `25165824` (24 MiB) | Stricter of this and the server announced limit wins. |
| `connectTimeout` | Duration | `10s` | Total budget for connect + session setup. |
| `statementTimeout` | Duration | `ZERO` | Until the first execution response; `ZERO` disables it. |
| `iotdb.rpcTimeout` | Duration | `60s` | Per RPC budget; running out of it is reported as `R2dbcTimeoutException` and marks the connection as broken. |
| `iotdb.serverQueryTimeout` | Duration | none | Server side budget; absent follows the server default. Reported as `R2dbcTimeoutException` whether it lands on the first page or a later one, and the connection stays usable. |
| `iotdb.cleanupTimeout` | Duration | `10s` | Budget for cursor/handle cleanup and for closing. A silent server does not make `close()` hang, and a connection whose query handle could not be released is destroyed instead of being reused. |
| `iotdb.maxRedirects` | Integer | `3` | Retained for configuration compatibility; currently has no effect because execution responses are not replayed based on routing recommendations. |
| `ssl` / `r2dbcs:` | Boolean or string `true` | disabled | Enables TLS; the string `false` and any other value are rejected. |
| `iotdb.trustStore` / `iotdb.trustStorePassword` | String | JVM default trust store | Certificate chain verification; the host name is always verified against the certificate. |
| `iotdb.keyStore` / `iotdb.keyStorePassword` | String | none | Client certificate for mutual TLS. |

Durations accept an ISO-8601 value or an integer number of milliseconds. Values from a URL are
strings and are converted defensively; invalid, negative or conflicting values fail before any
connection is opened.

## Server version requirements

The driver speaks the RPC surface of the IoTDB revision it is built from. It does not probe the
server and does not fall back to another path when an RPC is missing, so a version mismatch surfaces
as an error instead of silently different behaviour. The table below was checked against the official
`apache/iotdb` tags (`client.thrift` for the RPCs and `TSStatusCode` for the table model status
codes), not inferred from release dates:

| IoTDB version | Table model (table SQL) | Table model query **with parameters** (typed prepare) | Tree model / writes / DDL / compound | Notes |
|-|-|-|-|-|
| 1.3.x (checked `v1.3.7`) | no | not usable | required RPCs present, **not tested** | 1.x has no table model |
| 2.0.2 - 2.0.8 (each of `v2.0.2` ... `v2.0.8` checked) | yes | **fails with `Invalid method name: 'prepareStatement'`** | required RPCs present, **not tested** | table model present, prepare RPCs absent |
| 2.0.10 | yes | yes | required RPCs present, **not tested end to end** | prepare RPCs appear in 2.0.10 |
| 2.0.11 | yes | yes | yes | **verified end to end** against the official `apache-iotdb-2.0.11-all-bin`: 1C1D plaintext/TLS and 1C3D plaintext |
| 2.0.11-SNAPSHOT (this branch build) | yes | yes | yes | verified against the branch distribution with the same plain/TLS/cluster acceptance suites |

- 2.0.9 was never released: the tags jump from `v2.0.8` to `v2.0.10`.
- 2.0.11 is the only released version covered by the complete live acceptance matrix in this
  repository. For the other versions, "not tested end to end" means the version may still lack a
  capability the driver relies on even when the required RPC names are present.
- To check a server: run a table model query with a bound parameter (`Invalid method name:
  'prepareStatement'` means the server predates 2.0.10), or read the version the driver reports
  (`ConnectionMetadata.getDatabaseVersion()`).
- Versions before 1.3 have a different module layout and were not checked.

Behind the table: `executeStatementV2`, `fetchResultsV2`, `requestStatementId` and `getProperties`
(upstream since commit `a45248d495`, 2023-08-18) and `prepareStatement`, `executePreparedStatement`
and `deallocatePreparedStatement` (PR #17027, commit `8896075ab1`, 2026-02-06, first release 2.0.10).

## Capability matrix

| Capability | State |
|-|-|
| Table model, `SELECT` / `INSERT` / DDL | Supported, verified end to end on the server revision listed above. |
| Tree model queries and writes | Supported; the time column is exposed as `Time` (`TIMESTAMP`), the default `LocalDateTime`, with `get(..., Long.class)`/`get(..., Instant.class)` for raw ticks and instants. |
| Typed parameters | Table model queries with `String`, `Boolean`, `Byte`/`Short`/`Integer`, `Long`, `Float`, `Double` and `Instant`/`LocalDateTime`, using the server side prepare RPC. |
| Time parameters | `Instant` and `LocalDateTime` are supported and converted to epoch ticks in the session precision (`iotdb.timeZone` resolves `LocalDateTime`); ambiguous DST times and precision loss are rejected. `LocalDate`, `LocalTime`, ISO-8601 timestamp strings and binary input are not supported parameter forms. |
| Literal parameters | All statements and dialects for the types above; placeholders inside strings and quoted identifiers are never rewritten. In Table SQL, placeholders inside `--` and `/* ... */` comments are also ignored; Tree SQL follows its server grammar, where those forms are not comments. |
| `DATE` parameters | Not supported: this server version rejects both `DATE < STRING` and `DATE < INT32` comparisons, so binding a `LocalDate` fails at bind time instead of being sent. Rows of a `DATE` column are decoded with the TsFile date encoding. |
| BLOB results | Default and explicit `ByteBuffer` reads return independent, read-only byte snapshots; `byte[]` reads return copies. Binary parameters remain unsupported. |
| OBJECT results | The decoder exposes object references as `VARCHAR`/`String`, using the reference RPC client's object size description; native metadata remains `OBJECT`. The community server rejects stored OBJECT columns. Live tests cover that rejection and null OBJECT expressions; non-null reference decoding has synthetic coverage only. Object contents are not downloaded. |
| Connection pool | Verified with `r2dbc-pool` 1.0.2: borrow, `LOCAL` validation, release, lifecycle reset and reuse of the same connection. |
| Prepared statements | One server side handle per execution; a statement that fails after prepare is deallocated, so the next execution cannot collide with a leaked handle. |
| Parameters | Zero-based positional indexes as required by the SPI; `io.r2dbc.spi.Parameter` wrappers (used by frameworks) are unwrapped and typed nulls become `bindNull`. |
| Output parameters | Not supported. `Parameters.out(...)` is rejected at bind time; `Parameters.inOut(...)` is accepted as an input parameter because the SPI type also implements `Parameter.In`, and its output half is ignored, so a caller that expects a returned value gets none. |
| Batch | Parameter free statements run in order and stop at the first failure; effects of already executed statements are kept. Each `add(sql)` may contain multiple SQL statements, split with the session's Tree/Table quoting and comment rules, with one Result per executable statement. Table comment-only fragments, including a trailing comment after a semicolon, are ignored. Tree SQL retains its server grammar, where those comment forms are not comments. |
| Validation | `LOCAL` checks the local session state, `REMOTE` issues a light weight `getProperties` RPC. |
| Stability | Verified against a live instance: 150 prepared executions in a row without leaking a server side handle, 300 rows paged with `fetchSize=7` without loss or duplication, demand driven consumption, 8 parallel connections sharing one selector runtime and 20 connect/close cycles that release their selector thread. |
| Backpressure | Pages are fetched only when the current page is exhausted and more rows are demanded. |
| Single result per connection | One active result; a second statement fails or waits for cleanup, never interleaves. |
| Result consumption | Once per result; `map`/`filter`/`flatMap`/`getRowsUpdated` share one consumption state. |
| Transactions | Not supported (strict): `beginTransaction`, `commitTransaction`, `rollbackTransaction`, `setAutoCommit(false)` and savepoints report an error instead of succeeding silently. |
| Update counts | Not reported by IoTDB; the count stream completes empty, `0`/`1` is never fabricated. |
| TLS (`r2dbcs:`, `ssl=true`) | Supported with a non-blocking `SSLEngine` transport: the handshake, record layer and partial writes never block the selector thread, certificate chain and host name are verified, and a TLS failure is reported as an error without falling back to plaintext. Verified against a TLS enabled instance by `IoTDBR2dbcTlsIT` (paged queries, repeated frames, concurrent connections, untrusted certificate rejection) as well as a plaintext misconnect that fails in about 0.2 s with a bounded frame error. |
| Execution routing recommendations | The driver never replays an execution RPC based on routing metadata. It accepts the proven successful response shapes (`SUCCESS`, including a Tree write's `redirectNode`, and `REDIRECTION_RECOMMEND` with successful per-device sub-statuses and at least one target) as the completed execution. A bare top-level `REDIRECTION_RECOMMEND` remains an error because the response does not prove whether execution completed; the driver reports it without retrying. Local framed-Thrift tests cover single and multiple Table targets, Tree success with a target, prepared execution, and a bare recommendation. `IoTDBR2dbcClusterIT` checks writes from every DataNode with a zero redirect budget and reads them through another node. |
| Snappy, time and binary parameters, generated keys, named parameters, streaming LOB | Rejected with a clear error; none of them can silently fall back to a different path. |
| Server side cancellation | Not implemented by the server; the driver stops consuming and releases the handle. |

## Build and test

For an isolated server and a strict acceptance gate, use the repository-owned
[fixture scripts](src/test/scripts/README.md). Build the matching all-bin distribution and
dependencies, then run from the repository root (`mvnw.cmd` on Windows):

```bash
./mvnw -B -pl distribution,iotdb-client/jdbc,iotdb-client/r2dbc-spring-data -am -DskipTests install
python iotdb-client/r2dbc/src/test/scripts/run_acceptance.py \
    --distribution distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin \
    --run-root target/r2dbc-acceptance --mode plain
```

Repeat with `--mode tls` and `--mode cluster` for TLS and 1C3D validation. The standard-library
Python runner works on Windows and Linux, creates isolated data and random loopback ports, and
always stops its fixture. It runs the official TCK with an explicit 28-method contract: 17 must pass,
11 must fail with the documented capability diagnostics, and errors, skips, missing or duplicated
methods fail the gate. This does not mean the full TCK passes; see [TCK.md](TCK.md).

[The CI workflow](../../.github/workflows/r2dbc-acceptance.yml) runs script negative controls and
the three fixture modes, and uploads scrubbed evidence only. The workflow has been delivered;
hosted CI execution is not claimed here.

Local live verification on 2026-10-26 includes both `IoTDBR2dbcObjectIT` cases: stored OBJECT DDL
fails with `701` (`SEMANTIC_ERROR`) while the connection stays valid, and a paged
`CAST(NULL AS OBJECT)` expression retains its OBJECT metadata alongside readable stored values.
This server cannot persist non-null OBJECT columns, so no live upload/download result is claimed.
The actual DataNode stop/restart case also passed (1/1): its old connection becomes invalid and
another DataNode remains RPC-accessible. The fixture has replication factor one; this does not
establish continuous access to a region stored only on the stopped node.

The following commands are useful for individual suites against an existing instance:

```bash
# Unit tests (no server required)
mvn test -pl iotdb-client/r2dbc -am

# End to end test against a running instance (skipped when no host is given)
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcIT -DfailIfNoTests=false \
    -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667 \
    -Diotdb.it.user=<user> -Diotdb.it.password=<password>

# Soak and timeout suites (the unresponsive server case needs the stalling proxy helper)
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcSoakIT -DfailIfNoTests=false \
    -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667 \
    -Diotdb.it.user=<user> -Diotdb.it.password=<password> \
    -Diotdb.it.stallProxy=iotdb-client/r2dbc/src/test/scripts/stall_proxy.py

# Frame guard against a TLS endpoint (skipped without the TLS options)
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcTlsIT -DfailIfNoTests=false     -Diotdb.it.host=127.0.0.1 -Diotdb.it.tls.port=6667     -Diotdb.it.user=<user> -Diotdb.it.password=<password>     -Diotdb.it.tls.trustStore=<trust.p12> -Diotdb.it.tls.trustStorePassword=<password>

# Formatting / style
mvn spotless:apply -pl iotdb-client/r2dbc
```

Both client artifacts were built and installed with dependency analysis, RAT and Spotless enabled.
The driver JAR contains its SPI provider registration; the Spring Data JAR contains both resolver
registrations, and both include LICENSE and NOTICE. The complete default reactor's CycloneDX
aggregate and repository dependency comparison were also checked: `dependencies.json` records the
12 Spring dependencies introduced by the optional integration artifact.

## Spring `DatabaseClient`

When using the base `iotdb-r2dbc` artifact alone, Spring cannot derive bind markers because that
artifact publishes no Spring types, so the default builder fails with
`NoBindMarkersFactoryException`. Build the client with named parameters disabled (or with an
explicit `BindMarkersFactory`):

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

The optional [Spring Data integration](../r2dbc-spring-data/README.md), artifact
`iotdb-r2dbc-spring-data`, registers bind-marker and dialect resolvers. With it, Boot can create
its default `DatabaseClient`, `R2dbcEntityTemplate` and query repositories, including named binding
and Table pagination. Its supported write contract is explicit `@Query`/`@Modifying` methods
returning `Mono<Void>`; it does not emulate transactions, affected row counts, generated keys or
standard CRUD `save`. Use Table SQL connections and follow the integration's entity/key guidance.

### Spring Boot sample

`IoTDBR2dbcSpringBootSampleIT` (profile `with-r2dbc-spring`) starts a real Boot application context
that discovers the driver through the ServiceLoader, lets Boot build the `ConnectionFactory` bean
(including the `r2dbc-pool` wrapper) and runs explicit SQL through a `DatabaseClient` bean:

```bash
mvn test -pl iotdb-client/r2dbc -P with-r2dbc-spring -Dtest=IoTDBR2dbcSpringBootSampleIT     -DfailIfNoTests=false -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667     -Diotdb.it.user=<user> -Diotdb.it.password=<password>
```

The same test asserts the base-driver limitation: without the explicit bean or optional integration
artifact, Boot's default `DatabaseClient` auto configuration fails with
`NoBindMarkersFactoryException`. The optional module's separate live repository test verifies
automatic wiring, derived reads, pagination, repeated named binding and explicit writes. Verified
versions: Boot 3.3.8, Spring 6.1.13, R2DBC SPI 1.0.0.RELEASE, r2dbc-pool 1.0.2.RELEASE,
Reactor 3.7.19.

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

Future optimization and broader release work, tracked by the design document above:

- A longer stability run than the recorded ten minutes (`PERFORMANCE.md`, "Long run"), ideally on a
  machine that is not shared with other work.
- A write coalescing layer for the non-blocking transport. Thrift's async client writes the frame
  size and payload as two socket writes, which costs one extra transport delay per request on a high
  latency path (`PERFORMANCE.md`, "RTT arm"); on a LAN it is not visible.
