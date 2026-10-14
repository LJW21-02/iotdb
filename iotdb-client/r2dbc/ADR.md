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

# ADR: transport choice for the IoTDB R2DBC driver

Status: accepted, transport frozen for the first release; the Netty re-evaluation trigger is recorded
below. Scope: which transport carries the Thrift RPCs of `iotdb-client/r2dbc`, and how TLS is
provided.

## Decision

1. **Plaintext**: the generated Thrift async client over `TNonblockingSocket`, wrapped by
   `FrameGuardSocket` so that response frames are validated before Thrift allocates memory. One
   `TAsyncClientManager` (one daemon selector thread) is shared by every connection of a factory.
2. **TLS**: a driver owned `SSLEngine` transport, `TlsNonblockingSocket`, driven from `read`/`write`
   of the same async client. Certificate chain and host name are verified with the HTTPS endpoint
   identification algorithm; a TLS failure fails the connection and never falls back to plaintext.
3. **No blocking client in the runtime path**: `Session` and JDBC are used only as test fixtures and
   as the ecosystem baseline for measurements.
4. **One active request and one active result per connection**; concurrency is provided by more
   connections and by `r2dbc-pool`.
5. Decoding and user callbacks run on a bounded scheduler, never on the selector thread.

## Alternatives and why they were rejected

| Alternative | Reason |
|-|-|
| Blocking `Session` plus a bounded thread pool | Satisfies reactive behaviour but not non-blocking I/O; it also does not remove server side worker usage. Kept as the measurement baseline only. |
| Thrift's `TNonblockingSSLSocket` | Its handshake busy waits on the shared selector thread, and its `read` returns the requested length instead of the bytes read, which does not match what the async client expects. Verified from the 0.24.0 bytecode in the design investigation. |
| Netty + Thrift codecs | Follows the design document's preference and gives a mature TLS stack, but the generated async client is bound to `TNonblockingTransport` + `TAsyncClientManager`; a Netty transport means re-implementing serialization and response dispatch per RPC method (about 1–2 person weeks) and replacing a transport that is already verified end to end. Revisit when the trigger below fires. |
| Following server redirects without a policy | Unbounded re-sending can duplicate writes and loop between nodes. The driver follows a recommendation only for a request that has not been executed, at most `iotdb.maxRedirects` times, re-establishes the session on the target node, and rejects a response that recommends several different nodes for one request. Verified against a 1C3D cluster. |

## Evidence

### Frame protection (first release gate)

- A plaintext connection to a TLS enabled endpoint fails in about 0.2 s with a controlled
  `TTransportException("Received a frame of 352518912 bytes which exceeds the configured limit of
  25165824 bytes")` instead of allocating a buffer for the TLS handshake bytes, which is the OOM case
  of the transport prototype. `IoTDBR2dbcTlsIT`.
- `FrameGuardSocketTest` covers an accepted frame, an oversized frame, a zero frame and a negative
  frame, and that the guard stays in sync across frames.

### TLS (first release gate)

`IoTDBR2dbcTlsIT` against a TLS enabled instance: DDL/DML, a 120 row query with `fetchSize=7`,
25 repeated queries, 4 concurrent TLS connections, `validate(REMOTE)` and a clean close; an untrusted
certificate fails the connection; a raw `SSLSocket` handshake confirms the endpoint really speaks
TLS.

### Non-blocking and thread behaviour

A 10 second mixed workload (benchmark plus soak suites, 1C1D, JDK 21) recorded with JFR
`settings=profile`:

| Observation | Result |
|-|-|
| `jdk.SocketRead` on any driver thread | none. The only four events (10–19 ms) are on the test's `main` thread executing the blocking `Session` baseline. |
| `jdk.SocketWrite` | none. |
| `jdk.ThreadPark` on `TAsyncClientManager#SelectorThread*` | none. |
| `jdk.ThreadPark` on `iotdb-r2dbc-callbacks-*` | 13, all `LockSupport.park()` on a condition object, i.e. idle bounded scheduler workers. |
| `jdk.JavaMonitorEnter` / `jdk.JavaMonitorWait` | 0 / 0. |
| Threads added by 16 connections that each hold an active result | +1 (versus at least one waiting thread per request in a blocking model). |

### Performance

`PERFORMANCE.md` records the plaintext, TLS and RTT arms: the driver reaches about 79% of the
blocking client's single connection throughput and 694 queries/s across 16 connections; TLS costs
about 14% sequentially, and almost nothing once RTT dominates. Under an injected 50 ms per chunk the
driver shows a higher per query latency than the blocking baseline (372 ms versus 248 ms), which is
recorded as an observation to investigate rather than a tuned result.

### Contract

`TCK.md`: 17 of the 28 runnable TCK tests pass; the 11 failures are the documented transaction and
update count differences, and 6 further tests are excluded with reasons.

A failure the server reported keeps its type on every page: `ExceptionMapper.mapPageFailure` returns
an `R2dbcException` unchanged, so a query timeout reported while reading a later page stays an
`R2dbcTimeoutException` (transient, with the server message, code 720 and the SQL) instead of becoming
a generic non transient driver exception. Measured on the single node probe with a slow consumer and
`iotdb.serverQueryTimeout` just above the first page latency: before the change the same failure was
`IoTDBR2dbcException: Reading the next page failed: IoTDB server error: Query has executed more than
104ms, and now is in flushing state` after 2320 rows, after it `R2dbcTimeoutException: IoTDB server
error: Query has executed more than 105ms, and now is in flushing state` after 2960 rows. The server
enforces the budget in `MPPQueryContext.checkTimeOut` during analysis and in
`FragmentInstanceManager.cancelTimeoutFlushingInstances` (every 2 s) for instances that are still
flushing past their deadline; a fast consumer can therefore see the timeout on the first page.

## Consequences

- The transport is reusable for plaintext and TLS with the same async client, request serialization
  and cleanup path.
- Redirection replaces the session of a connection (new socket, `openSession`, statement id) and is
  bounded per statement. A prepared execution is prepared again on the target node, because the
  server side handle belongs to the session that created it; the handle of an attempt that never
  reached the caller is released, so a followed redirection leaves no registered statement behind.
- `TlsNonblockingSocket` is a hand written state machine (handshake, record layer, partial write
  accounting, and one extra transition so Thrift can read a frame body from buffered plaintext). This
  is the main maintenance cost of the decision and the main argument for Netty if it grows.
- Known capability differences stay visible: no cross statement transactions, no affected row counts,
  no Snappy, no query redirection, no `DATE` parameters, server side cancellation not implemented.
- Thrift's async client writes the frame size and the frame payload as two socket writes
  (`TAsyncMethodCall` states `WRITING_REQUEST_SIZE` and `WRITING_REQUEST`), so a request reaches the
  wire as two segments where a blocking client's two writes are coalesced into one. On a LAN this is
  invisible; with latency injected per chunk it costs one extra transport delay per request, which is
  what the RTT arms in `PERFORMANCE.md` measure. A coalescing layer between the async client and the
  socket is the candidate fix; it is not implemented because the deferred size prefix has to be
  reported back through `TNonblockingTransport.write(ByteBuffer)`, where a wrong partial write report
  corrupts the protocol instead of failing a test.

## Re-evaluation trigger

Revisit the Netty option when any of the following holds: the two socket writes per request turn
into a measured problem on a real WAN path (and a coalescing layer cannot be added safely); a
required feature (for example mutual TLS with hardware keys, proxy support, or very high connection
counts) cannot be met by the nio transport; a defect in the TLS state machine cannot be fixed with
contained changes; or the measured maintenance cost of the hand written transport exceeds the
estimated cost of the Netty path.

## Reproducing

```bash
# plaintext suites (benchmark, soak, integration, pool) and the TLS suite
mvn test -pl iotdb-client/r2dbc -Dtest='IoTDBR2dbcBenchmarkIT,IoTDBR2dbcSoakIT' ...
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcTlsIT -Diotdb.it.tls.port=... -Diotdb.it.tls.trustStore=...

# the benchmark IT measures all four arms, including the blocking transport with the driver codec
# the RTT arms put .r2dbc-validation/rtt_proxy.py (delay per chunk) in front of the server
# .r2dbc-validation/frame_proxy.py counts frames and chunks per connection for one arm at a time
# .r2dbc-validation/stall_proxy.py keeps the socket open but stops answering, for the timeout case:
mvn test -pl iotdb-client/r2dbc -Dtest=IoTDBR2dbcSoakIT     -Diotdb.it.stallProxy=.../.r2dbc-validation/stall_proxy.py -Diotdb.it.host=... ...
# .r2dbc-validation/PageTimeoutProbe.java drives the page phase timeout A/B (slow consumer plus a
# server query budget just above the first page); run it with the driver classes of the build under
# test in front of the distribution lib/* of the matching server, not with locally resolved snapshots
# JFR recording of the mixed workload
mvn test -pl iotdb-client/r2dbc -Dtest='IoTDBR2dbcBenchmarkIT,IoTDBR2dbcSoakIT' \
    -DargLine="-XX:StartFlightRecording=filename=run.jfr,settings=profile,dumponexit=true"
```
