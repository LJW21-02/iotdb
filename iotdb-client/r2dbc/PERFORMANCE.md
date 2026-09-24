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

# Performance and resource record (plaintext, 1C1D)

This record documents the first measurement of the driver under sustained load and its comparison
with the blocking `Session` client. It is evidence for the design decision to use a non-blocking
transport, not a throughput promise.

## Method

`IoTDBR2dbcBenchmarkIT` (skipped unless `-Diotdb.it.host` is given) runs three arms against one
IoTDB 2.0.11-SNAPSHOT instance:

- **r2dbc sequential**: 20 warmup queries, then 200 queries on one connection, consumed completely.
- **r2dbc parallel**: 16 connections, 10 queries each, driven from a 16 thread test executor.
- **session sequential**: the same workload through `org.apache.iotdb.session.Session`.

Every query is `SELECT v FROM r2dbc_bench.samples ORDER BY time` over 500 rows with `fetchSize=50`
and its checksum is asserted, so a faster arm cannot win by returning less data. Thread deltas are
measured with `Thread.activeCount()` before and after sixteen connections were opened and while they
each held an active result.

Environment: developer Windows laptop, JDK 21, one ConfigNode and one DataNode on the same machine
(server side unchanged, still a blocking worker model), three rounds after warmup; the numbers below
are the medians of those rounds.

## Results

| Arm | Throughput (queries/s) | p50 (ms) | p95 (ms) | Threads added for the connections |
|-|-|-|-|-|
| r2dbc sequential (1 connection) | 110.8 | 8.64 | 10.83 | 0 |
| r2dbc parallel (16 connections) | 694.3 | n/a | n/a | **+1** |
| session sequential (1 connection) | 140.5 | 6.34 | 12.53 | 0 |

### TLS arm

The same workload over a TLS enabled instance (`-Diotdb.it.tls.port` plus the trust store options):

| Arm | Throughput (queries/s) | p50 (ms) | p95 (ms) | Threads added |
|-|-|-|-|-|
| r2dbc sequential (1 connection) | 94.9 | 10.42 | 14.09 | 0 |
| r2dbc parallel (16 connections) | 530.6 | n/a | n/a | +1 |
| session sequential (1 connection) | 131.8 | 7.34 | 10.11 | 0 |

Compared with the plaintext medians above, TLS costs roughly 14% of the single connection
throughput and 24% of the 16 connection throughput for the driver (the session client loses about
6%): the handshake and the per record encryption are additional work on the same selector thread,
and the session client encrypts inside its blocking write. The thread saving is unchanged.

### RTT arm (TCP level delay)

The workload is reduced for these arms (100 rows per query, 40 queries, 8 connections) so the run
stays short while latency is injected. A TCP level proxy (`rtt_proxy.py`) delays every received chunk
by the configured amount in both directions, so the same injection works for plaintext and TLS and
does not depend on parsing Thrift frames; the proxy is the only change, both clients keep their
normal configuration.

| Arm | 10 ms/chunk: qps, p50/p95 | 50 ms/chunk: qps, p50/p95 |
|-|-|-|
| r2dbc plaintext | 8.8, 108.89 / 155.99 ms | 2.7, 371.77 / 387.14 ms |
| r2dbc TLS | not measured | 2.6, 373.23 / 559.28 ms |
| session plaintext | 12.8, 77.29 / 83.06 ms | 4.0, 248.37 / 251.93 ms |
| session TLS | not measured | 3.7, 248.24 / 377.66 ms |

Observations:

- TLS adds almost nothing once RTT dominates (373.2 ms versus 371.8 ms at 50 ms per chunk), which is
  the expected shape: the handshake and record encryption are per connection and per record costs,
  not per round trip.
- Under RTT the driver has a higher per query latency than the session baseline (371.8 ms versus
  248.4 ms at 50 ms per chunk). Part of that is one extra fetch round trip that the paging protocol
  needs to observe the empty final page, and part is the extra transitions of the non-blocking path;
  this is an observation to investigate, not a tuned result, and the parallel arm (21.5 qps at
  50 ms) still scales across connections.

Raw rounds:

| Round | r2dbc seq qps | r2dbc seq p50/p95 | r2dbc parallel qps | session seq qps | session seq p50/p95 |
|-|-|-|-|-|-|
| 1 | 93.3 | 9.49 / 17.25 | 694.3 | 125.5 | 7.41 / 11.58 |
| 2 | 110.8 | 8.64 / 10.83 | 609.2 | 140.5 | 6.34 / 12.53 |
| 3 | 128.6 | 7.56 / 9.70 | 695.2 | 156.4 | 6.09 / 7.68 |

## What the numbers support

- **The thread saving is real and measurable.** Sixteen connections that each held an active result
  added one thread in total. A blocking model needs at least one waiting thread per request; the
  earlier transport prototype measured 72 Java threads (64 workers) against 9 for the non-blocking
  arm on a different workload.
- **Concurrency scales across connections**: 16 connections reached 694 queries/s, about 6.3x the
  single connection throughput, on the same instance.
- **Throughput is in the same range as the blocking client** (median 110.8 vs 140.5 queries/s, about
  79%), and the median p95 is comparable (10.8 ms vs 12.5 ms).

## What the numbers do not support

- This is an **ecosystem comparison**, not a pure transport comparison: each arm uses its own client
  stack (the driver decodes per row and dispatches callbacks on a bounded scheduler, the session
  client is a blocking RPC client). The design document requires a shared codec and a synchronous
  baseline before attributing a difference to the transport.
- The RTT arms use an in-process TCP proxy on the same machine, which measures an injected transport
  delay rather than a real WAN path, and they run a reduced workload, so they are not comparable to
  the main table above.
- A single machine, a single instance and one workload shape (small row batch, no slow consumer, no
  overload) are not enough to state a general throughput or tail latency claim. The absolute values
  are also machine specific.
- The server keeps its blocking worker model, so the client side thread saving does not remove server
  side worker consumption.
