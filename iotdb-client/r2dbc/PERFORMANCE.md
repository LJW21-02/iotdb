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
- The **TLS arm and the RTT arms are not measured yet**. TLS correctness is verified (see
  `IoTDBR2dbcTlsIT`), but its throughput cost has not been compared under this method.
- A single machine, a single instance and one workload shape (small row batch, no slow consumer, no
  overload) are not enough to state a general throughput or tail latency claim. The absolute values
  are also machine specific.
- The server keeps its blocking worker model, so the client side thread saving does not remove server
  side worker consumption.
