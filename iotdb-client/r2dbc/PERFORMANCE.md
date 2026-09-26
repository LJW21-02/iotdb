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

`IoTDBR2dbcBenchmarkIT` (skipped unless `-Diotdb.it.host` is given) runs four arms against one
IoTDB 2.0.11-SNAPSHOT instance:

- **r2dbc sequential**: 20 warmup queries, then 200 queries on one connection, consumed completely.
- **r2dbc parallel**: 16 connections, 10 queries each, driven from a 16 thread test executor.
- **session sequential**: the same workload through `org.apache.iotdb.session.Session`.
- **blocking + driver codec**: `SyncSharedCodecClient`, a blocking Thrift transport that sends the
  same requests and decodes every page with the same `RowDecoder` the non-blocking path uses. It
  separates the codec from the transport: this arm and the r2dbc arm differ in the transport and its
  scheduling only, this arm and the session arm differ in the client stack and its codec only.

Every query is `SELECT v FROM r2dbc_bench.samples ORDER BY time` over 500 rows with `fetchSize=50`
and its checksum is asserted, so a faster arm cannot win by returning less data. Thread deltas are
measured with `Thread.activeCount()` before and after sixteen connections were opened and while they
each held an active result.

Environment: developer Windows laptop, JDK 21, one ConfigNode and one DataNode on the same
machine (server side unchanged, still a blocking worker model). Two sessions are recorded because
the machine is a shared laptop: compare arms within one session, not across sessions.

## Results, session A (three rounds)

| Arm | Throughput (queries/s) | p50 (ms) | p95 (ms) | Threads added for the connections |
|-|-|-|-|-|
| r2dbc sequential (1 connection) | 110.8 | 8.64 | 10.83 | 0 |
| r2dbc parallel (16 connections) | 694.3 | n/a | n/a | **+1** |
| session sequential (1 connection) | 140.5 | 6.34 | 12.53 | 0 |

## Results, session B (four rounds, with the codec baseline)

| Arm | Throughput (queries/s) | p50 (ms) | p95 (ms) | Threads added for the connections |
|-|-|-|-|-|
| r2dbc sequential (1 connection) | 166.2 | 5.86 | 8.15 | 0 |
| r2dbc parallel (16 connections) | 930.0 | n/a | n/a | **+0..+1** |
| session sequential (1 connection) | 191.8 | 4.77 | 8.54 | 0 |
| blocking + driver codec (1 connection) | **211.5** | 4.54 | 7.59 | not measured |

What the third and fourth rows mean together:

- **The codec is not the bottleneck.** Decoding 500 rows with the driver's own `RowDecoder` over a
  blocking socket is 10% *faster* than the same query through the session client, which uses its own
  decode path. If row decoding were the cost of the difference, this arm would be the slowest.
- **The non-blocking transport costs about 21% of the single connection throughput** on this LAN
  workload (211.5 to 166.2 queries/s) and buys the bounded thread count: sixteen connections that
  each hold an active result add at most one thread, while the blocking arms need a thread per
  waiting request. The 16 connection arm still reaches 930 queries/s (5.6x the single connection).

### TLS arm

The same workload over a TLS enabled instance (`-Diotdb.it.tls.port` plus the trust store options):

Session A (three rounds):

| Arm | Throughput (queries/s) | p50 (ms) | p95 (ms) | Threads added |
|-|-|-|-|-|
| r2dbc sequential (1 connection) | 94.9 | 10.42 | 14.09 | 0 |
| r2dbc parallel (16 connections) | 530.6 | n/a | n/a | +1 |
| session sequential (1 connection) | 131.8 | 7.34 | 10.11 | 0 |

Session B (four rounds, with the codec baseline):

| Arm | Throughput (queries/s) | p50 (ms) | p95 (ms) |
|-|-|-|-|
| r2dbc sequential (1 connection) | 104.5 | 9.07 | 12.58 |
| r2dbc parallel (16 connections) | 651.6 | n/a | n/a |
| session sequential (1 connection) | 137.6 | 7.05 | 8.89 |
| blocking + driver codec (1 connection) | **152.6** | 6.36 | 8.21 |

The blocking arm uses the JVM's blocking TLS socket (`TSSLTransportFactory`), the driver uses its
own `SSLEngine` state machine on the same selector thread. Reading session B as a chain: the driver's
TLS transport keeps 68% of the equivalent blocking TLS arm (104.5 versus 152.6 queries/s), and about
two thirds of that gap is the transport and dispatch cost that exists without TLS as well (session B
plaintext: 166.2 versus 211.5, 79%). The handshake and the per record encryption happen on the
selector thread, which is where the remainder goes; the thread saving is unchanged.

In session A, TLS cost the driver roughly 14% of the single connection throughput and 24% of the 16
connection throughput (the session client lost about 6%); session A did not include the blocking
baseline, so the split between record layer and transport cannot be read from it.

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
| blocking + driver codec plaintext | not measured | 4.0, 247.53 / 251.80 ms |
| blocking + driver codec TLS | not measured | 4.0, 248.33 / 251.06 ms |

Observations:

- TLS adds almost nothing once RTT dominates (plaintext 372.2 ms versus TLS 371.1 ms for the driver,
  247.5 versus 248.3 ms for the blocking arm at 50 ms per chunk): the handshake and record
  encryption are per connection and per record costs, not per round trip.
- Under RTT the driver has a higher per query latency than either blocking arm (372.2 ms versus
  248.6 ms session and 247.5 ms blocking + driver codec, 50 ms per chunk). The codec is ruled out:
  the blocking arm decodes with the driver's own `RowDecoder` and is not slower. So the difference is
  in the non-blocking path itself.
- **The cause is request chunking, not an extra round trip.** `frame_proxy.py`, a proxy that counts
  Thrift frames and TCP chunks per connection, was run with a 50 ms delay per chunk for one arm at a
  time (60 queries of 100 rows, `fetchSize=50`, same server). Both arms used the same number of
  request and response frames; the non-blocking arm needed about twice as many request chunks,
  because its writes reach the wire as two segments per frame where the blocking transport's writes
  are coalesced:

  | Arm (one connection, 60 queries) | request frames | request chunks | response frames | response chunks | p50 |
  |-|-|-|-|-|-|
  | r2dbc (non-blocking) | 125 | **249** | 125 | 125 | 372.4 ms |
  | blocking + driver codec | 123 | **123** | 123 | 123 | 248.9 ms |

  Each injected delay is charged per chunk, so ~2.1 extra request chunks per query explain
  ~105 ms of the ~124 ms difference. This is a property of the non-blocking write path (Thrift's
  async client writing a frame in more than one socket write, with no coalescing buffer); removing it
  would need an explicit write coalescing layer, which is not implemented. The same arm reaches
  23.7 queries/s with 8 connections, so the per query cost does not block concurrency.

Raw rounds, session A:

| Round | r2dbc seq qps | r2dbc seq p50/p95 | r2dbc parallel qps | session seq qps | session seq p50/p95 |
|-|-|-|-|-|-|
| 1 | 93.3 | 9.49 / 17.25 | 694.3 | 125.5 | 7.41 / 11.58 |
| 2 | 110.8 | 8.64 / 10.83 | 609.2 | 140.5 | 6.34 / 12.53 |
| 3 | 128.6 | 7.56 / 9.70 | 695.2 | 156.4 | 6.09 / 7.68 |

Raw rounds, session B (plaintext, same order as the tables above):

| Round | r2dbc seq qps | r2dbc seq p50/p95 | r2dbc parallel qps | session seq qps | session seq p50/p95 | blocking + codec qps | blocking + codec p50/p95 |
|-|-|-|-|-|-|-|-|
| 1 | 138.1 | 7.17 / 9.12 | 744.4 | 195.0 | 4.79 / 8.00 | 178.9 | 5.10 / 9.33 |
| 2 | 171.9 | 5.75 / 7.85 | 877.0 | 178.4 | 5.52 / 7.06 | 205.6 | 4.80 / 6.29 |
| 3 | 160.6 | 5.96 / 7.78 | 1078.9 | 193.6 | 4.74 / 9.18 | 224.1 | 4.29 / 6.09 |
| 4 | 174.6 | 5.43 / 8.45 | 982.9 | 190.1 | 4.74 / 9.09 | 217.4 | 4.06 / 8.88 |

Raw rounds, session B (TLS):

| Round | r2dbc seq qps | r2dbc seq p50/p95 | r2dbc parallel qps | session seq qps | session seq p50/p95 | blocking + codec qps | blocking + codec p50/p95 |
|-|-|-|-|-|-|-|-|
| 1 | 88.6 | 11.27 / 14.40 | 582.5 | 116.1 | 8.27 / 10.94 | 100.8 | 8.90 / 16.96 |
| 2 | 109.2 | 8.75 / 12.34 | 642.1 | 135.2 | 6.95 / 9.05 | 156.6 | 6.31 / 7.81 |
| 3 | 99.8 | 9.38 / 12.82 | 661.1 | 139.9 | 7.15 / 8.73 | 148.6 | 6.41 / 8.62 |
| 4 | 153.1 | 6.31 / 8.61 | 694.0 | 175.4 | 5.57 / 7.27 | 214.4 | 4.52 / 6.19 |

### Long run (ten minutes, one instance)

`IoTDBR2dbcSoakIT#sustainedMixedWorkloadKeepsResourcesBounded` is skipped by default; giving it a
budget (`-Diotdb.it.soak.seconds=600`) runs a mixed workload of a paged query, a prepared statement
with a bound parameter and a discarded result per iteration, and asserts what each iteration read.
The recorded run:

| Measure | Value |
|-|-|
| Duration | 600.4 s |
| Statements | 358,761 (about 598/s) |
| Rows read and checked | 35,995,687 (about 60,000/s) |
| Heap in use after a collection | 0.7 MiB *below* the start of the run |
| Selector threads | 1, unchanged (one factory) |
| Driver threads | 2, one extra callback worker (workers expire after a minute of inactivity) |
| Flight recording (18.7 MiB, `profile`) | 2 `jdk.SocketRead` events, both the async client's own non-blocking size and body read, 0 `jdk.JavaMonitorEnter` events |

The run used the same shared laptop and the same single instance as the tables above, and it is a
stability measurement, not a throughput claim: the fixture is 300 rows, so the numbers show that the
driver keeps working and stays flat, not what it can move at peak.

### Reproducing the ordering

A later single round on the quiet machine measured 328.4 queries/s (r2dbc), 510.1 (session) and 634.1
(blocking + driver codec) for the same workload: every arm was about three times faster than in
session B and the order did not change. Absolute numbers from this laptop therefore say little, while
the ordering and the per session comparisons reproduce.

## What the numbers support

- **The thread saving is real and measurable.** Sixteen connections that each held an active result
  added one thread in total. A blocking model needs at least one waiting thread per request; the
  earlier transport prototype measured 72 Java threads (64 workers) against 9 for the non-blocking
  arm on a different workload.
- **Concurrency scales across connections**: 16 connections reached 694 queries/s in session A and
  930 queries/s in session B, 6.3x and 5.6x the single connection throughput of the same session.
- **Throughput is in the same range as the blocking clients** (session B: 166.2 queries/s driver,
  191.8 session client, 211.5 blocking with the driver codec), and the median p95 is comparable.
- **The difference to the blocking arms is transport and dispatch, not decoding**, because the arm
  that shares the driver's `RowDecoder` over a blocking socket is the fastest of the three. The RTT
  arm locates the same difference in the request write path rather than in the round trip count.
- **The resource bounds hold over a long run**: ten minutes of mixed work left the heap and the
  thread counts where they started, with one selector thread per factory.

## What the numbers do not support

- The driver to session comparison is still an **ecosystem comparison**: each arm uses its own
  client stack. The shared codec arm removes the decoding difference but is written for the
  benchmark; it does not validate the driver's scheduling, backpressure or lifecycle behaviour, and
  the two blocking arms use a blocking TLS socket where the driver uses its own `SSLEngine` state
  machine.
- The RTT arms use an in-process TCP proxy on the same machine, which measures an injected transport
  delay rather than a real WAN path, and they run a reduced workload, so they are not comparable to
  the main table above. The 10 ms column of the RTT table was measured in session A only.
- A single machine, a single instance and one workload shape (small row batch, no slow consumer, no
  overload) are not enough to state a general throughput or tail latency claim. The absolute values
  are also machine specific.
- The server keeps its blocking worker model, so the client side thread saving does not remove server
  side worker consumption.
