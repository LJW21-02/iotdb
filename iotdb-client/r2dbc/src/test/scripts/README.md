<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy at
  http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Reproducible R2DBC acceptance

Requirements: JDK 17 or later (`java`, `javac`, and `keytool` on PATH), Python
3.10 or later, and an all-bin distribution built from this checkout. The Python
scripts use only the standard library and run on Windows and Linux.

Build and install the matching server and JDBC/Spring dependencies first:

```sh
./mvnw -B -pl distribution,iotdb-client/jdbc,iotdb-client/r2dbc-spring-data -am -DskipTests install
```

Use `mvnw.cmd` on Windows. Every fixture uses a fresh directory, random loopback
ports, its own data, and random administrator credentials. Configurations and
key material stay in `private/`, which is owner-only on Linux and grants only
the current Windows user and SYSTEM on Windows. No existing IoTDB installation
or service is modified.

The CI entry point starts, verifies, and always stops one fixture:

```sh
python iotdb-client/r2dbc/src/test/scripts/run_acceptance.py \
  --distribution distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin \
  --run-root target/r2dbc-acceptance --mode plain
```

Run it with `--mode tls` and `--mode cluster` for the other jobs. Plain runs the
driver/OBJECT boundary/pool suites, the complete official TCK, the DatabaseClient
Boot sample, and the Spring Data repository integration. TLS generates a fresh
certificate with a localhost/IP SAN and tests trusted/untrusted certificates,
paging, concurrent connections, and a plaintext misconnect. Cluster runs four
cross-node/routing/proxy cases, then a separate actual DataNode stop/restart case.
The cluster has replication factor one. Node loss checks invalidation of the
connection to that node and continued RPC availability on a surviving node; it
does not claim uninterrupted access to data stored only on the stopped node.

Individual commands allow local debugging without exposing credentials:

```sh
python iotdb-client/r2dbc/src/test/scripts/fixture.py start \
  --distribution path/to/all-bin --run-root target/r2dbc-acceptance --mode tls
# Copy the STATE=... path printed above, then:
python iotdb-client/r2dbc/src/test/scripts/fixture.py run-tests --state path/to/state.json --suite tls
python iotdb-client/r2dbc/src/test/scripts/fixture.py stop --state path/to/state.json
```

`run-tests` also accepts `plain`, `cluster`, `node-loss`, `tck`, `boot`, and
`spring-data`, with `--offline` if dependencies are already installed. It refuses
to accept missing, duplicated, failed or skipped methods as a passing live
suite. `stop` verifies both the recorded PID and exact `IOTDB_HOME` JVM argument
before terminating a process, then checks all fixture ports are released. It
leaves logs and data for inspection.

The official TCK has database capability differences: 17 supported methods must
pass; 11 methods must fail with the exact documented transaction/update-count
diagnostics. The gate parses the new XML, rejects errors and skips, verifies all
28 inherited methods by name, and checks the six documented disabled overrides
in the adapter source. It does not use `maven.test.failure.ignore`, and a Maven
failure without the exact reviewed report fails the gate. If a capability is
added or a TCK version changes, review and update the explicit contract rather
than loosening it to accept arbitrary failures.

Public evidence is under each run's `artifacts/`: redacted Maven and node logs,
sanitized Surefire XML, and machine-readable summaries. Do not upload `private/` or raw
Surefire XML from module targets; those contain system properties and may include
fixture credentials. The workflow uploads only this scrubbed public evidence.

Run the gate's negative controls without starting a server:

```sh
python -m unittest discover -s iotdb-client/r2dbc/src/test/scripts -p "test_*.py"
```
