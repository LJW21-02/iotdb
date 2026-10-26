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

# R2DBC TCK difference report

The driver runs the official TCK from `io.r2dbc:r2dbc-spi-test:1.0.0.RELEASE`
(`io.r2dbc.spi.test.TestKit<T>`, JUnit Jupiter) against a live IoTDB 2.0.11-SNAPSHOT instance.

## Running it

```bash
mvn test -pl iotdb-client/r2dbc -P with-r2dbc-tck -Dtest=IoTDBR2dbcTckTest -DfailIfNoTests=false \
    -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667 \
    -Diotdb.it.user=<user> -Diotdb.it.password=<password>
```

The `with-r2dbc-tck` profile adds the Jupiter engine, the vintage engine (so the module's JUnit 4 tests
keep running) and the IoTDB JDBC driver that the TCK uses as its fixture. Without `-Diotdb.it.host`
the whole class is skipped, so a plain `mvn test` never depends on a server.

For a reproducible isolated server, follow the [fixture instructions](src/test/scripts/README.md).
After building the matching distribution and installing reactor dependencies, run from the repository
root:

```bash
python iotdb-client/r2dbc/src/test/scripts/run_acceptance.py \
    --distribution distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin \
    --run-root target/r2dbc-acceptance --mode plain
```

This entry point runs the official TCK as part of plain acceptance and checks its new Surefire XML
with `verify_tck.py`. Every one of the 28 inherited methods must occur exactly once: the 17 supported
methods must pass, and the 11 capability differences must fail with their documented diagnostic.
Errors, skips, missing methods, duplicate methods, unexpected failures or a changed exclusion list
fail the gate. It also checks the six excluded override methods in the adapter source. The runner
does not use `maven.test.failure.ignore`: Maven must report the expected TCK failure, and the gate
accepts only the reviewed capability contract. A successful acceptance gate is not a full TCK pass.

For an individually executed TCK suite, check the report directly:

```bash
python iotdb-client/r2dbc/src/test/scripts/verify_tck.py \
    iotdb-client/r2dbc/target/surefire-reports/TEST-org.apache.iotdb.r2dbc.IoTDBR2dbcTckTest.xml
```

[The acceptance workflow](../../.github/workflows/r2dbc-acceptance.yml) runs the same plain gate
alongside TLS and cluster jobs. Its definition is delivered; no successful hosted CI run is claimed
by this document. Only scrubbed fixture artifacts should be uploaded, because raw Surefire XML can
contain connection credentials in system properties.

## Fixture

`IoTDBR2dbcTckTest` implements `TestKit<Integer>`, uses `?` placeholders with zero-based indexes and
creates the fixture tables through the IoTDB JDBC driver connected to the same instance. The TCK's
ANSI SQL is translated to the IoTDB table model:

| TCK statement | IoTDB statement |
|-|-|
| `CREATE TABLE test ( test_value INTEGER )` | `CREATE TABLE test (test_value INT32 FIELD)` |
| `INSERT INTO test VALUES(…)` | `INSERT INTO test(time, test_value) VALUES (…, …)` |
| `CREATE TABLE test_two_column ( col1 INTEGER, col2 VARCHAR(100) )` | `CREATE TABLE test_two_column (col1 INT32 FIELD, col2 STRING FIELD)` |
| `CREATE TABLE blob_test ( test_value BLOB )` | `CREATE TABLE blob_test (test_value BLOB FIELD)` |
| `CREATE TABLE clob_test ( test_value CLOB )` | `CREATE TABLE clob_test (test_value TEXT FIELD)` |

Everything else, including statement splitting for compound statements, is handled by the driver.

The adapter prepares its fixture through the IoTDB JDBC driver, which only the `with-r2dbc-tck`
profile puts on the test classpath. Without that profile it skips itself with a message instead of
failing, so a plain `mvn test -Diotdb.it.host=...` run of the integration suites stays green.

## Result of the last run (2026-10-26)

28 inherited test methods ran against an isolated binary/ms 1C1D built from this branch:
**17 passed, 11 failed, 0 errored, 0 skipped**. The 11 failures match the capability differences below;
the Maven command correctly returns a failure status. Six further methods are excluded through
`@Disabled` overrides. These overrides have no test annotation and are not included in the 28-method
Surefire count, so they must not be reported as six passing tests or as six Surefire skips.

### Passed (17)

`autoCommitByDefault`, `batch`, `bindFails`, `bindNullFails`, `columnMetadata`, `compoundStatement`,
`createStatementFails`, `duplicateColumnNames`, `prepareStatementWithIncompleteBatchFails`,
`prepareStatementWithIncompleteBindingFails`, `prepareStatementWithTrailingAddShouldFail`, `row`,
`rowMetadata`, `segmentInsertWithFilterCompletesWithoutOnNext`, `segmentSelectWithEmitsRow`,
`segmentSelectWithFilterCompletesWithoutOnNext`, `validate`.

### Expected differences (11)

| Test | Expectation | Driver behaviour | Reason |
|-|-|-|-|
| `transactionCommit`, `transactionRollback`, `savePoint`, `savePointStartsTransaction`, `changeAutoCommitCommitsTransaction`, `sameAutoCommitLeavesTransactionUnchanged` | transaction support | an explicit `IoTDBR2dbcException` for `beginTransaction`, `setAutoCommit(false)`, savepoints and isolation changes | IoTDB has no cross-statement transactions; the driver reports that instead of faking a commit (decision D7). |
| `bindNull`, `bindNullAsParameter`, `bindValueAsParameter`, `prepareStatement`, `segmentInsertEmitsUpdateCount` | one `UpdateCount` segment per inserted row | the update count stream completes empty | IoTDB does not report affected row counts; the driver never fabricates `0`/`1` (decision D9). The statements themselves execute and the rows are readable, which the passing select/compound tests show. |

### Excluded with a reason (6)

| Test | Reason |
|-|-|
| `blobInsert`, `blobSelect` | Streaming `Blob` values are outside the first release scope. |
| `clobInsert`, `clobSelect` | IoTDB has no `CLOB` type; streaming character LOBs are outside the first release scope. |
| `returnGeneratedValues`, `returnGeneratedValuesFails` | IoTDB does not support generated keys. |

## Conclusion

The 17 supported TCK methods pass, including statement validity checks, dead parameter binding,
metadata access, result segments, filtering, validation and compound statements. The full official
TCK does not pass: its 11 failures remain visible and are checked against the explicit transaction
and update-count capability contract. The database capabilities are reported truthfully rather than
emulated or disabled to produce a green Maven run.
