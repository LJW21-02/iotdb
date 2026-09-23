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

## Result of the last run (2026-09-23)

28 inherited test methods ran: **17 passed, 11 failed, 0 errored**. Six further tests are excluded
through `@Disabled` overrides, which is the mechanism the TCK documents for unsupported features.

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

Apart from the documented transaction and update-count differences, the TCK passes, including
statement validity checks, dead parameter binding, metadata access, result segments, filtering,
validation and compound statements. The failures above are capability differences of the database,
not silent driver defects, and they stay visible here rather than being disabled to produce a green
run.
