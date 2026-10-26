# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate the complete official TCK against an explicit capability contract.

This is not a blanket test-failure waiver. Every supported method must pass,
every declared difference must fail with its documented diagnostic, and errors,
unexpected failures, skipped inherited methods and missing cases fail the gate.
"""

import argparse
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

SUPPORTED = {
    "autoCommitByDefault", "batch", "bindFails", "bindNullFails", "columnMetadata",
    "compoundStatement", "createStatementFails", "duplicateColumnNames",
    "prepareStatementWithIncompleteBatchFails", "prepareStatementWithIncompleteBindingFails",
    "prepareStatementWithTrailingAddShouldFail", "row", "rowMetadata",
    "segmentInsertWithFilterCompletesWithoutOnNext", "segmentSelectWithEmitsRow",
    "segmentSelectWithFilterCompletesWithoutOnNext", "validate",
}
TRANSACTION_DIFFERENCES = {
    "transactionCommit": "Transactions are not supported by IoTDB",
    "transactionRollback": "Transactions are not supported by IoTDB",
    "savePoint": "Transactions are not supported by IoTDB",
    "savePointStartsTransaction": "Savepoints are not supported by IoTDB",
    "changeAutoCommitCommitsTransaction": "setAutoCommit(false) is not supported because IoTDB has no cross-statement transactions",
    "sameAutoCommitLeavesTransactionUnchanged": "setAutoCommit(false) is not supported because IoTDB has no cross-statement transactions",
}
COUNT_DIFFERENCES = {
    "bindNull": 'expectation "rows inserted" failed (expected: count = 1; actual: counted = 0; signal: onComplete())',
    "bindNullAsParameter": 'expectation "rows inserted" failed (expected: count = 1; actual: counted = 0; signal: onComplete())',
    "bindValueAsParameter": 'expectation "rows inserted" failed (expected: count = 1; actual: counted = 0; signal: onComplete())',
    "prepareStatement": 'expectation "values from insertions" failed (expected: count = 10; actual: counted = 0; signal: onComplete())',
    "segmentInsertEmitsUpdateCount": 'expectation "insert of a single row" failed (expected: onNext(1); actual: onComplete())',
}
EXCLUDED = {"blobInsert", "blobSelect", "clobInsert", "clobSelect", "returnGeneratedValues", "returnGeneratedValuesFails"}


def verify(report, source):
    root = ET.parse(report).getroot()
    cases = list(root.iter("testcase"))
    names = [case.attrib["name"] for case in cases]
    expected = SUPPORTED | set(TRANSACTION_DIFFERENCES) | set(COUNT_DIFFERENCES)
    if len(cases) != 28 or len(set(names)) != 28 or set(names) != expected:
        raise ValueError("TCK must contain exactly the 28 documented inherited methods")
    for case in cases:
        name = case.attrib["name"]
        if case.find("error") is not None or case.find("skipped") is not None:
            raise ValueError(f"TCK method errored or was skipped: {name}")
        failure = case.find("failure")
        if name in SUPPORTED:
            if failure is not None:
                raise ValueError(f"Supported TCK method regressed: {name}")
            continue
        if failure is None or failure.attrib.get("type") != "java.lang.AssertionError":
            raise ValueError(f"TCK capability difference changed: {name}")
        message = failure.attrib.get("message", "")
        if name in TRANSACTION_DIFFERENCES:
            marker = "onError(org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException: " + TRANSACTION_DIFFERENCES[name] + ")"
            if marker not in message or not message.startswith('expectation "'):
                raise ValueError(f"Unexpected transaction failure: {name}")
        elif message != COUNT_DIFFERENCES[name]:
            raise ValueError(f"Unexpected update-count failure: {name}")
    adapter = Path(source).read_text(encoding="utf-8")
    disabled = set(re.findall(r'@Disabled\("[^"\n]+"\)\s+public void (\w+)\(\)\s*\{\}', adapter))
    if disabled != EXCLUDED:
        raise ValueError("TCK exclusions changed; review the documented capability contract")
    return {"executed": 28, "passed": 17, "capabilityDifferences": 11, "errors": 0, "skipped": 0, "excludedOverrides": sorted(EXCLUDED)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("--source", type=Path, default=Path(__file__).parents[1] / "java/org/apache/iotdb/r2dbc/IoTDBR2dbcTckTest.java")
    arguments = parser.parse_args()
    print(json.dumps(verify(arguments.report, arguments.source), sort_keys=True))


if __name__ == "__main__":
    main()
