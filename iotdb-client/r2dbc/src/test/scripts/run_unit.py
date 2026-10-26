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

"""Run all source-declared driver/Spring unit methods with a fail-closed XML gate."""

import argparse
import json
import os
import re
import xml.etree.ElementTree as ET
from pathlib import Path

from fixture import maven_command, REPOSITORY, run_maven, sanitize_report


def strip_java_comments(source):
    """Remove Java comments without treating comment-like text in strings as comments."""
    result = []
    index = 0
    state = "code"
    while index < len(source):
        char = source[index]
        next_char = source[index + 1] if index + 1 < len(source) else ""
        if state == "code":
            if char == '"':
                state = "string"
                result.append(char)
            elif char == "'":
                state = "char"
                result.append(char)
            elif char == "/" and next_char == "/":
                state = "line-comment"
                result.extend((" ", " "))
                index += 1
            elif char == "/" and next_char == "*":
                state = "block-comment"
                result.extend((" ", " "))
                index += 1
            else:
                result.append(char)
        elif state == "line-comment":
            if char == "\n":
                state = "code"
                result.append(char)
            else:
                result.append(" ")
        elif state == "block-comment":
            if char == "*" and next_char == "/":
                state = "code"
                result.extend((" ", " "))
                index += 1
            else:
                result.append("\n" if char == "\n" else " ")
        else:
            result.append(char)
            if char == "\\" and index + 1 < len(source):
                result.append(source[index + 1])
                index += 1
            elif (state == "string" and char == '"') or (state == "char" and char == "'"):
                state = "code"
        index += 1
    return "".join(result)


def unit_contract(module):
    expected = set()
    for folder in (module / "src/test/java", module / "src/test/spring/java"):
        for source in sorted(folder.rglob("*Test.java")):
            # *IT classes are live-server acceptance tests. They intentionally
            # assume a fixture and belong to run_acceptance, not this unit gate.
            if source.name.endswith("IT.java"):
                continue
            if source.name == "IoTDBR2dbcTckTest.java":
                continue
            text = strip_java_comments(source.read_text(encoding="utf-8"))
            package = re.search(r"\bpackage\s+([\w.]+)\s*;", text)
            methods = re.findall(r"@Test(?:\([^)]*\))?\s+(?:public\s+)?(?:final\s+)?void\s+(\w+)\s*\(", text)
            if not package or not methods or len(methods) != len(re.findall(r"@Test\b", text)):
                raise ValueError("Unsupported or empty unit-test source contract: " + source.name)
            if re.search(r"@(Ignore|Disabled|ParameterizedTest|RepeatedTest|TestFactory)\b", text):
                raise ValueError("Unit test exclusions/parameterization need an explicit gate contract")
            expected.update((package.group(1) + "." + source.stem, method) for method in methods)
    if not expected:
        raise ValueError("No unit tests discovered in module")
    return expected


def verify_units(reports, expected):
    cases = [case for report in reports for case in ET.parse(report).getroot().iter("testcase")]
    actual = [(case.attrib.get("classname"), case.attrib.get("name")) for case in cases]
    if len(actual) != len(expected) or set(actual) != expected:
        raise ValueError("Unit methods differ from current sources (missing, duplicated or stale report)")
    if any(case.find(tag) is not None for case in cases for tag in ("failure", "error", "skipped")):
        raise ValueError("Unit tests failed, errored or skipped")
    return {"executed": len(cases), "passed": len(cases), "errors": 0, "skipped": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--offline", action="store_true")
    arguments = parser.parse_args()
    arguments.artifacts.mkdir(parents=True, exist_ok=True)
    for name, profile in (("r2dbc", "with-r2dbc-spring"), ("r2dbc-spring-data", None)):
        module = REPOSITORY / "iotdb-client" / name
        expected = unit_contract(module)
        classes = sorted({class_name for class_name, _ in expected})
        reports = module / "target/surefire-reports"
        for class_name in classes:
            (reports / ("TEST-" + class_name + ".xml")).unlink(missing_ok=True)
        # Build the selected module in the reactor so its ServiceLoader resources are loaded from
        # the exact source revision under test, rather than from a possibly stale SNAPSHOT cache.
        command = [
            maven_command(),
            "-B",
            "--no-transfer-progress",
            "-pl",
            "iotdb-client/" + name,
            "-am",
        ]
        if arguments.offline:
            command.append("-o")
        if profile:
            command += ["-P", profile]
        command += ["-Dtest=" + ",".join(classes), "-Dsurefire.failIfNoSpecifiedTests=false", "test"]
        log = arguments.artifacts / (name + "-unit.log")
        with log.open("wb") as output:
            result = run_maven(command, output, arguments.timeout)
        public = arguments.artifacts / name
        public.mkdir(exist_ok=True)
        current = [reports / ("TEST-" + class_name + ".xml") for class_name in classes]
        for report in current:
            if report.is_file():
                sanitize_report(report, public / report.name, [])
        if result != 0 or not all(report.is_file() for report in current):
            raise RuntimeError("Unit Maven execution failed or omitted reports: " + name)
        summary = verify_units(current, expected)
        (arguments.artifacts / (name + "-unit-summary.json")).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(name + " " + json.dumps(summary, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
