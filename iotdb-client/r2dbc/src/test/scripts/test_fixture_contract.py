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

"""Negative controls for the TCK gate and public fixture evidence."""

import tempfile
import unittest
import xml.etree.ElementTree as ET
import os
from pathlib import Path

from fixture import EXPECTED_CASES, sanitize_report, stop_windows_node, verify_live
from run_unit import unit_contract, verify_units
from verify_tck import COUNT_DIFFERENCES, SUPPORTED, TRANSACTION_DIFFERENCES, verify


class TckGateTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.report = Path(self.temporary.name) / "TEST-tck.xml"
        self.source = Path(__file__).parents[1] / "java/org/apache/iotdb/r2dbc/IoTDBR2dbcTckTest.java"
        self.root = ET.Element("testsuite")
        self.cases = {}
        for name in sorted(SUPPORTED | set(TRANSACTION_DIFFERENCES) | set(COUNT_DIFFERENCES)):
            case = ET.SubElement(self.root, "testcase", name=name)
            self.cases[name] = case
            if name in TRANSACTION_DIFFERENCES:
                ET.SubElement(case, "failure", type="java.lang.AssertionError", message='expectation "fixture" failed (actual: onError(org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException: ' + TRANSACTION_DIFFERENCES[name] + "))")
            elif name in COUNT_DIFFERENCES:
                ET.SubElement(case, "failure", type="java.lang.AssertionError", message=COUNT_DIFFERENCES[name])

    def check(self):
        ET.ElementTree(self.root).write(self.report, encoding="utf-8")
        return verify(self.report, self.source)

    def test_documented_differences_are_explicitly_counted(self):
        self.assertEqual(11, self.check()["capabilityDifferences"])

    def test_supported_method_failure_is_not_waived(self):
        ET.SubElement(self.cases["batch"], "failure", type="java.lang.AssertionError", message="unexpected")
        with self.assertRaises(ValueError):
            self.check()

    def test_same_method_name_with_wrong_failure_is_not_waived(self):
        self.cases["bindNull"].find("failure").set("message", "connection timed out")
        with self.assertRaises(ValueError):
            self.check()

    def test_transaction_transport_failure_is_not_a_capability_difference(self):
        self.cases["transactionCommit"].find("failure").set("message", 'expectation "fixture" failed (actual: onError(java.io.IOException: closed))')
        with self.assertRaises(ValueError):
            self.check()

    def test_missing_method_is_not_accepted(self):
        self.root.remove(self.cases["validate"])
        with self.assertRaises(ValueError):
            self.check()

    def test_duplicate_method_is_not_accepted(self):
        self.cases["validate"].set("name", "batch")
        with self.assertRaises(ValueError):
            self.check()

    def test_skips_are_not_accepted(self):
        ET.SubElement(self.cases["batch"], "skipped")
        with self.assertRaises(ValueError):
            self.check()

    def test_errors_are_not_accepted(self):
        ET.SubElement(self.cases["batch"], "error", type="java.lang.Exception")
        with self.assertRaises(ValueError):
            self.check()

    def test_new_unreviewed_success_changes_the_contract(self):
        self.cases["bindNull"].remove(self.cases["bindNull"].find("failure"))
        with self.assertRaises(ValueError):
            self.check()


class PublicEvidenceTest(unittest.TestCase):
    @unittest.skipUnless(os.name == "nt", "Windows native identity guard")
    def test_windows_controller_refuses_an_unrelated_process(self):
        with tempfile.TemporaryDirectory() as directory:
            # Open this test's own process handle, but never terminate it: its
            # command line has neither ServerLauncher nor the exact home token.
            with self.assertRaises(RuntimeError):
                stop_windows_node({"pid": os.getpid(), "home": directory, "main": "org.apache.iotdb.db.service.DataNode"})

    def test_reports_scrub_credentials_from_properties_attributes_and_output(self):
        with tempfile.TemporaryDirectory() as directory:
            source, destination = Path(directory) / "raw.xml", Path(directory) / "public.xml"
            source.write_text('<testsuite><properties><property name="iotdb.it.password" value="private-value"/></properties><testcase name="private-value"><system-out>private-value</system-out></testcase></testsuite>', encoding="utf-8")
            sanitize_report(source, destination, ["private-value"])
            self.assertNotIn("private-value", destination.read_text(encoding="utf-8"))
            self.assertIn("[REDACTED]", destination.read_text(encoding="utf-8"))

    def test_live_suite_cannot_pass_with_a_missing_or_duplicate_method(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "live.xml"
            root = ET.Element("testsuite")
            for class_name, method_name in EXPECTED_CASES["tls"]:
                ET.SubElement(root, "testcase", classname="org.apache.iotdb.r2dbc." + class_name, name=method_name)
            ET.ElementTree(root).write(report)
            self.assertEqual(4, verify_live([report], "tls")["passed"])
            root.remove(root[-1])
            ET.ElementTree(root).write(report)
            with self.assertRaises(RuntimeError):
                verify_live([report], "tls")
            ET.SubElement(root, "testcase", **root[0].attrib)
            ET.ElementTree(root).write(report)
            with self.assertRaises(RuntimeError):
                verify_live([report], "tls")


class UnitGateTest(unittest.TestCase):
    def test_contract_includes_profile_tests_and_excludes_the_live_tck(self):
        with tempfile.TemporaryDirectory() as directory:
            module = Path(directory)
            for folder, name in (("java", "DriverTest"), ("spring/java", "LifecycleTest"), ("java", "IoTDBR2dbcTckTest")):
                source = module / "src/test" / folder / (name + ".java")
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_text("package example; public class " + name + " { @Test public void coversBehavior() {} }", encoding="utf-8")
            self.assertEqual({("example.DriverTest", "coversBehavior"), ("example.LifecycleTest", "coversBehavior")}, unit_contract(module))

    def test_missing_duplicate_skipped_and_failed_unit_methods_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            report = Path(directory) / "unit.xml"
            expected = {("example.DriverTest", "first"), ("example.DriverTest", "second")}
            root = ET.Element("testsuite")
            for class_name, method in sorted(expected):
                ET.SubElement(root, "testcase", classname=class_name, name=method)
            def verify_current():
                ET.ElementTree(root).write(report)
                return verify_units([report], expected)
            self.assertEqual(2, verify_current()["passed"])
            for tag in ("skipped", "failure", "error"):
                marker = ET.SubElement(root[0], tag)
                with self.assertRaises(ValueError):
                    verify_current()
                root[0].remove(marker)
            root[1].set("name", root[0].attrib["name"])
            with self.assertRaises(ValueError):
                verify_current()
            root.remove(root[1])
            with self.assertRaises(ValueError):
                verify_current()


if __name__ == "__main__":
    unittest.main()
