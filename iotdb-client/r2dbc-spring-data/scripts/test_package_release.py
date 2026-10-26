#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Regression tests for package_release.py without Maven or a database."""

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = Path(__file__).with_name("package_release.py")
spec = importlib.util.spec_from_file_location("package_release", SCRIPT)
package_release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package_release)


def write_jar(path, legal=True):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        if legal:
            archive.writestr("META-INF/LICENSE", "Apache License 2.0\n")


class PackageReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        (self.repo / "LICENSE").write_text("Apache License\n", encoding="utf-8")
        (self.repo / "NOTICE").write_text("Apache IoTDB\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(self.repo), "init", "-q"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.email", "test@example.invalid"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "config", "user.name", "Release Test"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "add", "."], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "test source"], check=True)
        self.commit = subprocess.check_output(["git", "-C", str(self.repo), "rev-parse", "HEAD"], text=True).strip()
        self.driver = self.root / "iotdb-r2dbc-2.0.11-SNAPSHOT.jar"
        self.spring = self.root / "iotdb-r2dbc-spring-data-2.0.11-SNAPSHOT.jar"
        write_jar(self.driver)
        write_jar(self.spring)
        self.driver_pom = self.root / "driver.pom"
        self.spring_pom = self.root / "spring.pom"
        pom = """<project xmlns='http://maven.apache.org/POM/4.0.0'><modelVersion>4.0.0</modelVersion><groupId>org.apache.iotdb</groupId><artifactId>{artifact}</artifactId><version>2.0.11-SNAPSHOT</version></project>"""
        self.driver_pom.write_text(pom.format(artifact="iotdb-r2dbc"), encoding="utf-8")
        self.spring_pom.write_text(pom.format(artifact="iotdb-r2dbc-spring-data"), encoding="utf-8")
        self.runtime = self.root / "runtime.jar"
        write_jar(self.runtime)
        self.classpath = self.root / "classpath.txt"
        self.classpath.write_text(str(self.runtime), encoding="utf-8")
        self.acceptance = self.root / "acceptance.json"
        self.sbom = self.root / "runtime.cdx.json"
        self.sbom.write_text(json.dumps({"bomFormat": "CycloneDX", "components": [{"name": "runtime"}]}), encoding="utf-8")
        self.review = self.root / "legal.json"
        self.review.write_text("{}", encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def args(self):
        return [
            sys.executable, str(SCRIPT), "--repo", str(self.repo), "--expected-commit", self.commit,
            "--driver-jar", str(self.driver), "--driver-pom", str(self.driver_pom),
            "--spring-jar", str(self.spring), "--spring-pom", str(self.spring_pom),
            "--classpath", str(self.classpath), "--acceptance", str(self.acceptance),
            "--sbom", str(self.sbom), "--legal-review", str(self.review), "--output-dir", str(self.root / "out"),
        ]

    def test_refuses_unreviewed_runtime_and_invalid_acceptance(self):
        self.acceptance.write_text(json.dumps({"schema": 1, "gitCommit": self.commit, "status": "passed", "artifacts": {}}), encoding="utf-8")
        result = subprocess.run(self.args(), capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Acceptance", result.stderr)

    def test_creates_deterministic_offline_bundle_with_verified_inputs(self):
        artifacts = {}
        for jar, pom, pom_name in (
            (self.driver, self.driver_pom, "iotdb-r2dbc-2.0.11-SNAPSHOT.pom"),
            (self.spring, self.spring_pom, "iotdb-r2dbc-spring-data-2.0.11-SNAPSHOT.pom"),
        ):
            artifacts[jar.name] = hashlib.sha512(jar.read_bytes()).hexdigest()
            artifacts[pom_name] = hashlib.sha512(pom.read_bytes()).hexdigest()
        self.acceptance.write_text(
            json.dumps({"schema": 1, "gitCommit": self.commit, "status": "passed", "artifacts": artifacts}),
            encoding="utf-8",
        )
        reviewed_jars = [self.driver, self.spring, self.runtime]
        components = {
            hashlib.sha512(path.read_bytes()).hexdigest(): {
                "license": "Apache-2.0",
                "files": ["jar:META-INF/LICENSE"],
            }
            for path in reviewed_jars
        }
        self.review.write_text(
            json.dumps({"schema": 1, "components": components}),
            encoding="utf-8",
        )
        first = subprocess.run(self.args(), capture_output=True, text=True)
        self.assertEqual(0, first.returncode, first.stderr)
        result = json.loads(first.stdout)
        archive_path = Path(result["archive"])
        archive_data = archive_path.read_bytes()
        self.assertEqual(result["sha512"], hashlib.sha512(archive_data).hexdigest())
        with zipfile.ZipFile(archive_path) as archive:
            self.assertIsNone(archive.testzip())
            self.assertIn("MANIFEST.sha512", archive.namelist())
            release = json.loads(archive.read("release.json"))
            self.assertEqual(self.commit, release["gitCommit"])
            self.assertIn("SNAPSHOT", release["coordinatePolicy"])
            legal = json.loads(archive.read("licenses/index.json"))
            self.assertEqual({self.runtime.name, self.driver.name, self.spring.name}, set(legal["components"]))
        second_args = self.args()
        second_args[-1] = str(self.root / "out-second")
        second = subprocess.run(second_args, capture_output=True, text=True)
        self.assertEqual(0, second.returncode, second.stderr)
        self.assertEqual(result["sha512"], json.loads(second.stdout)["sha512"])

    def test_refuses_dirty_checkout_and_output_inside_checkout(self):
        (self.repo / "uncommitted").write_text("changed", encoding="utf-8")
        with self.assertRaises(package_release.PackageError):
            package_release.verify_source(self.repo, self.commit)
        subprocess.run(["git", "-C", str(self.repo), "add", "uncommitted"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "clean state"], check=True)
        current = subprocess.check_output(["git", "-C", str(self.repo), "rev-parse", "HEAD"], text=True).strip()
        artifacts = {}
        for jar, pom, pom_name in (
            (self.driver, self.driver_pom, "iotdb-r2dbc-2.0.11-SNAPSHOT.pom"),
            (self.spring, self.spring_pom, "iotdb-r2dbc-spring-data-2.0.11-SNAPSHOT.pom"),
        ):
            artifacts[jar.name] = hashlib.sha512(jar.read_bytes()).hexdigest()
            artifacts[pom_name] = hashlib.sha512(pom.read_bytes()).hexdigest()
        self.acceptance.write_text(json.dumps({"schema": 1, "gitCommit": current, "status": "passed", "artifacts": artifacts}), encoding="utf-8")
        reviewed = {
            hashlib.sha512(path.read_bytes()).hexdigest(): {"license": "Apache-2.0", "files": ["jar:META-INF/LICENSE"]}
            for path in (self.driver, self.spring, self.runtime)
        }
        self.review.write_text(json.dumps({"schema": 1, "components": reviewed}), encoding="utf-8")
        with self.assertRaises(package_release.PackageError):
            package_release.assemble(type("Args", (), {
                "repo": self.repo, "expected_commit": current, "driver_jar": self.driver,
                "driver_pom": self.driver_pom, "spring_jar": self.spring, "spring_pom": self.spring_pom,
                "classpath": [self.classpath], "acceptance": self.acceptance, "sbom": [self.sbom],
                "legal_review": self.review, "output_dir": self.repo / "artifact",
            })())

    def test_rejects_unsafe_jar_member(self):
        with zipfile.ZipFile(self.runtime, "w") as archive:
            archive.writestr("../escape", "x")
        with self.assertRaises(package_release.PackageError):
            package_release.inspect_jar(self.runtime.read_bytes(), self.runtime.name)

    def test_rejects_colliding_zip_paths(self):
        collision = self.root / "collision.jar"
        with zipfile.ZipFile(collision, "w") as archive:
            archive.writestr("META-INF/LICENSE", "one")
            archive.writestr("meta-inf/license", "two")
        with self.assertRaises(package_release.PackageError):
            package_release.inspect_jar(collision.read_bytes(), collision.name)

    def test_manifest_hash_is_stable_for_same_inputs(self):
        self.assertEqual(package_release.sha512(b"x"), hashlib.sha512(b"x").hexdigest())
        self.assertEqual(package_release.json_bytes({"b": 1, "a": 2}), b'{\n  "a": 2,\n  "b": 1\n}\n')


if __name__ == "__main__":
    unittest.main()
