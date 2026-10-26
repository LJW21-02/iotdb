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
        self.runtime = self.root / "runtime-1.0.jar"
        write_jar(self.runtime)
        self.classpath = self.root / "classpath.txt"
        self.classpath.write_text(str(self.runtime), encoding="utf-8")
        self.acceptance = self.root / "acceptance.json"
        self.sbom = self.root / "runtime.cdx.json"
        self.review = self.root / "legal.json"
        self.review.write_text("{}", encoding="utf-8")
        self.consumer = self.root / "consumer"
        self.consumer.mkdir()
        for name in ("BundleConsumer.java", "run_consumer.py", "README.md"):
            (self.consumer / name).write_text("Standalone consumer fixture\n", encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def args(self):
        return [
            sys.executable, str(SCRIPT), "--repo", str(self.repo), "--expected-commit", self.commit,
            "--driver-jar", str(self.driver), "--driver-pom", str(self.driver_pom),
            "--spring-jar", str(self.spring), "--spring-pom", str(self.spring_pom),
            "--classpath", str(self.classpath), "--acceptance", str(self.acceptance),
            "--sbom", str(self.sbom), "--legal-review", str(self.review), "--consumer-dir", str(self.consumer),
            "--output-dir", str(self.root / "out"),
        ]

    def valid_inputs(self, commit=None):
        artifacts = {}
        for jar, pom, pom_name in (
            (self.driver, self.driver_pom, "iotdb-r2dbc-2.0.11-SNAPSHOT.pom"),
            (self.spring, self.spring_pom, "iotdb-r2dbc-spring-data-2.0.11-SNAPSHOT.pom"),
        ):
            artifacts[jar.name] = package_release.sha512(jar.read_bytes())
            artifacts[pom_name] = package_release.sha512(pom.read_bytes())
        runtime = {}
        components = []
        for path, group, artifact, version in (
            (self.driver, "org.apache.iotdb", "iotdb-r2dbc", "2.0.11-SNAPSHOT"),
            (self.spring, "org.apache.iotdb", "iotdb-r2dbc-spring-data", "2.0.11-SNAPSHOT"),
            (self.runtime, "example", "runtime", "1.0"),
        ):
            digest = package_release.sha512(path.read_bytes())
            runtime[path.name] = {"groupId": group, "artifactId": artifact, "version": version,
                                  "classifier": "", "sha512": digest}
            purl = f"pkg:maven/{group}/{artifact}@{version}?type=jar"
            components.append({"type": "library", "group": group, "name": artifact, "version": version,
                               "purl": purl, "bom-ref": purl,
                               "hashes": [{"alg": "SHA-512", "content": digest}],
                               "properties": [{"name": "iotdb.bundle.filename", "value": path.name}]})
        self.acceptance.write_text(json.dumps({"schema": 2, "gitCommit": commit or self.commit,
            "status": "passed", "artifacts": artifacts, "runtime": runtime,
            "classpaths": [[self.runtime.name]]}), encoding="utf-8")
        self.sbom.write_text(json.dumps({"bomFormat": "CycloneDX", "specVersion": "1.6", "version": 1,
                                        "components": components}), encoding="utf-8")
        reviewed = {package_release.sha512(path.read_bytes()): {"license": "Apache-2.0",
            "files": ["jar:META-INF/LICENSE"]} for path in (self.driver, self.spring, self.runtime)}
        self.review.write_text(json.dumps({"schema": 1, "components": reviewed}), encoding="utf-8")

    def run_package(self):
        return subprocess.run(self.args(), capture_output=True, text=True)

    def change_acceptance(self, change):
        value = json.loads(self.acceptance.read_text(encoding="utf-8"))
        change(value)
        self.acceptance.write_text(json.dumps(value), encoding="utf-8")

    def change_sbom(self, change):
        value = json.loads(self.sbom.read_text(encoding="utf-8"))
        change(value)
        self.sbom.write_text(json.dumps(value), encoding="utf-8")

    def test_refuses_unreviewed_runtime_and_invalid_acceptance(self):
        self.acceptance.write_text(json.dumps({"schema": 1, "gitCommit": self.commit, "status": "passed", "artifacts": {}}), encoding="utf-8")
        result = subprocess.run(self.args(), capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Acceptance", result.stderr)

    def test_creates_deterministic_offline_bundle_with_verified_inputs(self):
        self.valid_inputs()
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
            self.assertEqual(2, release["schema"])
            self.assertEqual({self.runtime.name, self.driver.name, self.spring.name}, set(release["runtime"]))
            self.assertIn("SNAPSHOT", release["coordinatePolicy"])
            legal = json.loads(archive.read("licenses/index.json"))
            self.assertEqual({self.runtime.name, self.driver.name, self.spring.name}, set(legal["components"]))
        second_args = self.args()
        second_args[-1] = str(self.root / "out-second")
        second = subprocess.run(second_args, capture_output=True, text=True)
        self.assertEqual(0, second.returncode, second.stderr)
        self.assertEqual(result["sha512"], json.loads(second.stdout)["sha512"])
        checked = package_release.verify_bundle(archive_path, result["sha512"])
        self.assertEqual(3, checked["jars"])

    def test_refuses_dirty_checkout_and_output_inside_checkout(self):
        (self.repo / "uncommitted").write_text("changed", encoding="utf-8")
        with self.assertRaises(package_release.PackageError):
            package_release.verify_source(self.repo, self.commit)
        subprocess.run(["git", "-C", str(self.repo), "add", "uncommitted"], check=True)
        subprocess.run(["git", "-C", str(self.repo), "commit", "-qm", "clean state"], check=True)
        current = subprocess.check_output(["git", "-C", str(self.repo), "rev-parse", "HEAD"], text=True).strip()
        self.valid_inputs(current)
        with self.assertRaises(package_release.PackageError):
            package_release.assemble(type("Args", (), {
                "repo": self.repo, "expected_commit": current, "driver_jar": self.driver,
                "driver_pom": self.driver_pom, "spring_jar": self.spring, "spring_pom": self.spring_pom,
                "classpath": [self.classpath], "acceptance": self.acceptance, "sbom": [self.sbom],
                "legal_review": self.review, "consumer_dir": self.consumer, "output_dir": self.repo / "artifact",
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

    def test_refuses_replaced_runtime_even_when_client_hashes_match(self):
        self.valid_inputs()
        with zipfile.ZipFile(self.runtime, "a") as archive:
            archive.writestr("changed.txt", "different dependency bytes")
        result = self.run_package()
        self.assertNotEqual(0, result.returncode)
        self.assertIn("runtime hash", result.stderr)

    def test_refuses_missing_or_extra_runtime_inventory(self):
        self.valid_inputs()
        self.change_acceptance(lambda data: data["runtime"].pop(self.runtime.name))
        result = self.run_package()
        self.assertIn("exactly every", result.stderr)
        self.valid_inputs()
        self.change_acceptance(lambda data: data["runtime"].update({"extra-1.jar": data["runtime"][self.runtime.name]}))
        self.assertIn("exactly every", self.run_package().stderr)

    def test_refuses_runtime_coordinates_that_differ_from_client_pom(self):
        self.valid_inputs()
        self.change_acceptance(lambda data: data["runtime"][self.driver.name].update(groupId="different.group"))
        self.assertIn("differ from POM", self.run_package().stderr)

    def test_refuses_duplicate_classpath_entries(self):
        self.valid_inputs()
        import os
        self.classpath.write_text(str(self.runtime) + os.pathsep + str(self.runtime), encoding="utf-8")
        self.assertIn("Duplicate runtime JAR", self.run_package().stderr)

    def test_refuses_changed_classpath_order(self):
        self.valid_inputs()
        import os
        self.classpath.write_text(str(self.runtime) + os.pathsep + str(self.driver), encoding="utf-8")
        self.change_acceptance(lambda data: data.update(classpaths=[[self.driver.name, self.runtime.name]]))
        self.assertIn("ordered runtime classpaths", self.run_package().stderr)

    def test_accepts_distinct_profiles_with_shared_runtime(self):
        self.valid_inputs()
        import os
        second = self.root / "spring-classpath.txt"
        second.write_text(str(self.runtime) + os.pathsep + str(self.spring), encoding="utf-8")
        self.change_acceptance(lambda data: data["classpaths"].append([self.runtime.name, self.spring.name]))
        args = self.args() + ["--classpath", str(second)]
        result = subprocess.run(args, capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_refuses_sbom_missing_extra_or_duplicate_components(self):
        for kind in ("missing", "extra", "duplicate"):
            with self.subTest(kind=kind):
                self.valid_inputs()
                def change(document):
                    if kind == "missing":
                        document["components"].pop()
                    else:
                        item = dict(document["components"][0])
                        if kind == "extra":
                            item["properties"] = [{"name": "iotdb.bundle.filename", "value": "extra-1.jar"}]
                        document["components"].append(item)
                self.change_sbom(change)
                result = self.run_package()
                self.assertNotEqual(0, result.returncode)
                self.assertIn("SBOM", result.stderr)

    def test_refuses_sbom_hash_and_coordinate_mismatch(self):
        for field in ("hash", "version", "purl"):
            with self.subTest(field=field):
                self.valid_inputs()
                def change(document):
                    component = document["components"][0]
                    if field == "hash":
                        component["hashes"][0]["content"] = "0" * 128
                    elif field == "version":
                        component["version"] = "wrong"
                    else:
                        component["purl"] += "&classifier=wrong"
                self.change_sbom(change)
                self.assertNotEqual(0, self.run_package().returncode)

    def test_refuses_duplicate_json_keys(self):
        with self.assertRaises(package_release.PackageError):
            package_release.parse_json('{"runtime":{},"runtime":{}}')

    def test_refuses_embedded_maven_coordinates_mismatch(self):
        with zipfile.ZipFile(self.runtime, "a") as archive:
            archive.writestr("META-INF/maven/other/runtime/pom.properties",
                             "groupId=other\nartifactId=runtime\nversion=1.0\n")
        self.valid_inputs()
        self.assertIn("Embedded Maven coordinates", self.run_package().stderr)

    def test_refuses_consumer_directory_with_private_or_compiled_files(self):
        self.valid_inputs()
        (self.consumer / "credentials.properties").write_text("not for distribution", encoding="utf-8")
        self.assertIn("Consumer directory", self.run_package().stderr)

    def test_bundles_public_evidence_and_self_contained_verifier(self):
        self.valid_inputs()
        evidence = self.root / "unit-summary.json"
        evidence.write_text('{"passed":19,"skipped":0}\n', encoding="utf-8")
        result = subprocess.run(self.args() + ["--evidence-file", str(evidence)], capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        bundle = json.loads(result.stdout)
        extracted = self.root / "consumer-workspace"
        with zipfile.ZipFile(bundle["archive"]) as archive:
            self.assertEqual(evidence.read_bytes(), archive.read("evidence/reports/unit-summary.json"))
            archive.extractall(extracted)
        verified = subprocess.run([sys.executable, str(extracted / "tools/verify_bundle.py"),
                                   "--bundle", bundle["archive"], "--sha512", bundle["sha512"]],
                                  cwd=extracted, capture_output=True, text=True)
        self.assertEqual(0, verified.returncode, verified.stderr)
        self.assertEqual(3, json.loads(verified.stdout)["jars"])

    def test_refuses_duplicate_evidence_filenames(self):
        self.valid_inputs()
        evidence = self.root / "summary.json"
        evidence.write_text('{"passed":1}', encoding="utf-8")
        result = subprocess.run(self.args() + ["--evidence-file", str(evidence), "--evidence-file", str(evidence)],
                                capture_output=True, text=True)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("Duplicate evidence", result.stderr)

    def test_bundle_verifier_rejects_wrong_archive_hash(self):
        self.valid_inputs()
        result = self.run_package()
        self.assertEqual(0, result.returncode, result.stderr)
        with self.assertRaisesRegex(package_release.PackageError, "archive digest"):
            package_release.verify_bundle(json.loads(result.stdout)["archive"], "0" * 128)

    def test_bundle_verifier_rejects_manifest_and_release_tampering(self):
        self.valid_inputs()
        result = self.run_package()
        self.assertEqual(0, result.returncode, result.stderr)
        original = Path(json.loads(result.stdout)["archive"])
        for semantic in (False, True):
            with self.subTest(semantic=semantic):
                modified = self.root / ("tampered-semantic.zip" if semantic else "tampered.zip")
                with zipfile.ZipFile(original) as archive:
                    files = {name: archive.read(name) for name in archive.namelist()}
                release = json.loads(files["release.json"])
                release["runtime"][self.runtime.name]["sha512"] = "0" * 128
                files["release.json"] = package_release.json_bytes(release)
                if semantic:
                    files["MANIFEST.sha512"] = "".join(
                        f"{package_release.sha512(data)}  {name}\n" for name, data in sorted(files.items())
                        if name != "MANIFEST.sha512").encode("utf-8")
                with zipfile.ZipFile(modified, "w") as archive:
                    for name, data in files.items():
                        archive.writestr(name, data)
                with self.assertRaises(package_release.PackageError):
                    package_release.verify_bundle(modified, package_release.sha512(modified.read_bytes()))


if __name__ == "__main__":
    unittest.main()
