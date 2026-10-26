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

"""Negative controls for the published-POM standalone consumer dependency gate."""

import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import xml.etree.ElementTree as ET

from check_consumer_runtime import baseline, consumer_pom, NS, REPOSITORY, run, verify_runtime


class ConsumerRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.expected = baseline(REPOSITORY)

    def inputs(self, profile="driver", lz4=None, framework=None, legacy=False):
        items = [("org.apache.iotdb", "iotdb-r2dbc", self.expected["client"]),
                 ("at.yawk.lz4", "lz4-java", lz4 or self.expected["lz4"])]
        if profile != "driver":
            items += [("org.apache.iotdb", "iotdb-r2dbc-spring-data", self.expected["client"])]
            items += [("org.springframework", name, framework or self.expected["framework"])
                      for name in ("spring-core", "spring-beans", "spring-context", "spring-r2dbc", "spring-tx")]
            items += [("org.springframework.data", name, self.expected["data"])
                      for name in ("spring-data-r2dbc", "spring-data-relational", "spring-data-commons")]
        if profile == "boot":
            items += [("org.springframework.boot", name, self.expected["boot"])
                      for name in ("spring-boot", "spring-boot-autoconfigure", "spring-boot-starter-data-r2dbc")]
        if legacy:
            items += [("org.lz4", "lz4-java", "1.8.0")]
        children, paths = [], []
        for group, artifact, version in items:
            path = self.directory.joinpath(*group.split("."), artifact, version, artifact + "-" + version + ".jar")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((group + artifact + version).encode())
            paths.append(str(path))
            children.append(dict(groupId=group, artifactId=artifact, version=version, type="jar", scope="runtime"))
        return {"children": children}, os.pathsep.join(paths)

    def test_all_three_consumer_shapes_have_no_workaround_management(self):
        for profile in ("driver", "spring", "boot"):
            root = ET.fromstring(consumer_pom(profile, self.expected))
            self.assertIsNone(root.find("m:parent", NS))
            direct = root.findall("m:dependencies/m:dependency", NS)
            self.assertNotIn("lz4-java", [node.findtext("m:artifactId", namespaces=NS) for node in direct])
            managed = root.findall("m:dependencyManagement/m:dependencies/m:dependency", NS)
            self.assertEqual(["spring-boot-dependencies"] if profile == "boot" else [],
                             [node.findtext("m:artifactId", namespaces=NS) for node in managed])
            graph, cp = self.inputs(profile)
            self.assertTrue(verify_runtime(profile, self.expected, graph, cp))

    def test_rejects_transitive_old_lz4_even_with_other_current_components(self):
        graph, cp = self.inputs("boot", lz4="1.11.1")
        with self.assertRaisesRegex(ValueError, "at.yawk.lz4:lz4-java"):
            verify_runtime("boot", self.expected, graph, cp)

    def test_rejects_legacy_coordinate_coexistence(self):
        graph, cp = self.inputs(legacy=True)
        with self.assertRaisesRegex(ValueError, "Legacy org.lz4"):
            verify_runtime("driver", self.expected, graph, cp)

    def test_rejects_unmanaged_spring_transitive_drift(self):
        graph, cp = self.inputs("spring", framework="6.2.18")
        with self.assertRaisesRegex(ValueError, "org.springframework:spring"):
            verify_runtime("spring", self.expected, graph, cp)

    def test_rejects_classpath_missing_graph_component_and_duplicates(self):
        graph, cp = self.inputs()
        with self.assertRaisesRegex(ValueError, "graph differ"):
            verify_runtime("driver", self.expected, graph, cp.split(os.pathsep)[0])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            verify_runtime("driver", self.expected, graph, cp + os.pathsep + cp.split(os.pathsep)[0])

    def test_refuses_existing_work_directory_without_running_maven(self):
        arguments = SimpleNamespace(repo=REPOSITORY, work_dir=self.directory)
        with mock.patch("check_consumer_runtime.subprocess.run") as maven:
            with self.assertRaises(FileExistsError):
                run(arguments)
            maven.assert_not_called()

    def test_failed_maven_resolution_never_records_passed_acceptance(self):
        summary = self.directory / "public-summary.json"
        arguments = SimpleNamespace(repo=REPOSITORY, work_dir=self.directory / "new-work",
                                    summary=summary, maven="unused-maven", local_repo=None, offline=True)
        with mock.patch("check_consumer_runtime.subprocess.run", return_value=SimpleNamespace(returncode=1)):
            with self.assertRaisesRegex(ValueError, "Maven tree failed"):
                run(arguments)
        result = json.loads(summary.read_text(encoding="utf-8"))
        self.assertEqual("failed", result["status"])
        self.assertEqual([], result["profiles"])
        self.assertEqual(3, len(result["sourceInputsSha512"]))


if __name__ == "__main__":
    unittest.main()
