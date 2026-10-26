#!/usr/bin/env python3
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

"""Resolve standalone consumers and reject runtime drift from the client POM baseline."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import xml.etree.ElementTree as ET


REPOSITORY = Path(__file__).resolve().parents[5]
NAMESPACE = "http://maven.apache.org/POM/4.0.0"
NS = {"m": NAMESPACE}
PLUGIN = "org.apache.maven.plugins:maven-dependency-plugin:3.8.1:"


def digest(data):
    return hashlib.sha512(data).hexdigest()


def baseline(repo):
    driver = ET.parse(repo / "iotdb-client/r2dbc/pom.xml").getroot()
    spring = ET.parse(repo / "iotdb-client/r2dbc-spring-data/pom.xml").getroot()

    def field(root, path):
        value = root.findtext(path, namespaces=NS)
        if not value or not re.fullmatch(r"[A-Za-z0-9_.+-]+", value):
            raise ValueError("Missing or unresolved declared consumer baseline: " + path)
        return value

    version = field(driver, "m:parent/m:version")
    if field(spring, "m:parent/m:version") != version:
        raise ValueError("Client modules declare different versions")
    lz4 = field(driver, "m:properties/m:lz4-java.version")
    if field(spring, "m:properties/m:lz4-java.version") != lz4:
        raise ValueError("Client modules declare different LZ4 baselines")
    return {
        "client": version,
        "lz4": lz4,
        "framework": field(spring, "m:properties/m:spring-framework.version"),
        "data": field(spring, "m:properties/m:spring-data.version"),
        "boot": field(spring, "m:properties/m:spring-boot.version"),
    }


def consumer_pom(profile, expected):
    """No IoTDB parent or workaround dependency management may mask published POM drift."""
    if profile not in ("driver", "spring", "boot"):
        raise ValueError("Unknown consumer profile")
    root = ET.Element("project", xmlns=NAMESPACE)
    for key, value in (("modelVersion", "4.0.0"), ("groupId", "org.apache.iotdb.acceptance"),
                       ("artifactId", "standalone-" + profile), ("version", "1")):
        ET.SubElement(root, key).text = value

    def dependency(parent, group, artifact, version):
        node = ET.SubElement(parent, "dependency")
        for key, value in (("groupId", group), ("artifactId", artifact), ("version", version)):
            ET.SubElement(node, key).text = value
        return node

    if profile == "boot":
        managed = ET.SubElement(ET.SubElement(root, "dependencyManagement"), "dependencies")
        node = dependency(managed, "org.springframework.boot", "spring-boot-dependencies", expected["boot"])
        ET.SubElement(node, "type").text = "pom"
        ET.SubElement(node, "scope").text = "import"
    dependencies = ET.SubElement(root, "dependencies")
    artifact = "iotdb-r2dbc" if profile == "driver" else "iotdb-r2dbc-spring-data"
    dependency(dependencies, "org.apache.iotdb", artifact, expected["client"])
    if profile == "boot":
        dependency(dependencies, "org.springframework.boot", "spring-boot-starter-data-r2dbc", expected["boot"])
    ET.indent(root)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def runtime_graph(tree):
    records = {}

    def visit(node):
        if not isinstance(node, dict):
            raise ValueError("Malformed Maven dependency graph node")
        group, artifact, version = (node.get(key) for key in ("groupId", "artifactId", "version"))
        classifier = node.get("classifier") or ""
        if node.get("type") != "jar" or node.get("scope") not in ("compile", "runtime"):
            raise ValueError("Unexpected type or scope in runtime dependency graph")
        if any(not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.+-]+", value)
               for value in (group, artifact, version)):
            raise ValueError("Unresolved runtime dependency coordinate")
        if classifier and not re.fullmatch(r"[A-Za-z0-9_.+-]+", classifier):
            raise ValueError("Invalid runtime classifier")
        key = (group, artifact, classifier)
        record = {"groupId": group, "artifactId": artifact, "version": version, "classifier": classifier}
        if key in records and records[key] != record:
            raise ValueError("Conflicting runtime versions for " + group + ":" + artifact)
        records[key] = record
        children = node.get("children", [])
        if not isinstance(children, list):
            raise ValueError("Malformed runtime dependency children")
        for child in children:
            visit(child)

    if not isinstance(tree, dict) or not isinstance(tree.get("children"), list) or not tree["children"]:
        raise ValueError("Missing standalone runtime dependency graph")
    for child in tree["children"]:
        visit(child)
    return list(records.values())


def verify_runtime(profile, expected, graph, classpath):
    records = runtime_graph(graph)
    by_name = {}
    for record in records:
        suffix = "-" + record["classifier"] if record["classifier"] else ""
        filename = record["artifactId"] + "-" + record["version"] + suffix + ".jar"
        if filename in by_name:
            raise ValueError("Different Maven coordinates share a runtime filename")
        by_name[filename] = record
    paths = [Path(value) for value in classpath.strip().split(os.pathsep) if value]
    if not paths or len(paths) != len(set(path.name for path in paths)):
        raise ValueError("Empty or duplicate standalone runtime classpath")
    if {path.name for path in paths} != set(by_name):
        raise ValueError("Standalone classpath and effective runtime graph differ")
    result = []
    for path in paths:
        record = by_name[path.name]
        suffix = tuple(record["groupId"].split(".")) + (record["artifactId"], record["version"], path.name)
        if not path.is_absolute() or path.is_symlink() or not path.is_file() or path.parts[-len(suffix):] != suffix:
            raise ValueError("Runtime classpath is not a regular Maven-layout JAR: " + path.name)
        result.append(dict(record, filename=path.name, sha512=digest(path.read_bytes())))
    found = {(r["groupId"], r["artifactId"], r["classifier"]): r["version"] for r in result}
    required = {("at.yawk.lz4", "lz4-java"): expected["lz4"],
                ("org.apache.iotdb", "iotdb-r2dbc"): expected["client"]}
    if profile in ("spring", "boot"):
        required[("org.apache.iotdb", "iotdb-r2dbc-spring-data")] = expected["client"]
        required.update({("org.springframework", artifact): expected["framework"]
                         for artifact in ("spring-core", "spring-beans", "spring-context", "spring-r2dbc", "spring-tx")})
        required.update({("org.springframework.data", artifact): expected["data"]
                         for artifact in ("spring-data-r2dbc", "spring-data-relational", "spring-data-commons")})
    if profile == "boot":
        required.update({("org.springframework.boot", artifact): expected["boot"]
                         for artifact in ("spring-boot", "spring-boot-autoconfigure", "spring-boot-starter-data-r2dbc")})
    for key, version in required.items():
        resolved = found.get((*key, ""))
        if resolved != version:
            raise ValueError(profile + " runtime baseline differs for " + ":".join(key)
                             + ": expected " + version + ", resolved " + str(resolved))
    for (group, artifact, classifier), version in found.items():
        if group == "org.lz4" and artifact == "lz4-java":
            raise ValueError("Legacy org.lz4:lz4-java must not coexist with the fixed runtime")
        if (group, artifact) == ("at.yawk.lz4", "lz4-java") and version != expected["lz4"]:
            raise ValueError("Resolved LZ4 classifier differs from the declared runtime baseline")
        line = {"org.springframework": "framework", "org.springframework.data": "data",
                "org.springframework.boot": "boot"}.get(group)
        if line and version != expected[line]:
            raise ValueError(profile + " runtime baseline differs for " + group + ":" + artifact
                             + ": expected " + expected[line] + ", resolved " + version)
        if profile == "driver" and line:
            raise ValueError("Driver-only consumer unexpectedly requires Spring")
    return result


def run(arguments):
    repo = arguments.repo.resolve()
    work = arguments.work_dir.absolute()
    work.mkdir(parents=True, exist_ok=False)
    expected = baseline(repo)
    source_inputs = {name: digest((repo / name).read_bytes()) for name in
                     ("iotdb-client/r2dbc/pom.xml", "iotdb-client/r2dbc-spring-data/pom.xml",
                      "iotdb-client/r2dbc/src/test/scripts/check_consumer_runtime.py")}
    summary = {"schema": 1, "status": "running", "baseline": expected, "sourceInputsSha512": source_inputs, "profiles": [],
               "scope": "Standalone Maven resolution and runtime byte inventory; not a live-server test or vulnerability scan."}
    destination = arguments.summary or work / "summary.json"
    destination = destination.absolute()
    if destination.exists():
        raise ValueError("Refusing to overwrite existing consumer summary")
    destination.parent.mkdir(parents=True, exist_ok=True)
    maven = arguments.maven or str(repo / ("mvnw.cmd" if os.name == "nt" else "mvnw"))
    common = [maven, "-B", "--no-transfer-progress"]
    if arguments.local_repo:
        common.append("-Dmaven.repo.local=" + str(arguments.local_repo.resolve()))
    if arguments.offline:
        common.append("--offline")
    try:
        for profile in ("driver", "spring", "boot"):
            directory = work / profile
            directory.mkdir()
            pom = consumer_pom(profile, expected)
            (directory / "pom.xml").write_bytes(pom)
            with (directory / "maven.log").open("wb") as log:
                for goal, extra in (("tree", ["-Dscope=runtime", "-DoutputType=json", "-DoutputFile=runtime-tree.json"]),
                                    ("build-classpath", ["-DincludeScope=runtime", "-Dmdep.outputFile=runtime-classpath.txt"])):
                    completed = subprocess.run(common + ["-f", str(directory / "pom.xml"), PLUGIN + goal] + extra,
                                               cwd=directory, stdout=log, stderr=subprocess.STDOUT, check=False)
                    if completed.returncode:
                        raise ValueError(profile + " standalone Maven " + goal + " failed with exit " + str(completed.returncode))
            graph = json.loads((directory / "runtime-tree.json").read_text(encoding="utf-8-sig"))
            cp = (directory / "runtime-classpath.txt").read_text(encoding="utf-8-sig")
            components = verify_runtime(profile, expected, graph, cp)
            summary["profiles"].append({"name": profile, "status": "passed", "consumerPomSha512": digest(pom),
                                        "components": components, "classpath": [r["filename"] for r in components]})
            print("PASSED standalone consumer runtime: " + profile, flush=True)
        summary["status"] = "passed"
    except (ValueError, OSError, KeyError, TypeError) as error:
        summary["status"] = "failed"
        summary["error"] = str(error)
        raise
    finally:
        with destination.open("x", encoding="utf-8") as output:
            json.dump(summary, output, indent=2)
            output.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REPOSITORY)
    parser.add_argument("--maven", help="Maven executable or wrapper path")
    parser.add_argument("--local-repo", type=Path)
    parser.add_argument("--work-dir", type=Path, required=True, help="New directory; existing paths are refused")
    parser.add_argument("--summary", type=Path, help="New public summary file; contains coordinates/hashes, not local classpath paths")
    parser.add_argument("--offline", action="store_true")
    try:
        run(parser.parse_args())
    except (ValueError, OSError, KeyError, TypeError) as error:
        print("Consumer runtime gate refused: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
