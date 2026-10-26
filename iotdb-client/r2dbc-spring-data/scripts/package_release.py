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
"""Assemble a content-pinned internal offline bundle; never publish to Maven."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import xml.etree.ElementTree as ET
from urllib.parse import parse_qsl, unquote, urlsplit
import zipfile


class PackageError(ValueError):
    pass


MAX_JAR_BYTES = 512 * 1024 * 1024
MAX_JAR_MEMBER_BYTES = 512 * 1024 * 1024
MAX_JAR_EXPANDED_BYTES = 2 * 1024 * 1024 * 1024
MAX_JAR_MEMBERS = 100_000


def sha512(data):
    return hashlib.sha512(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")


def parse_json(data):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise PackageError(f"Duplicate JSON object key: {key}")
            result[key] = value
        return result

    return json.loads(data, object_pairs_hook=unique_object)


def read_file(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise PackageError(f"Missing regular file, or symbolic link is not allowed: {path}")
    data = path.read_bytes()
    if not data:
        raise PackageError(f"Empty required file: {path}")
    return data


def git(repo, *args):
    result = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise PackageError(f"Git command failed: {result.stderr.strip()}")
    return result.stdout.strip()


def verify_source(repo, expected_commit):
    if not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
        raise PackageError("--expected-commit must be a full lowercase Git SHA-1")
    head = git(repo, "rev-parse", "HEAD")
    if head != expected_commit:
        raise PackageError(f"HEAD differs from accepted commit: {head}")
    if git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise PackageError("Refusing dirty source, including untracked files; commit the accepted source first")


def safe_name(name):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]*", name):
        raise PackageError(f"Unsafe artifact filename: {name!r}")
    return name


def validate_zip_name(name):
    parts = name.rstrip("/").split("/")
    if (
        not name
        or name.startswith("/")
        or "\\" in name
        or ":" in name
        or any(ord(char) < 32 or ord(char) == 127 for char in name)
        or any(part in ("", ".", "..") for part in parts)
    ):
        raise PackageError(f"Unsafe ZIP member path: {name!r}")


def inspect_jar(data, label):
    import io

    legal = {}
    seen = {}
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for entry in archive.infolist():
                if len(seen) >= MAX_JAR_MEMBERS:
                    raise PackageError(f"Too many ZIP members in {label}")
                validate_zip_name(entry.filename)
                folded = entry.filename.rstrip("/").casefold()
                if folded in seen:
                    raise PackageError(f"Duplicate/case-colliding ZIP member in {label}: {entry.filename}")
                parents = folded.split("/")[:-1]
                if any(seen.get("/".join(parents[:index])) is False for index in range(1, len(parents) + 1)):
                    raise PackageError(f"ZIP member is nested below a file in {label}: {entry.filename}")
                is_directory = entry.is_dir()
                if not is_directory and any(path.startswith(folded + "/") for path in seen):
                    raise PackageError(f"ZIP file collides with a directory in {label}: {entry.filename}")
                if entry.file_size > MAX_JAR_MEMBER_BYTES:
                    raise PackageError(f"Oversized ZIP member in {label}: {entry.filename}")
                if entry.compress_size and entry.file_size > entry.compress_size * 10_000:
                    raise PackageError(f"Excessive compression ratio in {label}: {entry.filename}")
                seen[folded] = is_directory
                if stat.S_ISLNK(entry.external_attr >> 16):
                    raise PackageError(f"Symbolic link ZIP member in {label}: {entry.filename}")
                if entry.flag_bits & 1:
                    raise PackageError(f"Encrypted ZIP member in {label}: {entry.filename}")
                leaf = PurePosixPath(entry.filename).name.upper()
                if not entry.is_dir() and leaf.startswith(("LICENSE", "NOTICE", "COPYING", "COPYRIGHT")):
                    if entry.file_size > 16 * 1024 * 1024:
                        raise PackageError(f"Oversized legal material in {label}: {entry.filename}")
                    legal[entry.filename] = archive.read(entry)
            if archive.testzip() is not None:
                raise PackageError(f"Corrupt ZIP member in {label}")
            if sum(entry.file_size for entry in archive.infolist()) > MAX_JAR_EXPANDED_BYTES:
                raise PackageError(f"Expanded JAR is too large: {label}")
    except (zipfile.BadZipFile, NotImplementedError, RuntimeError) as error:
        raise PackageError(f"Invalid JAR {label}: {error}") from error
    return legal


def pom_coordinates(data, expected_artifact):
    try:
        root = ET.fromstring(data)
    except ET.ParseError as error:
        raise PackageError(f"Invalid POM: {error}") from error
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}

    def value(path):
        return root.findtext(path, default="", namespaces=ns).strip()

    artifact = value("m:artifactId")
    group = value("m:groupId") or value("m:parent/m:groupId")
    version = value("m:version") or value("m:parent/m:version")
    if artifact != expected_artifact or group != "org.apache.iotdb" or not version or "${" in version:
        raise PackageError(f"Unexpected or unresolved POM coordinates: {group}:{artifact}:{version}")
    safe_name(version)
    return {"groupId": group, "artifactId": artifact, "version": version}


def runtime_files(classpath_files):
    files = []
    for classpath in classpath_files:
        text = read_file(classpath).decode("utf-8-sig").strip()
        separator = ";" if ";" in text else os.pathsep
        entries = text.split(separator)
        seen = set()
        for entry in entries:
            if not entry or "\n" in entry or "\r" in entry:
                raise PackageError(f"Expected one Maven build-classpath line: {classpath}")
            path = Path(entry)
            if not path.is_absolute():
                raise PackageError(f"Classpath entries must be absolute: {entry}")
            if path.suffix.lower() != ".jar":
                raise PackageError(f"Runtime classpath must contain JARs only: {entry}")
            if path.name.casefold() in seen:
                raise PackageError(f"Duplicate runtime JAR in one classpath: {path.name}")
            seen.add(path.name.casefold())
            files.append(path)
    if not files:
        raise PackageError("A nonempty runtime Maven build-classpath is required")
    return files


def coordinate_key(record):
    return tuple(record[key] for key in ("groupId", "artifactId", "version", "classifier"))


def verify_embedded_coordinates(data, name, record):
    """Cross-check unambiguous Maven metadata when the publisher embedded it in the JAR."""
    import io

    matches = []
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for member in archive.namelist():
            if not re.fullmatch(r"META-INF/maven/[^/]+/[^/]+/pom\.properties", member):
                continue
            values = {}
            for line in archive.read(member).decode("iso-8859-1").splitlines():
                key, separator, value = line.partition("=")
                if separator and not key.lstrip().startswith(("#", "!")):
                    values[key.strip()] = value.strip()
            if values.get("artifactId") == record["artifactId"]:
                matches.append(values)
    for values in matches:
        if tuple(values.get(key) for key in ("groupId", "artifactId", "version")) != coordinate_key(record)[:3]:
            raise PackageError(f"Embedded Maven coordinates differ from accepted runtime: {name}")


def verify_runtime(runtime, jars, client_coordinates):
    if not isinstance(runtime, dict) or set(runtime) != set(jars):
        raise PackageError("Acceptance runtime must cover exactly every bundled client/runtime JAR")
    seen_coordinates = set()
    for name, record in runtime.items():
        required = {"groupId", "artifactId", "version", "classifier", "sha512"}
        if not isinstance(record, dict) or set(record) != required:
            raise PackageError(f"Invalid runtime coordinate/hash record: {name}")
        if any(not isinstance(value, str) for value in record.values()):
            raise PackageError(f"Runtime coordinates and hashes must be strings: {name}")
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", record["groupId"]):
            raise PackageError(f"Invalid runtime groupId: {name}")
        for key in ("artifactId", "version"):
            safe_name(record[key])
        if record["classifier"]:
            safe_name(record["classifier"])
        suffix = "-" + record["classifier"] if record["classifier"] else ""
        if name != f"{record['artifactId']}-{record['version']}{suffix}.jar":
            raise PackageError(f"Runtime filename differs from coordinates: {name}")
        if not re.fullmatch(r"[0-9a-f]{128}", record["sha512"]) or record["sha512"] != sha512(jars[name]):
            raise PackageError(f"Acceptance runtime hash differs from bundled JAR: {name}")
        verify_embedded_coordinates(jars[name], name, record)
        key = coordinate_key(record)
        if key in seen_coordinates:
            raise PackageError(f"Duplicate runtime Maven coordinate: {name}")
        seen_coordinates.add(key)
        if name in client_coordinates:
            expected = dict(client_coordinates[name], classifier="")
            if key != coordinate_key(expected):
                raise PackageError(f"Client runtime coordinates differ from POM: {name}")


def verify_classpaths(classpaths, actual, runtime):
    if not isinstance(classpaths, list) or classpaths != actual:
        raise PackageError("Acceptance classpaths differ from the supplied ordered runtime classpaths")
    included = set()
    for entries in classpaths:
        if (not isinstance(entries, list) or not entries or any(not isinstance(name, str) for name in entries)
                or len(entries) != len(set(entries))):
            raise PackageError("Acceptance classpaths must be nonempty and contain no duplicate JARs")
        if not set(entries).issubset(runtime):
            raise PackageError("Acceptance classpath refers to an unbundled JAR")
        included.update(entries)
    # The top-level client artifacts may be selected separately by a consumer. Every other JAR
    # must have occurred in an accepted classpath; a release cannot add an untested dependency.
    return included


def verify_sboms(documents, runtime):
    seen_names, seen_refs, seen_coordinates = set(), set(), set()
    for document in documents:
        if (not isinstance(document, dict) or document.get("bomFormat") != "CycloneDX"
                or document.get("specVersion") not in ("1.4", "1.5", "1.6")
                or not isinstance(document.get("components"), list) or not document["components"]):
            raise PackageError("Expected nonempty CycloneDX JSON SBOM, version 1.4, 1.5 or 1.6")
        for component in document["components"]:
            if not isinstance(component, dict) or component.get("type") != "library" or component.get("components"):
                raise PackageError("SBOM must list bundled JARs as flat library components")
            properties = component.get("properties", [])
            if not isinstance(properties, list):
                raise PackageError("SBOM component properties must be an array")
            names = [item.get("value") for item in properties if isinstance(item, dict) and item.get("name") == "iotdb.bundle.filename"]
            if len(names) != 1 or not isinstance(names[0], str) or names[0] not in runtime:
                raise PackageError("SBOM component requires one matching iotdb.bundle.filename property")
            name = names[0]
            if name in seen_names:
                raise PackageError(f"Duplicate SBOM JAR component: {name}")
            seen_names.add(name)
            record = runtime[name]
            if (component.get("group"), component.get("name"), component.get("version")) != tuple(record[key] for key in ("groupId", "artifactId", "version")):
                raise PackageError(f"SBOM coordinates differ from accepted runtime: {name}")
            reference = component.get("bom-ref")
            if not isinstance(reference, str) or not reference or reference in seen_refs:
                raise PackageError(f"SBOM component has missing or duplicate bom-ref: {name}")
            seen_refs.add(reference)
            purl = component.get("purl")
            if not isinstance(purl, str):
                raise PackageError(f"SBOM component requires a Maven purl: {name}")
            try:
                parsed = urlsplit(purl)
                qualifiers = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
            except ValueError as error:
                raise PackageError(f"Invalid SBOM Maven purl: {name}") from error
            if len(qualifiers) != len(dict(qualifiers)):
                raise PackageError(f"Duplicate SBOM purl qualifier: {name}")
            qualifiers = dict(qualifiers)
            expected_path = f"maven/{record['groupId']}/{record['artifactId']}@{record['version']}"
            if (parsed.scheme != "pkg" or parsed.netloc or parsed.fragment or unquote(parsed.path) != expected_path
                    or set(qualifiers) - {"classifier", "type"}
                    or qualifiers.get("classifier", "") != record["classifier"]
                    or qualifiers.get("type", "jar") != "jar"):
                raise PackageError(f"SBOM Maven purl differs from accepted runtime: {name}")
            key = coordinate_key(record)
            if key in seen_coordinates:
                raise PackageError(f"Duplicate SBOM Maven coordinate: {name}")
            seen_coordinates.add(key)
            hashes = component.get("hashes", [])
            if not isinstance(hashes, list):
                raise PackageError("SBOM component hashes must be an array")
            sha_hashes = [item.get("content") for item in hashes if isinstance(item, dict) and item.get("alg") == "SHA-512"]
            if sha_hashes != [record["sha512"]]:
                raise PackageError(f"SBOM SHA-512 differs from accepted runtime: {name}")
    if seen_names != set(runtime):
        raise PackageError("SBOM components do not cover exactly every bundled JAR")


def collect_legal(jars, review_path, repo):
    review_path = Path(review_path)
    review = parse_json(read_file(review_path))
    if not isinstance(review, dict) or review.get("schema") != 1 or not isinstance(review.get("components"), dict):
        raise PackageError("Legal review must have schema=1 and a components object keyed by JAR SHA512")
    files, index = {}, {}
    for name, data in sorted(jars.items()):
        digest = sha512(data)
        component = review["components"].get(digest)
        if not isinstance(component, dict) or not isinstance(component.get("license"), str) or not component["license"].strip():
            raise PackageError(f"Missing reviewed license expression for {name} ({digest})")
        references = component.get("files")
        if not isinstance(references, list) or not references:
            raise PackageError(f"Missing reviewed license/notice files for {name}")
        embedded = inspect_jar(data, name)
        materials = dict(embedded)
        for reference in references:
            if not isinstance(reference, str):
                raise PackageError(f"Invalid legal file reference for {name}")
            if reference.startswith("jar:"):
                member = reference[4:]
                if member not in embedded:
                    raise PackageError(f"Referenced embedded legal material is missing: {name}:{member}")
                continue
            if reference.startswith("repo:"):
                relative = reference[5:]
                validate_zip_name(relative)
                path = repo / relative
            else:
                path = Path(reference)
                if not path.is_absolute():
                    path = review_path.parent / path
            reference_key = hashlib.sha256(reference.encode("utf-8")).hexdigest()[:12]
            materials[f"external/{reference_key}-{path.name}"] = read_file(path)
        bundled = []
        for number, (material_name, material_data) in enumerate(sorted(materials.items()), 1):
            leaf = re.sub(r"[^A-Za-z0-9._+-]", "_", PurePosixPath(material_name).name)
            destination = f"licenses/{name}/{number:02d}-{leaf}"
            files[destination] = material_data
            bundled.append(destination)
        index[name] = {"sha512": digest, "license": component["license"], "files": bundled}
    files["licenses/index.json"] = json_bytes({"schema": 1, "components": index})
    return files


def assemble(args):
    repo = Path(args.repo).resolve()
    verify_source(repo, args.expected_commit)
    payload, jars, client_hashes, coordinates, client_coordinates = {}, {}, {}, [], {}

    def add_jar(path, destination):
        path = Path(path)
        name = safe_name(path.name)
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_JAR_BYTES:
            raise PackageError(f"Missing regular JAR, or JAR exceeds size limit: {path}")
        data = read_file(path)
        if name.casefold() in {item.casefold() for item in jars}:
            original = next(item for item in jars if item.casefold() == name.casefold())
            if name != original or jars[original] != data:
                raise PackageError(f"Duplicate JAR filename with different content/case: {name}")
            return
        inspect_jar(data, name)
        jars[name] = data
        payload[f"{destination}/{name}"] = data

    for artifact, jar_path, pom_path in (
        ("iotdb-r2dbc", args.driver_jar, args.driver_pom),
        ("iotdb-r2dbc-spring-data", args.spring_jar, args.spring_pom),
    ):
        pom_path, jar_path = Path(pom_path), Path(jar_path)
        pom = read_file(pom_path)
        coordinate = pom_coordinates(pom, artifact)
        if jar_path.name != f"{artifact}-{coordinate['version']}.jar":
            raise PackageError(f"JAR filename does not match POM coordinates: {jar_path.name}")
        add_jar(jar_path, "clients")
        pom_name = f"{artifact}-{coordinate['version']}.pom"
        payload[f"clients/{pom_name}"] = pom
        client_hashes[jar_path.name] = sha512(jars[jar_path.name])
        client_hashes[pom_name] = sha512(pom)
        coordinates.append(coordinate)
        client_coordinates[jar_path.name] = coordinate
    if coordinates[0]["version"] != coordinates[1]["version"]:
        raise PackageError("The two client versions differ")
    actual_classpaths = []
    for classpath in args.classpath:
        paths = runtime_files([classpath])
        actual_classpaths.append([path.name for path in paths])
        for path in paths:
            add_jar(path, "lib")

    acceptance_data = read_file(args.acceptance)
    acceptance = parse_json(acceptance_data)
    if (
        not isinstance(acceptance, dict)
        or not isinstance(acceptance.get("artifacts"), dict)
        or acceptance.get("schema") != 2
        or acceptance.get("gitCommit") != args.expected_commit
        or acceptance.get("status") != "passed"
        or set(acceptance.get("artifacts", {})) != set(client_hashes)
        or any(
            not isinstance(acceptance.get("artifacts", {}).get(name), str)
            or not re.fullmatch(r"[0-9a-f]{128}", acceptance["artifacts"][name])
            or acceptance["artifacts"][name] != digest
            for name, digest in client_hashes.items()
        )
    ):
        raise PackageError("Acceptance schema=2 must pass for this commit and bind both client JAR/POM SHA512 values")
    runtime = acceptance.get("runtime")
    verify_runtime(runtime, jars, client_coordinates)
    included = verify_classpaths(acceptance.get("classpaths"), actual_classpaths, runtime)
    if included | set(client_coordinates) != set(runtime):
        raise PackageError("Acceptance classpaths omit a bundled runtime dependency")
    payload["evidence/acceptance.json"] = acceptance_data
    documents = []
    for number, path in enumerate(args.sbom, 1):
        data = read_file(path)
        document = parse_json(data)
        documents.append(document)
        payload[f"evidence/sbom-{number}.json"] = data
    verify_sboms(documents, runtime)
    payload["LICENSE"] = read_file(repo / "LICENSE")
    payload["NOTICE"] = read_file(repo / "NOTICE")
    payload.update(collect_legal(jars, args.legal_review, repo))
    payload["tools/package_release.py"] = read_file(Path(__file__))
    payload["tools/verify_bundle.py"] = read_file(Path(__file__).with_name("verify_bundle.py"))
    consumer_files = ("BundleConsumer.java", "run_consumer.py", "README.md")
    consumer_dir = Path(args.consumer_dir)
    if consumer_dir.is_symlink() or not consumer_dir.is_dir():
        raise PackageError("Consumer directory must be a regular directory")
    if {entry.name for entry in consumer_dir.iterdir()} != set(consumer_files):
        raise PackageError("Consumer directory must contain only BundleConsumer.java, run_consumer.py and README.md")
    for name in consumer_files:
        payload["consumer/" + name] = read_file(consumer_dir / name)
    report_names = []
    for path in getattr(args, "evidence_file", None) or []:
        path = Path(path)
        name = "evidence/reports/" + safe_name(path.name)
        if name in payload:
            raise PackageError(f"Duplicate evidence filename: {path.name}")
        payload[name] = read_file(path)
        report_names.append(name)
    payload["release.json"] = json_bytes(
        {
            "schema": 2,
            "gitCommit": args.expected_commit,
            "distribution": "internal-offline-bundle",
            "coordinates": coordinates,
            "coordinatePolicy": "SNAPSHOT Maven coordinates remain mutable; only this bundle's manifest and archive SHA512 pin these exact bytes. This is not a Maven Central release.",
            "clients": client_hashes,
            "runtime": runtime,
            "classpaths": actual_classpaths,
            "acceptance": "evidence/acceptance.json",
            "sbom": sorted(name for name in payload if name.startswith("evidence/sbom-")),
            "legal": "licenses/index.json",
            "consumer": ["consumer/" + name for name in consumer_files],
            "reports": sorted(report_names),
        }
    )
    manifest = "".join(f"{sha512(data)}  {name}\n" for name, data in sorted(payload.items())).encode("utf-8")
    manifest_digest = sha512(manifest)
    payload["MANIFEST.sha512"] = manifest
    verify_source(repo, args.expected_commit)
    output_dir = Path(args.output_dir).resolve()
    try:
        output_dir.relative_to(repo)
    except ValueError:
        pass
    else:
        raise PackageError("Output directory must be outside the source checkout")
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"iotdb-r2dbc-{coordinates[0]['version']}-g{args.expected_commit[:12]}-{manifest_digest[:24]}.zip"
    output = output_dir / filename
    sidecar = output.with_suffix(output.suffix + ".sha512")
    if output.exists() or sidecar.exists():
        raise PackageError(f"Refusing to overwrite existing bundle/checksum: {output}")
    created = False
    sidecar_created = False
    try:
        with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_STORED) as archive:
            created = True
            for name, data in sorted(payload.items()):
                validate_zip_name(name)
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.create_system = 3
                entry.external_attr = (stat.S_IFREG | 0o644) << 16
                archive.writestr(entry, data)
        archive_digest = sha512(output.read_bytes())
        with sidecar.open("x", encoding="utf-8", newline="\n") as checksum:
            sidecar_created = True
            checksum.write(f"{archive_digest}  {output.name}\n")
    except BaseException:
        if created:
            output.unlink(missing_ok=True)
        if sidecar_created:
            sidecar.unlink(missing_ok=True)
        raise
    return {"archive": str(output.resolve()), "sha512": archive_digest, "manifestSha512": manifest_digest, "gitCommit": args.expected_commit}


def verify_bundle(path, expected_sha512):
    """Verify a delivered archive before any JAR is executed or files are extracted."""
    path = Path(path)
    if not isinstance(expected_sha512, str) or not re.fullmatch(r"[0-9a-f]{128}", expected_sha512):
        raise PackageError("An independently obtained lowercase archive SHA-512 is required")
    data = read_file(path)
    if sha512(data) != expected_sha512:
        raise PackageError("Bundle SHA-512 differs from the expected archive digest")
    inspect_jar(data, path.name)
    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        if "MANIFEST.sha512" not in names:
            raise PackageError("Bundle has no MANIFEST.sha512")
        manifest = {}
        for line in archive.read("MANIFEST.sha512").decode("utf-8").splitlines():
            match = re.fullmatch(r"([0-9a-f]{128})  (.+)", line)
            if match is None:
                raise PackageError("Malformed bundle manifest line")
            digest, name = match.groups()
            validate_zip_name(name)
            if name in manifest:
                raise PackageError(f"Duplicate manifest member: {name}")
            manifest[name] = digest
        if set(manifest) != names - {"MANIFEST.sha512"}:
            raise PackageError("Manifest does not cover exactly every bundle member")
        for name, digest in manifest.items():
            if sha512(archive.read(name)) != digest:
                raise PackageError(f"Bundle member SHA-512 mismatch: {name}")
        release = parse_json(archive.read("release.json"))
        if release.get("schema") != 2 or release.get("distribution") != "internal-offline-bundle":
            raise PackageError("Unsupported bundle release schema/distribution")
        jars, client_coordinates, client_hashes = {}, {}, {}
        for name in sorted(names):
            parts = name.split("/")
            if len(parts) == 2 and parts[0] in ("clients", "lib") and parts[1].endswith(".jar"):
                if parts[1] in jars:
                    raise PackageError(f"JAR occurs in multiple bundle directories: {parts[1]}")
                jars[parts[1]] = archive.read(name)
                inspect_jar(jars[parts[1]], parts[1])
        coordinates = release.get("coordinates")
        if not isinstance(coordinates, list) or len(coordinates) != 2:
            raise PackageError("Bundle must declare exactly two client artifacts")
        for artifact, coordinate in zip(("iotdb-r2dbc", "iotdb-r2dbc-spring-data"), coordinates):
            stem = f"{artifact}-{coordinate.get('version')}"
            pom_name = stem + ".pom"
            jar_name = stem + ".jar"
            pom = archive.read("clients/" + pom_name)
            if pom_coordinates(pom, artifact) != coordinate:
                raise PackageError(f"Bundled client POM coordinates differ: {artifact}")
            client_coordinates[jar_name] = coordinate
            client_hashes[jar_name] = sha512(archive.read("clients/" + jar_name))
            client_hashes[pom_name] = sha512(pom)
        acceptance = parse_json(archive.read(release["acceptance"]))
        if (acceptance.get("schema") != 2 or acceptance.get("status") != "passed"
                or acceptance.get("gitCommit") != release.get("gitCommit")
                or not re.fullmatch(r"[0-9a-f]{40}", str(release.get("gitCommit")))
                or acceptance.get("artifacts") != client_hashes or release.get("clients") != client_hashes
                or acceptance.get("runtime") != release.get("runtime")
                or acceptance.get("classpaths") != release.get("classpaths")):
            raise PackageError("Bundle acceptance differs from release/client bytes")
        runtime = release["runtime"]
        verify_runtime(runtime, jars, client_coordinates)
        included = verify_classpaths(release["classpaths"], release["classpaths"], runtime)
        if included | set(client_coordinates) != set(runtime):
            raise PackageError("Bundle classpaths omit a runtime dependency")
        documents = [parse_json(archive.read(name)) for name in release["sbom"]]
        verify_sboms(documents, runtime)
        required_consumer = {"consumer/BundleConsumer.java", "consumer/run_consumer.py", "consumer/README.md"}
        if (not isinstance(release.get("consumer"), list) or len(release["consumer"]) != 3
                or set(release["consumer"]) != required_consumer or not required_consumer.issubset(manifest)):
            raise PackageError("Bundle is missing its standalone consumer sources/instructions")
        reports = release.get("reports", [])
        if (not isinstance(reports, list) or len(reports) != len(set(reports))
                or set(reports) != {name for name in manifest if name.startswith("evidence/reports/")}):
            raise PackageError("Bundle report index differs from its manifest")
        legal = parse_json(archive.read(release["legal"]))
        if legal.get("schema") != 1 or set(legal.get("components", {})) != set(runtime):
            raise PackageError("Bundle legal index does not cover every JAR")
        for name, record in legal["components"].items():
            if (record.get("sha512") != runtime[name]["sha512"] or not record.get("license")
                    or not record.get("files") or any(member not in manifest for member in record["files"])):
                raise PackageError(f"Bundle legal material is incomplete: {name}")
        return {"archive": str(path.resolve()), "gitCommit": release["gitCommit"],
                "sha512": expected_sha512, "jars": len(jars), "members": len(names),
                "classpaths": release["classpaths"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    for name in ("expected-commit", "driver-jar", "driver-pom", "spring-jar", "spring-pom", "acceptance", "legal-review", "consumer-dir", "output-dir"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--classpath", type=Path, action="append", required=True)
    parser.add_argument("--sbom", type=Path, action="append", required=True)
    parser.add_argument("--evidence-file", type=Path, action="append")
    args = parser.parse_args()
    try:
        print(json.dumps(assemble(args), indent=2))
    except (PackageError, OSError, json.JSONDecodeError, UnicodeError) as error:
        print(f"Packaging refused: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
