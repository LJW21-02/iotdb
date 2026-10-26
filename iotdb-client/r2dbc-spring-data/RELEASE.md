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
# Offline delivery bundle

`scripts/package_release.py` creates an internal, offline ZIP containing the two R2DBC client
artifacts, the exact Maven runtime JARs used for acceptance, acceptance evidence, CycloneDX SBOMs,
Apache notices, and per-JAR license material. It does not publish to Maven or modify a repository.

Run it only from a clean checkout whose `HEAD` is the accepted commit. Build both client modules and
write Maven `dependency:build-classpath` output files for the runtime profiles used for acceptance
(for example, driver-only, Spring Data and Boot). Each file must contain one ordered line of absolute
JAR paths separated by the platform path separator, with no duplicate JAR in a profile. Profiles may
share dependencies. Freeze the classpaths and their JAR hashes before running acceptance, run the
consumer with those exact files and bytes, and set the acceptance status only after the tests pass.
Supply a schema-2 JSON acceptance record whose commit, client JAR/POM hashes, ordered classpaths and
complete runtime inventory match those accepted inputs. Supply a CycloneDX JSON SBOM covering exactly
the bundled JARs. Finally, supply a legal-review JSON that maps
every JAR SHA-512 to an SPDX license expression and at least one verified license or notice source;
the source may be an embedded `jar:META-INF/LICENSE...` member or a repository file using
`repo:path/to/file`.

```powershell
python scripts/package_release.py `
  --expected-commit 0123456789abcdef0123456789abcdef01234567 `
  --driver-jar ..\r2dbc\target\iotdb-r2dbc-2.0.11-SNAPSHOT.jar `
  --driver-pom ..\r2dbc\target\iotdb-r2dbc-2.0.11-SNAPSHOT.pom `
  --spring-jar target\iotdb-r2dbc-spring-data-2.0.11-SNAPSHOT.jar `
  --spring-pom target\iotdb-r2dbc-spring-data-2.0.11-SNAPSHOT.pom `
  --classpath acceptance-runtime-classpath.txt `
  --acceptance acceptance.json `
  --sbom runtime.cdx.json `
  --legal-review legal-review.json `
  --consumer-dir consumer `
  --output-dir ..\..\..\artifacts
```

The script refuses a dirty or wrong commit, missing/empty inputs, unresolved POM coordinates,
mismatched artifact or runtime hashes, altered classpath order, missing/extra/duplicate SBOM JARs,
SBOM coordinate/hash mismatches, missing legal coverage, symlinks, encrypted or unsafe ZIP
members, duplicate runtime filenames with different bytes, archive overwrite, and path traversal.
The output filename contains the exact version, accepted commit prefix, and manifest digest. The
archive is deterministic: ZIP timestamps, permissions, ordering, and compression are fixed. A
sidecar `<bundle>.zip.sha512` records the archive digest; `MANIFEST.sha512` records every bundled
member. The manifest and `release.json` make clear that the Maven `SNAPSHOT` coordinate remains
mutable and that only this offline bundle is content-pinned.

The required consumer directory contains exactly `BundleConsumer.java`, `run_consumer.py` and
`README.md`. These source/instruction files are copied into `consumer/` and covered by the manifest.
Compile outputs, endpoint credentials and fixture private material stay outside the bundle.
Use repeated `--evidence-file` arguments to include scrubbed suite summaries or logs under
`evidence/reports/`; filenames must be unique. Only provide public, redacted reports, never fixture
credentials, private configuration/key material or raw Surefire reports containing system properties.

## Acceptance and SBOM contracts

The acceptance JSON uses `schema: 2`, `gitCommit` (the full accepted commit), `status: "passed"`, and:

| Field | Contract |
| --- | --- |
| `artifacts` | Object mapping both client JAR and POM filenames to their lowercase SHA-512 digests. |
| `runtime` | Object mapping every bundled JAR filename, including both client JARs, to exactly `groupId`, `artifactId`, `version`, `classifier` and `sha512`. Use an empty classifier string when absent. |
| `classpaths` | Array of ordered filename arrays, one per `--classpath` argument, in the same argument order. The union must cover all runtime dependencies; client artifacts may also be selected separately. |

Every runtime filename must be `artifactId-version[-classifier].jar`. Client coordinates must match
the supplied POMs. Embedded Maven `pom.properties` for the named artifact are cross-checked when
present; JARs without that metadata require coordinates reviewed against the resolved Maven inputs.
The hash binds the record to the actual JAR bytes. The record is trusted test evidence: generating
an inventory alone does not prove that tests ran or justify a `passed` status.

Supply CycloneDX 1.4, 1.5 or 1.6 JSON with flat `library` components. Each component needs `group`,
`name`, `version`, a unique `bom-ref`, a Maven `purl`, exactly one `SHA-512` hash, and exactly one
property named `iotdb.bundle.filename` whose value is its runtime filename. A classifier, if any,
belongs in the Maven purl qualifier; `type` may be absent or `jar`. For example:

```json
{
  "type": "library",
  "group": "org.apache.iotdb",
  "name": "iotdb-r2dbc",
  "version": "2.0.11-SNAPSHOT",
  "bom-ref": "pkg:maven/org.apache.iotdb/iotdb-r2dbc@2.0.11-SNAPSHOT?type=jar",
  "purl": "pkg:maven/org.apache.iotdb/iotdb-r2dbc@2.0.11-SNAPSHOT?type=jar",
  "hashes": [{"alg": "SHA-512", "content": "<actual 128 lowercase hexadecimal characters>"}],
  "properties": [{"name": "iotdb.bundle.filename", "value": "iotdb-r2dbc-2.0.11-SNAPSHOT.jar"}]
}
```

Multiple `--sbom` files are permitted only when their component sets are disjoint and their union
covers exactly the bundle runtime. A full-reactor aggregate SBOM containing unrelated modules fails
this gate. SHA-1/256 hashes cannot substitute for the required SHA-512. Duplicate JSON object keys,
duplicate JARs within a classpath and duplicate SBOM references/components are rejected.

## Verify before consumption

Obtain the archive SHA-512 through the delivery channel and verify the unopened archive using a
trusted checkout of the verifier:

```powershell
python scripts/verify_bundle.py --bundle path\to\bundle.zip --sha512 <archive-sha512>
```

The verifier checks the archive digest, member paths and hashes, exact manifest coverage, accepted
client/runtime bytes, ordered classpaths, SBOM coverage and legal materials. After verification,
extract the archive and follow `consumer/README.md`; `tools/verify_bundle.py` and its supporting
module are included for later offline checks. Digest verification checks integrity against the
expected value; it is not a publisher signature or proof that an untrusted `passed` record is true.

The legal-review file is a release input, not an assertion generated by the packager. A reviewer must
verify each dependency's license and notice against its source distribution or embedded metadata
before running the script. The script copies those reviewed materials into `licenses/` and refuses
to produce a bundle when any runtime JAR is unreviewed.
