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

"""Private, isolated R2DBC live fixtures on Windows and Linux (JDK 17+, Python 3).

start --distribution ALL_BIN --run-root TARGET --mode plain|tls|cluster
run-tests --state STATE_JSON --suite plain|tls|cluster|node-loss|tck|boot|spring-data
stop --state STATE_JSON

All data/config/classes are generated under a new run directory. Credentials are
random and remain in its private directory. Stops check both PID and the exact
IOTDB_HOME JVM argument. No shared installation or pre-existing fixture is used.
"""

import argparse
import ctypes
import json
import os
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from verify_tck import verify as verify_tck

SCRIPTS = Path(__file__).resolve().parent
REPOSITORY = SCRIPTS.parents[4]


def maven_command():
    """Use an explicitly supplied Maven binary for hermetic runners."""
    configured = os.environ.get("R2DBC_MAVEN")
    if configured:
        resolved = shutil.which(configured)
        return resolved or configured
    return str(REPOSITORY / ("mvnw.cmd" if os.name == "nt" else "mvnw"))
JAVA_OPENS = [
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
]
EXPECTED_CASES = {
    "compatibility": {("IoTDBR2dbcCompatibilityIT", name) for name in (
        "configuredTransportAndTemporalValuesRoundTrip",
        "serverVersionAndOptionalClientCertificatePolicyAreVerified",
    )},
    "long-soak": {("IoTDBR2dbcSoakIT", "sustainedMixedWorkloadKeepsResourcesBounded")},
    "soak": {("IoTDBR2dbcSoakIT", name) for name in (
        "repeatedPreparedExecutionsDoNotLeakServerHandles",
        "pagesThroughManyFetchesWithoutLosingOrRepeatingRows",
        "slowConsumerReceivesRowsOnDemandAcrossPages",
        "parallelConnectionsShareOneSelectorRuntime",
        "connectCloseCyclesReuseOneSelectorThread",
        "unresponsiveServerFailsWithinTheRpcTimeout",
        "closingAConnectionWithASilentServerStaysWithinTheCleanupTimeout",
        "abandoningAResultWithASilentServerDestroysTheConnection",
    )},
    "plain": {
        ("IoTDBR2dbcIT", name) for name in (
            "tableModelRoundTripWithPagingAndTypedParameters",
            "treeDialectKeepsTheTimeColumnAndUsesLiteralBinding",
            "timestampParametersUseEpochTicksAndFailedPreparesAreReleased",
            "batchStatementsRunInOrderAndStopAtTheFirstFailure",
            "statementTimeoutsAndClosedConnectionAreEnforced",
            "wrongCredentialsAreRejectedAsPermissionErrors",
        )
    } | {
        ("IoTDBR2dbcPoolIT", "pooledConnectionsAreValidatedAndReused"),
        ("IoTDBR2dbcObjectIT", "communityServerRejectsStoredObjectColumnsWithoutInvalidatingTheConnection"),
        ("IoTDBR2dbcObjectIT", "nullObjectExpressionKeepsItsMetadataAndOtherStoredColumnsReadable"),
    },
    "tls": {("IoTDBR2dbcTlsIT", name) for name in (
        "driverConnectsOverTlsAndPagesResults", "untrustedCertificateFailsWithoutDowngrading",
        "tlsEndpointRejectsAPlaintextConnectionWithABoundedFrameError", "tlsEndpointReallySpeaksTls",
    )},
    "cluster": {("IoTDBR2dbcClusterIT", name) for name in (
        "dataWrittenThroughOneDataNodeIsReadableFromTheOthers", "typedParameterQueryRunsFromAnyDataNode",
        "writesOnEveryDataNodeCompleteWithNoRedirectBudget", "brokenConnectionFailsFastAndIsNotReused",
    )},
    "node-loss": {("IoTDBR2dbcClusterIT", "stoppedDataNodeInvalidatesItsConnectionWhileAnotherNodeStaysHealthy")},
    "boot": {("IoTDBR2dbcSpringBootSampleIT", name) for name in (
        "bootStartsAndRunsExplicitSqlThroughDatabaseClient", "defaultDatabaseClientAutoConfigurationFailsAsDocumented",
    )},
    "spring-data": {("IoTDBSpringDataRepositoryIT", "bootDiscoversTheDialectAndExecutesRepositoriesWithoutFakeCounts")},
}


def save(state):
    path = Path(state["stateFile"])
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_state(path):
    state = json.loads(path.resolve().read_text(encoding="utf-8"))
    if Path(state["stateFile"]).resolve() != path.resolve():
        raise ValueError("State file does not belong to this run")
    return state


def java_tool(name):
    executable = shutil.which(name)
    if not executable:
        raise ValueError(f"Required JDK tool is absent: {name}")
    return executable


def windows_process(pid):
    # ProcessHandle.Info.arguments is unavailable on some Windows JDKs. CIM is
    # queried into memory; never print the full command line or configuration.
    script = (
        "$p=Get-CimInstance Win32_Process -Filter 'ProcessId = " + str(int(pid)) + "'; "
        "if($null -ne $p){[pscustomobject]@{commandLine=$p.CommandLine; "
        "creationFileTime=[string]$p.CreationDate.ToFileTimeUtc()}|ConvertTo-Json -Compress}"
    )
    result = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", script], text=True, capture_output=True, check=True, timeout=20, creationflags=subprocess.CREATE_NO_WINDOW)
    return json.loads(result.stdout) if result.stdout.strip() else None


def windows_arguments(command_line):
    shell32 = ctypes.WinDLL("shell32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    shell32.CommandLineToArgvW.argtypes = [ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_int)]
    shell32.CommandLineToArgvW.restype = ctypes.POINTER(ctypes.c_wchar_p)
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    count = ctypes.c_int()
    parsed = shell32.CommandLineToArgvW(command_line, ctypes.byref(count))
    if not parsed:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return [parsed[index] for index in range(count.value)]
    finally:
        kernel32.LocalFree(ctypes.cast(parsed, ctypes.c_void_p))


def stop_windows_node(node):
    # Hold a native process handle through identity checks and termination, so
    # PID reuse cannot switch the process being terminated after verification.
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    kernel32.GetProcessTimes.restype = wintypes.BOOL
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(0x100000 | 0x1000 | 0x0001, False, int(node["pid"]))
    if not handle:
        if ctypes.get_last_error() == 87:  # ERROR_INVALID_PARAMETER: no such PID.
            return
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        snapshot = windows_process(node["pid"])
        if snapshot is None:
            return
        created, exited, kernel, user = (wintypes.FILETIME() for _ in range(4))
        if not kernel32.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user)):
            raise ctypes.WinError(ctypes.get_last_error())
        creation_time = (created.dwHighDateTime << 32) | created.dwLowDateTime
        # CIM_DATETIME records microseconds; FILETIME additionally has a tenth
        # microsecond digit. Compare at the precision actually returned by CIM.
        if creation_time // 10 != int(snapshot["creationFileTime"]) // 10:
            raise RuntimeError("Fixture PID was reused during identity verification")
        if node.get("creationFileTime") and creation_time // 10 != int(node["creationFileTime"]) // 10:
            raise RuntimeError("Fixture process start time does not match")
        command = windows_arguments(snapshot["commandLine"] or "")
        homes = [argument[len("-DIOTDB_HOME="):] for argument in command if argument.startswith("-DIOTDB_HOME=")]
        if len(homes) != 1 or Path(homes[0]).resolve() != Path(node["home"]).resolve() or "ServerLauncher" not in command or node["main"] not in command:
            raise RuntimeError("Fixture process identity does not match")
        if not kernel32.TerminateProcess(handle, 1):
            raise ctypes.WinError(ctypes.get_last_error())
        if kernel32.WaitForSingleObject(handle, 20_000) != 0:
            raise TimeoutError("Fixture process did not terminate before its deadline")
    finally:
        kernel32.CloseHandle(handle)


def write_properties(path, entries):
    # Generated values are ASCII; use forward slashes because java.util.Properties
    # treats backslashes as escapes, including in Windows paths.
    path.write_text("".join(f"{key}={str(value).replace(chr(92), '/')}\n" for key, value in entries.items()), encoding="utf-8")


def private_directory(path):
    path.mkdir(mode=0o700)
    if os.name == "nt":
        owner = subprocess.check_output(["whoami"], text=True).strip()
        subprocess.run(["icacls", str(path), "/inheritance:r", "/grant:r", f"{owner}:(OI)(CI)F", "*S-1-5-18:(OI)(CI)F"], check=True, stdout=subprocess.DEVNULL)
    else:
        path.chmod(0o700)


def reserve_ports(count):
    sockets = []
    for _ in range(count):
        reservation = socket.socket()
        reservation.bind(("127.0.0.1", 0))
        sockets.append(reservation)
    return sockets


def listener(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


def wait_listener(process, port, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("Fixture JVM exited before its listener became ready; inspect role logs")
        if listener(port):
            return
        time.sleep(0.3)
    raise TimeoutError("Fixture listener did not become ready before its deadline")


def resolve_lib(distribution):
    candidates = [distribution / "lib"] + [child / "lib" for child in distribution.iterdir() if child.is_dir()]
    libraries = [candidate.resolve() for candidate in candidates if candidate.is_dir() and any(candidate.glob("*.jar"))]
    if len(libraries) != 1:
        raise ValueError("Expected exactly one all-bin lib directory")
    return libraries[0]


def start_node(state, name):
    node = state["nodes"][name]
    home = Path(node["home"])
    conf = Path(node["conf"])
    command = [state["java"], "-Xms128m", "-Xmx768m", "-XX:ActiveProcessorCount=2", "-XX:MaxDirectMemorySize=256m", "-XX:+UseG1GC"] + JAVA_OPENS
    for key, value in {
        "IOTDB_HOME": home, "CONFIGNODE_HOME": home,
        "IOTDB_CONF": conf, "CONFIGNODE_CONF": conf,
        "TSFILE_HOME": home, "TSFILE_CONF": conf,
        "IOTDB_LOG_DIR": home / "logs", "CONFIGNODE_LOG_DIR": home / "logs",
        "fixture.credentials": state["credentials"],
        "logback.configurationFile": conf / "fixture-logback.xml",
    }.items():
        command.append(f"-D{key}={value}")
    command += ["-cp", os.pathsep.join([state["classes"], str(Path(state["distributionLib"]) / "*")]), "ServerLauncher", node["main"]]
    with (home / "server.log").open("ab") as output:
        process = subprocess.Popen(command, cwd=home, stdout=output, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
    # In run_acceptance this Python process stays the JVM's parent. Reap exited
    # JVMs promptly on Unix, so a separate ProcessHandle controller never waits
    # for a zombie whose parent is itself blocked waiting for that controller.
    threading.Thread(target=process.wait, daemon=True).start()
    node["pid"] = process.pid
    if os.name == "nt":
        snapshot = windows_process(process.pid)
        if snapshot is None:
            raise RuntimeError("Fixture JVM exited before identity could be recorded")
        node["creationFileTime"] = snapshot["creationFileTime"]
    node["running"] = True
    save(state)
    wait_listener(process, node["listener"])


def control(state, name):
    node = state["nodes"][name]
    if not node.get("pid"):
        return
    if os.name == "nt":
        stop_windows_node(node)
    else:
        subprocess.run([state["java"], "-cp", state["classes"], "FixtureNodeControl", str(node["pid"]), node["home"]], check=True, timeout=30, stdout=subprocess.DEVNULL)
    deadline = time.monotonic() + 15
    while listener(node["listener"]) and time.monotonic() < deadline:
        time.sleep(0.2)
    if listener(node["listener"]):
        raise RuntimeError("Stopped fixture process still has a listening port")
    node["running"] = False
    save(state)


def stop(state):
    failures = []
    for name in reversed(list(state["nodes"])):
        try:
            control(state, name)
        except Exception as error:
            failures.append(f"{name}: {type(error).__name__}")
    remaining = [port for port in state["ports"] if listener(port)]
    state["cleanup"] = {"checkedAt": datetime.now(timezone.utc).isoformat(), "listeningPorts": remaining, "failures": failures}
    state["stage"] = "stopped" if not failures and not remaining else "cleanup-failed"
    save(state)
    # Server logs can mention an administrator name. Only scrubbed copies belong
    # in public CI evidence; configuration and key material are never copied.
    if Path(state["credentials"]).is_file():
        sensitive = list(read_credentials(state).values())
        public_logs = Path(state["runDirectory"]) / "artifacts/node-logs"
        public_logs.mkdir(exist_ok=True)
        for name, node in state["nodes"].items():
            source = Path(node["home"]) / "server.log"
            if source.exists():
                text = source.read_text(encoding="utf-8", errors="replace")
                for secret in sensitive:
                    text = text.replace(secret, "[REDACTED]")
                (public_logs / (name + ".log")).write_text(text, encoding="utf-8")
    if failures or remaining:
        raise RuntimeError("Fixture cleanup failed; inspect the state file")
    print("STOPPED; all fixture ports released", flush=True)


def start(arguments):
    distribution = arguments.distribution.resolve()
    lib = resolve_lib(distribution)
    arguments.run_root.mkdir(parents=True, exist_ok=True)
    run = arguments.run_root.resolve() / ("r2dbc-" + arguments.mode + "-" + uuid.uuid4().hex[:12])
    run.mkdir(mode=0o700)
    private = run / "private"
    private_directory(private)
    classes = run / "classes"
    classes.mkdir()
    artifacts = run / "artifacts"
    artifacts.mkdir()
    cluster = arguments.mode in ("cluster", "tls-cluster")
    tls = arguments.mode in ("tls", "mtls", "tls-cluster")
    client_auth = arguments.mode == "mtls"
    ports_reserved = reserve_ports(17 if cluster else 7)
    ports = [item.getsockname()[1] for item in ports_reserved]
    state = {"stateFile": str(run / "state.json"), "runDirectory": str(run), "distributionLib": str(lib), "mode": arguments.mode, "protocol": getattr(arguments, "protocol", "binary"), "precision": getattr(arguments, "precision", "ms"), "tls": tls, "clientAuth": client_auth, "cluster": cluster, "ports": ports, "rpcPort": ports[2], "credentials": str(private / "credentials.properties"), "classes": str(classes), "java": java_tool("java"), "nodes": {}, "stage": "preparing"}
    save(state)
    print("STATE=" + state["stateFile"], flush=True)
    try:
        credentials = {"username": "r2dbc_" + secrets.token_hex(6), "password": secrets.token_hex(20)}
        write_properties(Path(state["credentials"]), credentials)
        tls_properties = {}
        if tls:
            tls_password = secrets.token_hex(20)
            tls_env = dict(os.environ, R2DBC_FIXTURE_STORE_PASSWORD=tls_password)
            key_store, trust_store, certificate = private / "server.p12", private / "trust.p12", private / "server.cer"
            keytool = java_tool("keytool")
            commands = [
                ["-genkeypair", "-alias", "fixture", "-keyalg", "RSA", "-keysize", "2048", "-validity", "7", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12", "-keystore", str(key_store), "-storepass:env", "R2DBC_FIXTURE_STORE_PASSWORD", "-keypass:env", "R2DBC_FIXTURE_STORE_PASSWORD"],
                ["-exportcert", "-alias", "fixture", "-keystore", str(key_store), "-storepass:env", "R2DBC_FIXTURE_STORE_PASSWORD", "-file", str(certificate)],
                ["-importcert", "-noprompt", "-alias", "fixture", "-storetype", "PKCS12", "-keystore", str(trust_store), "-storepass:env", "R2DBC_FIXTURE_STORE_PASSWORD", "-file", str(certificate)],
            ]
            if client_auth:
                client_store, client_certificate, server_trust = private / "client.p12", private / "client.cer", private / "server-trust.p12"
                commands += [
                    ["-genkeypair", "-alias", "client", "-keyalg", "RSA", "-keysize", "2048", "-validity", "7", "-dname", "CN=r2dbc-fixture-client", "-ext", "EKU=clientAuth", "-storetype", "PKCS12", "-keystore", str(client_store), "-storepass:env", "R2DBC_FIXTURE_STORE_PASSWORD", "-keypass:env", "R2DBC_FIXTURE_STORE_PASSWORD"],
                    ["-exportcert", "-alias", "client", "-keystore", str(client_store), "-storepass:env", "R2DBC_FIXTURE_STORE_PASSWORD", "-file", str(client_certificate)],
                    ["-importcert", "-noprompt", "-alias", "client", "-storetype", "PKCS12", "-keystore", str(server_trust), "-storepass:env", "R2DBC_FIXTURE_STORE_PASSWORD", "-file", str(client_certificate)],
                ]
            with (private / "keytool.log").open("wb") as output:
                for command in commands:
                    subprocess.run([keytool] + command, env=tls_env, stdout=output, stderr=subprocess.STDOUT, check=True, timeout=60)
            credentials["trustStorePassword"] = tls_password
            write_properties(Path(state["credentials"]), credentials)
            state["trustStore"] = str(trust_store)
            tls_properties = {"key_store_path": key_store, "key_store_pwd": tls_password}
            if client_auth:
                state["keyStore"] = str(client_store)
                credentials["keyStorePassword"] = tls_password
                tls_properties.update({"trust_store_path": server_trust, "trust_store_pwd": tls_password})
                write_properties(Path(state["credentials"]), credentials)
        with (artifacts / "launcher-compile.log").open("wb") as output:
            subprocess.run([java_tool("javac"), "--release", "17", "-cp", str(lib / "*"), "-d", str(classes), str(SCRIPTS / "ServerLauncher.java"), str(SCRIPTS / "FixtureNodeControl.java")], stdout=output, stderr=subprocess.STDOUT, check=True, timeout=60)
        common = {
            "cluster_name": run.name, "cn_seed_config_node": f"127.0.0.1:{ports[0]}", "dn_seed_config_node": f"127.0.0.1:{ports[0]}",
            "cn_internal_address": "127.0.0.1", "dn_internal_address": "127.0.0.1", "dn_rpc_address": "127.0.0.1",
            "schema_replication_factor": 1, "data_replication_factor": 1,
            "config_node_consensus_protocol_class": "org.apache.iotdb.consensus.simple.SimpleConsensus",
            "schema_region_consensus_protocol_class": "org.apache.iotdb.consensus.simple.SimpleConsensus",
            "data_region_consensus_protocol_class": "org.apache.iotdb.consensus.simple.SimpleConsensus",
            # Node-loss forcibly kills a JVM after an acknowledged write. Require
            # synchronous WAL persistence before asserting crash recovery of that row.
            "wal_mode": "SYNC",
            "cn_rpc_thrift_compression_enable": "false", "dn_rpc_thrift_compression_enable": str(state["protocol"] == "compact").lower(), "dn_rpc_advanced_compression_enable": "false",
            "timestamp_precision": state["precision"], "enable_thrift_ssl": str(tls).lower(), "thrift_ssl_client_auth": str(client_auth).lower(),
            "enable_rest_service": "false", "enable_mqtt_service": "false", "cn_metric_reporter_list": "", "dn_metric_reporter_list": "", "dn_metric_internal_reporter_type": "MEMORY", "max_tsblock_line_number": 128,
        }
        common.update(tls_properties)
        descriptions = [("cn", "org.apache.iotdb.confignode.service.ConfigNode", {"cn_internal_port": ports[0], "cn_consensus_port": ports[1]}, ports[0])]
        for index in range(3 if cluster else 1):
            part = ports[2 + index * 5:7 + index * 5]
            descriptions.append((f"dn{index + 1}", "org.apache.iotdb.db.service.DataNode", dict(zip(["dn_rpc_port", "dn_internal_port", "dn_mpp_data_exchange_port", "dn_schema_region_consensus_port", "dn_data_region_consensus_port"], part)), part[0]))
        for name, main, properties, ready_port in descriptions:
            home = run / name
            home.mkdir()
            (home / "logs").mkdir()
            conf = private / name
            conf.mkdir()
            write_properties(conf / "iotdb-system.properties", dict(common, **properties))
            (conf / "fixture-logback.xml").write_text('<configuration><appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender"><encoder><pattern>%date %-5level [%thread] %logger{40} - %msg%n</pattern></encoder></appender><root level="WARN"><appender-ref ref="STDOUT"/></root></configuration>', encoding="utf-8")
            state["nodes"][name] = {"home": str(home), "conf": str(conf), "main": main, "listener": ready_port, "pid": None, "running": False}
        state["clusterPorts"] = [value["listener"] for key, value in state["nodes"].items() if key.startswith("dn")]
        save(state)
        for reservation in ports_reserved:
            reservation.close()
        for name in state["nodes"]:
            start_node(state, name)
        state["stage"] = "listening-authentication-pending"
        save(state)
        print("READY; RPC authentication is verified by run-tests", flush=True)
        return state
    except BaseException:
        for reservation in ports_reserved:
            reservation.close()
        state["stage"] = "start-failed"
        save(state)
        # Cleanup never guesses process names; each launched PID is verified.
        try:
            stop(state)
        except BaseException as cleanup_error:
            print("CLEANUP FAILED: " + type(cleanup_error).__name__ + "; original fixture startup failure preserved", file=sys.stderr)
        raise


def read_credentials(state):
    return dict(line.split("=", 1) for line in Path(state["credentials"]).read_text(encoding="utf-8").splitlines() if "=" in line)


def sanitize_report(source, destination, sensitive):
    tree = ET.parse(source)
    for element in tree.getroot().iter():
        for key, value in list(element.attrib.items()):
            for secret in sensitive:
                value = value.replace(secret, "[REDACTED]")
            if element.tag == "property" and element.attrib.get("name", "").lower().endswith(("password", "user", "username")) and key == "value":
                value = "[REDACTED]"
            element.set(key, value)
        if element.text:
            for secret in sensitive:
                element.text = element.text.replace(secret, "[REDACTED]")
    tree.write(destination, encoding="utf-8", xml_declaration=True)


def verify_live(reports, suite):
    cases = [case for report in reports for case in ET.parse(report).getroot().iter("testcase")]
    identities = [(case.attrib.get("classname", "").rsplit(".", 1)[-1], case.attrib.get("name")) for case in cases]
    if len(identities) != len(EXPECTED_CASES[suite]) or set(identities) != EXPECTED_CASES[suite]:
        raise RuntimeError("Live suite did not execute exactly its documented methods")
    if any(any(case.find(tag) is not None for tag in ("error", "failure", "skipped")) for case in cases):
        raise RuntimeError("Live suite had failed or skipped methods")
    return {"executed": len(cases), "passed": len(cases), "errors": 0, "skipped": 0}


def run_maven(command, output, timeout):
    process = subprocess.Popen(
        command, cwd=REPOSITORY, stdout=output, stderr=subprocess.STDOUT,
        start_new_session=os.name != "nt",
        creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
    )
    try:
        return process.wait(timeout=timeout)
    except BaseException:
        # A timed-out Maven process may still own a Surefire fork. Terminate its
        # dedicated process tree before scrubbing logs, otherwise a late child
        # could write credentials into already-scrubbed public evidence.
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20, check=False, creationflags=subprocess.CREATE_NO_WINDOW)
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        process.wait(timeout=20)
        raise


def run_tests(arguments, state):
    credentials = read_credentials(state)
    suite = arguments.suite
    base = ["-Diotdb.it.host=127.0.0.1", f"-Diotdb.it.port={state['rpcPort']}", f"-Diotdb.it.user={credentials['username']}", f"-Diotdb.it.password={credentials['password']}", "-Dsurefire.failIfNoSpecifiedTests=true"]
    module, profile = "iotdb-client/r2dbc", None
    if suite == "compatibility":
        tests = "IoTDBR2dbcCompatibilityIT"
        base += [f"-Diotdb.it.protocol={state['protocol']}", f"-Diotdb.it.precision={state['precision']}", f"-Diotdb.it.ssl={str(state['tls']).lower()}", f"-Diotdb.it.clientAuth={str(state['clientAuth']).lower()}", "-Diotdb.it.cluster.ports=" + ",".join(map(str, state["clusterPorts"]))]
        if state["tls"]:
            base += [f"-Diotdb.it.tls.trustStore={state['trustStore']}", f"-Diotdb.it.tls.trustStorePassword={credentials['trustStorePassword']}"]
        if state["clientAuth"]:
            base += [f"-Diotdb.it.tls.keyStore={state['keyStore']}", f"-Diotdb.it.tls.keyStorePassword={credentials['keyStorePassword']}"]
    elif suite == "soak":
        if state["mode"] != "plain" or state["protocol"] != "binary" or state["precision"] != "ms":
            raise ValueError("The full soak suite currently requires plain binary/ms")
        tests = "IoTDBR2dbcSoakIT#" + "+".join(sorted(method for _, method in EXPECTED_CASES["soak"]))
        base += [f"-Diotdb.it.stallProxy={SCRIPTS / 'stall_proxy.py'}", f"-Diotdb.it.python={sys.executable}"]
    elif suite == "long-soak":
        if state["mode"] != "plain" or state["protocol"] != "binary" or state["precision"] != "ms" or getattr(arguments, "soak_seconds", 0) <= 0:
            raise ValueError("Sustained soak requires plain binary/ms and --soak-seconds > 0")
        tests = "IoTDBR2dbcSoakIT#sustainedMixedWorkloadKeepsResourcesBounded"
        base += [f"-Diotdb.it.soak.seconds={arguments.soak_seconds}"]
    elif suite == "plain":
        tests = "IoTDBR2dbcIT,IoTDBR2dbcPoolIT,IoTDBR2dbcObjectIT"
    elif suite == "tls":
        if state["mode"] != "tls":
            raise ValueError("TLS suite requires a TLS fixture")
        tests = "IoTDBR2dbcTlsIT"
        base += [f"-Diotdb.it.tls.port={state['rpcPort']}", f"-Diotdb.it.tls.trustStore={state['trustStore']}", f"-Diotdb.it.tls.trustStorePassword={credentials['trustStorePassword']}"]
    elif suite in ("cluster", "node-loss"):
        if state["mode"] != "cluster":
            raise ValueError("Cluster suite requires a cluster fixture")
        base += ["-Diotdb.it.cluster.ports=" + ",".join(map(str, state["clusterPorts"])), f"-Diotdb.it.cluster.proxy={SCRIPTS / 'rtt_proxy.py'}", f"-Diotdb.it.cluster.python={sys.executable}"]
        tests = "IoTDBR2dbcClusterIT#dataWrittenThroughOneDataNodeIsReadableFromTheOthers+typedParameterQueryRunsFromAnyDataNode+writesOnEveryDataNodeCompleteWithNoRedirectBudget+brokenConnectionFailsFastAndIsNotReused" if suite == "cluster" else "IoTDBR2dbcClusterIT#stoppedDataNodeInvalidatesItsConnectionWhileAnotherNodeStaysHealthy"
        if suite == "node-loss":
            recovery = Path(state["runDirectory"]) / "artifacts/node-recovery.json"
            recovery.unlink(missing_ok=True)
            base += [f"-Diotdb.it.cluster.fixture={state['stateFile']}", f"-Diotdb.it.cluster.control={SCRIPTS / 'fixture.py'}", f"-Diotdb.it.cluster.recoveryEvidence={recovery}"]
    elif suite == "tck":
        tests, profile = "IoTDBR2dbcTckTest", "with-r2dbc-tck"
    elif suite == "boot":
        tests, profile = "IoTDBR2dbcSpringBootSampleIT", "with-r2dbc-spring"
    else:
        module, tests = "iotdb-client/r2dbc-spring-data", "IoTDBSpringDataRepositoryIT"
    maven = [maven_command(), "-B", "--no-transfer-progress", "-pl", module]
    if arguments.offline:
        maven.append("-o")
    if profile:
        maven += ["-P", profile]
    reports = REPOSITORY / module / "target/surefire-reports"
    # Selectors always refer to one suite; delete only its previous XML so stale
    # success cannot satisfy the current gate if Maven fails before executing tests.
    selected = tests.split("#")[0].split(",")
    for name in selected:
        for old in reports.glob(f"TEST-*.{name}.xml"):
            old.unlink()
    maven += [f"-Dtest={tests}"] + base + ["test"]
    artifacts = Path(state["runDirectory"]) / "artifacts"
    log = artifacts / (suite + ".log")
    print("RUNNING suite=" + suite, flush=True)
    sensitive = list(credentials.values())
    try:
        with log.open("wb") as output:
            maven_exit = run_maven(maven, output, arguments.timeout)
    finally:
        # Timeouts and subprocess failures must not leave an unsanitized public log.
        if log.exists():
            text = log.read_text(encoding="utf-8", errors="replace")
            for secret in sensitive:
                text = text.replace(secret, "[REDACTED]")
            log.write_text(text, encoding="utf-8")
        # Keep current failure XML too. Its raw system properties contain the
        # private credentials, so uploading module target reports is unsafe.
        public_reports = artifacts / suite
        public_reports.mkdir(exist_ok=True)
        for name in selected:
            for report in reports.glob(f"TEST-*.{name}.xml"):
                sanitize_report(report, public_reports / report.name, sensitive)
    actual_reports = []
    for name in selected:
        actual_reports += list(reports.glob(f"TEST-*.{name}.xml"))
    if len(actual_reports) != len(selected):
        raise RuntimeError("Current suite did not produce all expected reports; inspect its log")
    if suite == "tck":
        summary = verify_tck(actual_reports[0], REPOSITORY / "iotdb-client/r2dbc/src/test/java/org/apache/iotdb/r2dbc/IoTDBR2dbcTckTest.java")
        if maven_exit != 1:
            raise RuntimeError("TCK Maven exit code does not match documented capability differences")
    else:
        if maven_exit != 0:
            raise RuntimeError("Live suite failed; inspect its log")
        summary = verify_live(actual_reports, suite)
        if suite == "node-loss":
            proof = json.loads(recovery.read_text(encoding="utf-8"))
            if proof != {"rpcPort": state["clusterPorts"][2], "authenticated": True, "remoteValidated": True, "rejoinedRunning": True, "beforeValue": 101, "afterValue": 202}:
                raise RuntimeError("Node-recovery evidence did not match the asserted read/write contract")
            refreshed = read_state(Path(state["stateFile"]))
            refreshed["nodes"]["dn3"]["recovery"] = proof
            refreshed["stage"] = "recovery-verified"
            save(refreshed)
    (artifacts / (suite + "-summary.json")).write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print("PASSED suite=" + suite + " " + json.dumps(summary, sort_keys=True), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    actions = parser.add_subparsers(dest="action", required=True)
    start_parser = actions.add_parser("start")
    start_parser.add_argument("--distribution", type=Path, required=True)
    start_parser.add_argument("--run-root", type=Path, required=True)
    start_parser.add_argument("--mode", choices=["plain", "tls", "cluster", "mtls", "tls-cluster"], required=True)
    start_parser.add_argument("--protocol", choices=["binary", "compact"], default="binary")
    start_parser.add_argument("--precision", choices=["ms", "us", "ns"], default="ms")
    for action in ("stop", "kill-node", "restart-node", "run-tests"):
        command = actions.add_parser(action)
        command.add_argument("--state", type=Path, required=True)
        if action in ("kill-node", "restart-node"):
            command.add_argument("--node", choices=["dn1", "dn2", "dn3"], required=True)
        if action == "run-tests":
            command.add_argument("--suite", choices=list(EXPECTED_CASES) + ["tck"], required=True)
            command.add_argument("--soak-seconds", type=int, default=0)
            command.add_argument("--offline", action="store_true")
            command.add_argument("--timeout", type=int, default=900)
    arguments = parser.parse_args()
    if arguments.action == "start":
        start(arguments)
        return
    state = read_state(arguments.state)
    if arguments.action == "stop":
        stop(state)
    elif arguments.action == "kill-node":
        control(state, arguments.node)
        print("NODE_STOPPED=" + arguments.node, flush=True)
    elif arguments.action == "restart-node":
        if listener(state["nodes"][arguments.node]["listener"]):
            raise RuntimeError("Refusing to restart a listening node")
        start_node(state, arguments.node)
        state["stage"] = "restarted-listener-authentication-pending"
        state["nodes"][arguments.node].pop("recovery", None)
        save(state)
        print("NODE_LISTENING_RECOVERY_PENDING=" + arguments.node, flush=True)
    else:
        run_tests(arguments, state)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Avoid exception representations from subprocesses containing credentials.
        print("FAILED: " + type(error).__name__ + "; inspect this run's artifacts/state", file=sys.stderr)
        sys.exit(1)
