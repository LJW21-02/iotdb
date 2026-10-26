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

"""Run one reproducible R2DBC CI job and always release its isolated nodes."""

import argparse
import sys
from pathlib import Path

from fixture import read_state, run_tests, start, stop


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--distribution", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--mode", choices=["plain", "tls", "cluster"], required=True)
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--timeout", type=int, default=900)
    arguments = parser.parse_args()
    state = start(arguments)
    suites = {"plain": ["plain", "tck", "boot", "spring-data"], "tls": ["tls"], "cluster": ["cluster", "node-loss"]}
    try:
        for suite in suites[arguments.mode]:
            arguments.suite = suite
            run_tests(arguments, read_state(Path(state["stateFile"])))
    finally:
        # Node-loss updates the PID on restart; reload instead of keeping its old PID.
        stop(read_state(Path(state["stateFile"])))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # A TimeoutExpired representation includes Maven arguments, which carry
        # private fixture credentials. Logs have already been scrubbed in finally.
        print("FAILED: " + type(error).__name__ + "; inspect run artifacts", file=sys.stderr)
        sys.exit(1)
