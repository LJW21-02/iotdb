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

"""Loopback TCP proxy: a flag pauses forwarding while keeping both sockets open."""

import argparse
import socket
import threading
import time
from pathlib import Path


def pipe(source, destination, flag, closed):
    source.settimeout(0.1)
    try:
        while not closed.is_set():
            if flag.exists():
                time.sleep(0.05)
                continue
            try:
                data = source.recv(65536)
            except socket.timeout:
                continue
            if not data:
                return
            while flag.exists() and not closed.is_set():
                time.sleep(0.05)
            if not closed.is_set():
                destination.sendall(data)
    except OSError:
        pass
    finally:
        closed.set()
        for endpoint in (source, destination):
            try:
                endpoint.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            endpoint.close()


def handle(client, target_port, flag):
    try:
        upstream = socket.create_connection(("127.0.0.1", target_port), timeout=5)
    except OSError:
        client.close()
        return
    closed = threading.Event()
    for source, destination in ((client, upstream), (upstream, client)):
        threading.Thread(target=pipe, args=(source, destination, flag, closed), daemon=True).start()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--target-port", type=int, required=True)
    parser.add_argument("--stall-file", type=Path, required=True)
    arguments = parser.parse_args()
    with socket.socket() as server:
        server.bind(("127.0.0.1", arguments.listen_port))
        server.listen(32)
        print("STALL_PROXY_READY", flush=True)
        while True:
            client, _ = server.accept()
            threading.Thread(target=handle, args=(client, arguments.target_port, arguments.stall_file), daemon=True).start()


if __name__ == "__main__":
    main()
