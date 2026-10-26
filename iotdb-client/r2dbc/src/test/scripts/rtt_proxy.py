# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""TCP level delay proxy used for the RTT comparison.

Delays every received TCP chunk by the configured amount in both directions, so it works for
plaintext and TLS without parsing the protocol. This injects transport level latency, unlike an
application level "delay per response frame" proxy.
"""
import argparse
import socket
import threading
import time


def pipe(source, destination, delay_seconds):
    try:
        while True:
            data = source.recv(65536)
            if not data:
                break
            if delay_seconds:
                time.sleep(delay_seconds)
            destination.sendall(data)
    except OSError:
        pass
    finally:
        for sock in (source, destination):
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass


def handle(client, target_host, target_port, delay_seconds):
    upstream = socket.create_connection((target_host, target_port))
    client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    threading.Thread(target=pipe, args=(client, upstream, delay_seconds), daemon=True).start()
    threading.Thread(target=pipe, args=(upstream, client, delay_seconds), daemon=True).start()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--target-port", type=int, required=True)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--delay-ms", type=float, default=10.0)
    args = parser.parse_args()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", args.listen_port))
    server.listen(64)
    print(
        "proxy listening on %d -> %s:%d, delay=%.1fms per chunk"
        % (args.listen_port, args.target_host, args.target_port, args.delay_ms),
        flush=True,
    )
    while True:
        client, _ = server.accept()
        threading.Thread(
            target=handle,
            args=(client, args.target_host, args.target_port, args.delay_ms / 1000.0),
            daemon=True,
        ).start()


if __name__ == "__main__":
    main()
