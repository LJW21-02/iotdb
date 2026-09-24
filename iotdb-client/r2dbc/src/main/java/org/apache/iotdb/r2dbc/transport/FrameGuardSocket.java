/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.r2dbc.transport;

import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;

/** A plaintext non-blocking Thrift socket guarded by {@link FrameSizeGuard}. */
final class FrameGuardSocket extends TNonblockingSocket implements FrameLimitedTransport {

  private final FrameSizeGuard guard;

  FrameGuardSocket(String host, int port, int maxFrameSize)
      throws IOException, TTransportException {
    super(host, port, 0);
    this.guard = new FrameSizeGuard(maxFrameSize);
  }

  /** Applies the stricter of the configured and the server announced frame limit. */
  public void limitFrameSize(int limit) {
    guard.limitFrameSize(limit);
  }

  @Override
  public int read(ByteBuffer buffer) throws TTransportException {
    int read = super.read(buffer);
    guard.afterRead(buffer, read);
    return read;
  }
}
