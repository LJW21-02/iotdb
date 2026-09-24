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

import org.apache.iotdb.r2dbc.R2dbcMessages;

import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;

/**
 * Validates the length prefix of every response frame before Thrift allocates a buffer for it.
 *
 * <p>Thrift's async machinery allocates the declared frame length without checking it, so a bogus
 * or malicious length can exhaust the heap and kill the shared selector thread. The transport reads
 * the four byte prefix itself and fails the call with a controlled {@link TTransportException}
 * before any allocation happens. Both the plaintext and the TLS transport feed their plaintext
 * reads through this guard.
 */
final class FrameSizeGuard {

  private static final int FRAME_SIZE_BYTES = 4;

  private volatile int maxFrameSize;
  private boolean readingFrameSize = true;
  private int remainingFrameBytes;

  FrameSizeGuard(int maxFrameSize) {
    this.maxFrameSize = maxFrameSize;
  }

  /** Applies the stricter of the configured and the server announced frame limit. */
  void limitFrameSize(int limit) {
    if (limit > 0 && limit < maxFrameSize) {
      maxFrameSize = limit;
    }
  }

  /**
   * Updates the guard with the bytes that were copied into {@code buffer}.
   *
   * <p>Thrift reads the four byte length prefix first and the frame body afterwards; the guard
   * mirrors that sequence per response.
   */
  void afterRead(ByteBuffer buffer, int read) throws TTransportException {
    if (read <= 0) {
      return;
    }
    if (readingFrameSize) {
      if (buffer.remaining() == 0 && buffer.position() == FRAME_SIZE_BYTES) {
        int frameSize = frameSize(buffer);
        if (frameSize <= 0 || frameSize > maxFrameSize) {
          throw new TTransportException(
              frameSize <= 0
                  ? String.format(
                      R2dbcMessages.EXCEPTION_RECEIVED_AN_INVALID_FRAME_SIZE_ARG_280DE971,
                      frameSize)
                  : String.format(
                      R2dbcMessages
                          .EXCEPTION_RECEIVED_A_FRAME_OF_ARG_BYTES_WHICH_EXCEEDS_THE_CONFIGURED_LIMIT_OF_ARG_BYTES_FEBB2338,
                      frameSize,
                      maxFrameSize));
        }
        remainingFrameBytes = frameSize;
        readingFrameSize = false;
      }
      return;
    }
    remainingFrameBytes -= read;
    if (remainingFrameBytes <= 0) {
      readingFrameSize = true;
    }
  }

  private static int frameSize(ByteBuffer buffer) {
    return ((buffer.get(0) & 0xFF) << 24)
        | ((buffer.get(1) & 0xFF) << 16)
        | ((buffer.get(2) & 0xFF) << 8)
        | (buffer.get(3) & 0xFF);
  }
}
