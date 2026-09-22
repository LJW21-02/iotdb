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

import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

public class FrameGuardSocketTest {

  private static final int MAX_FRAME_SIZE = 1024;

  @Test
  public void acceptsFramesWithinTheLimitAndStaysInSync() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      startServer(server, new byte[] {1, 2, 3}, new byte[] {9});
      FrameGuardSocket socket = connect(server.getLocalPort());
      try {
        ByteBuffer firstSize = ByteBuffer.allocate(4);
        readFully(socket, firstSize);
        assertEquals(3, frameSize(firstSize));
        ByteBuffer firstBody = ByteBuffer.allocate(3);
        readFully(socket, firstBody);
        assertEquals(3, firstBody.position());

        ByteBuffer secondSize = ByteBuffer.allocate(4);
        readFully(socket, secondSize);
        assertEquals(1, frameSize(secondSize));
        ByteBuffer secondBody = ByteBuffer.allocate(1);
        readFully(socket, secondBody);
        assertEquals(1, secondBody.position());
      } finally {
        socket.close();
      }
    }
  }

  @Test
  public void rejectsAnOversizedFrameBeforeThriftAllocatesIt() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      startRawServer(server, 64 * 1024 * 1024);
      FrameGuardSocket socket = connect(server.getLocalPort());
      try {
        assertThrows(TTransportException.class, () -> readFully(socket, ByteBuffer.allocate(4)));
      } finally {
        socket.close();
      }
    }
  }

  @Test
  public void rejectsInvalidFrameSizes() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      startRawServer(server, 0);
      FrameGuardSocket socket = connect(server.getLocalPort());
      try {
        assertThrows(TTransportException.class, () -> readFully(socket, ByteBuffer.allocate(4)));
      } finally {
        socket.close();
      }
    }
  }

  @Test
  public void rejectsNegativeFrameSizes() throws Exception {
    try (ServerSocket server = new ServerSocket(0)) {
      startRawServer(server, Integer.MIN_VALUE);
      FrameGuardSocket socket = connect(server.getLocalPort());
      try {
        assertThrows(TTransportException.class, () -> readFully(socket, ByteBuffer.allocate(4)));
      } finally {
        socket.close();
      }
    }
  }

  private static FrameGuardSocket connect(int port) throws Exception {
    FrameGuardSocket socket = new FrameGuardSocket("127.0.0.1", port, MAX_FRAME_SIZE);
    if (!socket.startConnect()) {
      long deadline = System.currentTimeMillis() + 5_000;
      while (!socket.finishConnect()) {
        if (System.currentTimeMillis() > deadline) {
          throw new AssertionError("could not connect to the test server");
        }
        Thread.sleep(5);
      }
    }
    return socket;
  }

  private static void readFully(FrameGuardSocket socket, ByteBuffer buffer) throws Exception {
    long deadline = System.currentTimeMillis() + 5_000;
    while (buffer.hasRemaining() && System.currentTimeMillis() < deadline) {
      int read = socket.read(buffer);
      if (read < 0) {
        throw new AssertionError("unexpected end of stream");
      }
      if (read == 0) {
        Thread.sleep(5);
      }
    }
    assertFalse("timed out while reading from the socket", buffer.hasRemaining());
  }

  private static int frameSize(ByteBuffer buffer) {
    return ((buffer.get(0) & 0xFF) << 24)
        | ((buffer.get(1) & 0xFF) << 16)
        | ((buffer.get(2) & 0xFF) << 8)
        | (buffer.get(3) & 0xFF);
  }

  private static void startServer(ServerSocket server, byte[]... frames) {
    Thread thread =
        new Thread(
            () -> {
              try (Socket socket = server.accept()) {
                OutputStream output = socket.getOutputStream();
                for (byte[] frame : frames) {
                  writeFrameSize(output, frame.length);
                  output.write(frame);
                }
                output.flush();
              } catch (IOException ignored) {
                // the test closes the socket when the assertion is done
              }
            });
    thread.setDaemon(true);
    thread.start();
  }

  private static void startRawServer(ServerSocket server, int rawFrameSize) {
    Thread thread =
        new Thread(
            () -> {
              try (Socket socket = server.accept()) {
                OutputStream output = socket.getOutputStream();
                writeFrameSize(output, rawFrameSize);
                output.flush();
              } catch (IOException ignored) {
                // the test closes the socket when the assertion is done
              }
            });
    thread.setDaemon(true);
    thread.start();
  }

  private static void writeFrameSize(OutputStream output, int size) throws IOException {
    output.write((size >>> 24) & 0xFF);
    output.write((size >>> 16) & 0xFF);
    output.write((size >>> 8) & 0xFF);
    output.write(size & 0xFF);
  }
}
