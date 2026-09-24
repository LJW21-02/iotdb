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

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

/**
 * A TLS transport for Thrift's async client.
 *
 * <p>Thrift ships a non-blocking SSL socket, but its handshake busy waits on the shared selector
 * thread and its read contract does not match what the async machinery expects. This transport
 * drives an {@link SSLEngine} explicitly instead:
 *
 * <ul>
 *   <li>the TLS handshake runs from {@code finishConnect()} and only ever advances as far as the
 *       socket allows, registering read/write interest through the selection key it was given;
 *   <li>plaintext is handed out from decrypted buffers and every read passes through {@link
 *       FrameSizeGuard}, so a bogus frame length cannot allocate memory;
 *   <li>plaintext is only acknowledged to the caller once the matching ciphertext reached the
 *       socket, so a full send buffer results in a partial write instead of a lost record.
 * </ul>
 *
 * <p>The engine is configured for client mode with the HTTPS endpoint identification algorithm, so
 * both the certificate chain and the host name are verified during the handshake. TLS failures are
 * reported as transport errors; the connection is never retried in plaintext.
 */
final class TlsNonblockingSocket extends TNonblockingTransport implements FrameLimitedTransport {

  private static final int MAX_TLS_PLAINTEXT_RECORD = 16 * 1024;

  private final SocketChannel socketChannel;
  private final SocketAddress remoteAddress;
  private final SSLEngine engine;
  private final FrameSizeGuard guard;
  private final ByteBuffer netIn;
  private final ByteBuffer appIn;
  private ByteBuffer netOut;
  private final ByteBuffer appOut;

  private boolean connected;
  private boolean handshakeComplete;
  private SelectionKey selectionKey;
  private int encryptedButUnacknowledged;

  TlsNonblockingSocket(
      int maxFrameSize, SSLEngine engine, SocketChannel socketChannel, SocketAddress remoteAddress)
      throws TTransportException {
    super(new TConfiguration());
    this.socketChannel = socketChannel;
    this.remoteAddress = remoteAddress;
    this.engine = engine;
    this.guard = new FrameSizeGuard(maxFrameSize);
    int packetSize = engine.getSession().getPacketBufferSize();
    int applicationSize = engine.getSession().getApplicationBufferSize();
    this.netIn = ByteBuffer.allocate(packetSize + MAX_TLS_PLAINTEXT_RECORD);
    this.appIn = ByteBuffer.allocate(applicationSize + 1024);
    this.netOut = ByteBuffer.allocate(packetSize * 2);
    this.appOut = ByteBuffer.allocate(applicationSize);
    // Both buffers must start empty in read mode: a fresh buffer is in write mode and would look
    // like unread data to the unwrap loop.
    this.netIn.flip();
    this.appIn.flip();
  }

  /** Applies the stricter of the configured and the server announced frame limit. */
  public void limitFrameSize(int limit) {
    guard.limitFrameSize(limit);
  }

  @Override
  public boolean startConnect() throws IOException {
    if (!connected) {
      connected = socketChannel.connect(remoteAddress);
    }
    if (connected) {
      engine.beginHandshake();
    }
    // Only the TCP connect may be awaited here: Thrift's async client treats a second OP_CONNECT
    // with a false finishConnect() as an error, so the TLS handshake is driven from read/write.
    return connected;
  }

  @Override
  public boolean finishConnect() throws IOException {
    if (!connected) {
      if (!socketChannel.finishConnect()) {
        return false;
      }
      connected = true;
      engine.beginHandshake();
    }
    return true;
  }

  /** Advances the handshake as far as the socket allows without blocking. */
  private void driveHandshake() throws IOException {
    while (!handshakeComplete) {
      HandshakeStatus status = engine.getHandshakeStatus();
      switch (status) {
        case NEED_UNWRAP:
          if (!netIn.hasRemaining() && readCiphertext() <= 0) {
            interestOps(SelectionKey.OP_READ);
            return;
          }
          SSLEngineResult unwrap = engine.unwrap(netIn, appIn);
          if (unwrap.getStatus() == Status.BUFFER_UNDERFLOW) {
            interestOps(SelectionKey.OP_READ);
            return;
          }
          requireOk(unwrap, "handshake");
          break;
        case NEED_WRAP:
          appOut.clear();
          appOut.flip();
          int produced = wrap(appOut);
          flushNetOutIfWritable();
          if (netOut.position() > 0) {
            interestOps(SelectionKey.OP_WRITE);
            return;
          }
          if (produced == 0) {
            throw new IOException("TLS handshake made no progress while wrapping");
          }
          break;
        case NEED_TASK:
          runDelegatedTasks();
          break;
        case FINISHED:
        case NOT_HANDSHAKING:
          handshakeComplete = true;
          interestOps(SelectionKey.OP_READ);
          break;
        default:
          throw new IOException("unsupported TLS handshake status " + status);
      }
    }
  }

  private void runDelegatedTasks() {
    Runnable task;
    while ((task = engine.getDelegatedTask()) != null) {
      task.run();
    }
  }

  private void requireOk(SSLEngineResult result, String phase) throws SSLException {
    if (result.getStatus() == Status.CLOSED) {
      throw new SSLException("TLS connection closed during " + phase);
    }
    if (result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
      runDelegatedTasks();
    }
  }

  private void interestOps(int ops) {
    SelectionKey key = selectionKey;
    if (key != null && key.isValid()) {
      key.interestOps(ops);
    }
  }

  @Override
  public SelectionKey registerSelector(Selector selector, int ops) throws IOException {
    this.selectionKey = socketChannel.register(selector, ops);
    return selectionKey;
  }

  @Override
  public boolean isOpen() {
    // Same contract as the plaintext socket: open once TCP is connected. The TLS handshake is
    // driven lazily by read/write and its failure is reported as a transport error.
    return socketChannel.isOpen() && socketChannel.isConnected();
  }

  @Override
  public void open() throws TTransportException {
    throw new TTransportException("open() is not implemented for TlsNonblockingSocket");
  }

  @Override
  public void close() {
    try {
      engine.closeOutbound();
      appOut.clear();
      appOut.flip();
      wrap(appOut);
      flushNetOutIfWritable();
    } catch (Exception ignored) {
      // best effort close_notify; the socket is closed below in any case
    }
    try {
      socketChannel.close();
    } catch (IOException ignored) {
      // already closed
    }
  }

  @Override
  public int read(ByteBuffer destination) throws TTransportException {
    if (!handshakeComplete) {
      try {
        driveHandshake();
      } catch (IOException e) {
        throw new TTransportException(TTransportException.UNKNOWN, e.getMessage(), e);
      }
      if (!handshakeComplete) {
        return 0;
      }
    }
    flushNetOutIfWritable();
    if (!appIn.hasRemaining()) {
      fillPlaintext();
    }
    if (!appIn.hasRemaining()) {
      interestOps(SelectionKey.OP_READ);
      return 0;
    }
    int read = copy(appIn, destination);
    guard.afterRead(destination, read);
    // Thrift reads the frame size and the frame body in two transitions, so buffered plaintext must
    // produce another transition even when the kernel buffer is empty. A writable socket is always
    // ready and the state machine dispatches reads by state, which gives exactly one more call; it
    // is cleared as soon as the buffer is drained.
    interestOps(
        appIn.hasRemaining() ? SelectionKey.OP_READ | SelectionKey.OP_WRITE : SelectionKey.OP_READ);
    return read;
  }

  /** Reads ciphertext and decrypts it until plaintext is available or the socket has no more. */
  private void fillPlaintext() throws TTransportException {
    while (!appIn.hasRemaining()) {
      try {
        if (!netIn.hasRemaining() && readCiphertext() <= 0) {
          return;
        }
        appIn.clear();
        SSLEngineResult result = engine.unwrap(netIn, appIn);
        appIn.flip();
        requireOk(result, "unwrap");
        if (result.getStatus() == Status.BUFFER_UNDERFLOW) {
          // The record is incomplete; compact and wait for the rest.
          return;
        }
        if (!appIn.hasRemaining() && !netIn.hasRemaining()) {
          continue;
        }
        return;
      } catch (SSLException e) {
        throw new TTransportException(TTransportException.CORRUPTED_DATA, e.getMessage(), e);
      } catch (IOException e) {
        throw new TTransportException(TTransportException.UNKNOWN, e.getMessage(), e);
      }
    }
  }

  /** Reads from the channel into the ciphertext buffer; returns the channel read count. */
  private int readCiphertext() throws IOException {
    netIn.compact();
    int read = socketChannel.read(netIn);
    netIn.flip();
    return read;
  }

  @Override
  public int write(ByteBuffer source) throws TTransportException {
    if (!handshakeComplete) {
      // Report a partial write while the handshake needs more I/O; the interest ops chosen by the
      // handshake tell the selector what to wait for.
      try {
        driveHandshake();
      } catch (IOException e) {
        throw new TTransportException(TTransportException.UNKNOWN, e.getMessage(), e);
      }
      if (!handshakeComplete) {
        return 0;
      }
    }
    flushNetOutIfWritable();
    int acknowledged = 0;
    if (encryptedButUnacknowledged > 0) {
      if (netOut.position() > 0) {
        // Still draining previously encrypted data; ask to be called again.
        interestOps(SelectionKey.OP_WRITE);
        return 0;
      }
      if (encryptedButUnacknowledged > source.remaining()) {
        throw new TTransportException(
            TTransportException.CORRUPTED_DATA, "write buffer replaced while data was pending");
      }
      int settled = encryptedButUnacknowledged;
      source.position(source.position() + settled);
      encryptedButUnacknowledged = 0;
      acknowledged += settled;
    }
    if (!source.hasRemaining()) {
      return acknowledged;
    }
    if (netOut.position() > 0) {
      interestOps(SelectionKey.OP_WRITE);
      return acknowledged;
    }
    try {
      int chunk = Math.min(source.remaining(), MAX_TLS_PLAINTEXT_RECORD);
      ByteBuffer view = source.duplicate();
      view.limit(view.position() + chunk);
      appOut.clear();
      appOut.put(view);
      appOut.flip();
      wrap(appOut);
      flushNetOutIfWritable();
      if (netOut.position() > 0) {
        // The ciphertext could not be flushed completely: do not acknowledge the plaintext yet.
        encryptedButUnacknowledged = chunk;
        interestOps(SelectionKey.OP_WRITE);
        return acknowledged;
      }
      source.position(source.position() + chunk);
      return acknowledged + chunk;
    } catch (SSLException e) {
      throw new TTransportException(TTransportException.UNKNOWN, e.getMessage(), e);
    } catch (IOException e) {
      throw new TTransportException(TTransportException.UNKNOWN, e.getMessage(), e);
    }
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws TTransportException {
    return read(ByteBuffer.wrap(buffer, offset, length));
  }

  @Override
  public void write(byte[] buffer, int offset, int length) throws TTransportException {
    ByteBuffer source = ByteBuffer.wrap(buffer, offset, length);
    while (source.hasRemaining()) {
      write(source);
    }
  }

  @Override
  public void flush() throws TTransportException {
    flushNetOutIfWritable();
  }

  /**
   * Wraps plaintext from {@code plain} into the ciphertext buffer, growing it when needed.
   *
   * <p>The engine must be called at least once even for an empty source, because handshake records
   * such as the ClientHello have no application data; the loop therefore runs once before it
   * depends on remaining plaintext.
   */
  private int wrap(ByteBuffer plain) throws IOException {
    int produced = 0;
    boolean first = true;
    while (first || plain.hasRemaining()) {
      first = false;
      SSLEngineResult result = engine.wrap(plain, netOut);
      produced += result.bytesProduced();
      if (result.getStatus() == Status.BUFFER_OVERFLOW) {
        ensureNetOutCapacity(engine.getSession().getPacketBufferSize());
        continue;
      }
      requireOk(result, "wrap");
      if (result.bytesConsumed() == 0 && result.bytesProduced() == 0) {
        break;
      }
    }
    return produced;
  }

  private void ensureNetOutCapacity(int needed) {
    if (netOut.remaining() >= needed) {
      return;
    }
    ByteBuffer grown =
        ByteBuffer.allocate(Math.max(netOut.capacity() * 2, netOut.position() + needed));
    ByteBuffer pending = netOut.duplicate();
    pending.flip();
    grown.put(pending);
    netOut = grown;
  }

  /** Writes as much buffered ciphertext as the socket accepts. */
  private void flushNetOutIfWritable() {
    netOut.flip();
    try {
      while (netOut.hasRemaining()) {
        if (socketChannel.write(netOut) == 0) {
          break;
        }
      }
    } catch (IOException ignored) {
      // Reported by the next read or write; the async client turns it into onError.
    } finally {
      netOut.compact();
    }
  }

  private static int copy(ByteBuffer source, ByteBuffer destination) {
    int length = Math.min(source.remaining(), destination.remaining());
    ByteBuffer view = source.duplicate();
    view.limit(view.position() + length);
    destination.put(view);
    source.position(source.position() + length);
    return length;
  }
}
