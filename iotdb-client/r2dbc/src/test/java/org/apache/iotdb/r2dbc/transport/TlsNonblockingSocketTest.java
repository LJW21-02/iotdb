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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TlsNonblockingSocketTest {

  private static final SSLContext DEFAULT_CONTEXT = defaultContext();

  @Test
  public void readsTheNextTcpFragmentAfterTlsUnderflow() throws Exception {
    CountDownLatch firstFragmentSent = new CountDownLatch(1);
    CountDownLatch releaseSecondFragment = new CountDownLatch(1);
    try (ServerSocket server = new ServerSocket(0);
        SocketChannel channel = SocketChannel.open()) {
      Thread peer =
          new Thread(
              () -> {
                try (Socket socket = server.accept()) {
                  socket.getOutputStream().write(1);
                  socket.getOutputStream().flush();
                  firstFragmentSent.countDown();
                  releaseSecondFragment.await();
                  socket.getOutputStream().write(new byte[] {2, 3});
                  socket.getOutputStream().flush();
                } catch (Exception ignored) {
                  // The test owns the client channel and may close it after an assertion.
                }
              });
      peer.setDaemon(true);
      peer.start();

      channel.configureBlocking(false);
      channel.connect(server.getLocalSocketAddress());
      while (!channel.finishConnect()) {
        Thread.yield();
      }
      assertTrue(firstFragmentSent.await(5, TimeUnit.SECONDS));

      TlsNonblockingSocket transport =
          new TlsNonblockingSocket(
              1024, new FragmentedHandshakeEngine(), channel, server.getLocalSocketAddress());
      ByteBuffer destination = ByteBuffer.allocate(1);
      assertEquals(0, transport.read(destination));

      releaseSecondFragment.countDown();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (destination.position() == 0 && System.nanoTime() < deadline) {
        transport.read(destination);
        if (destination.position() == 0) {
          Thread.yield();
        }
      }
      assertEquals(1, destination.position());
      assertEquals(7, destination.get(0));
      transport.close();
    }
  }

  @Test
  public void reportsTcpEofAndStopsTheTransport() throws Exception {
    try (ServerSocket server = new ServerSocket(0);
        SocketChannel channel = SocketChannel.open()) {
      Thread peer =
          new Thread(
              () -> {
                try (Socket ignored = server.accept()) {
                  // Closing immediately produces a TCP FIN without a TLS close_notify.
                } catch (IOException ignored) {
                  // The test owns the client channel.
                }
              });
      peer.setDaemon(true);
      peer.start();

      channel.configureBlocking(false);
      channel.connect(server.getLocalSocketAddress());
      while (!channel.finishConnect()) {
        Thread.yield();
      }
      TlsNonblockingSocket transport =
          new TlsNonblockingSocket(1024, DEFAULT_CONTEXT.createSSLEngine(), channel, null);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (System.nanoTime() < deadline) {
        try {
          transport.read(ByteBuffer.allocate(1));
        } catch (TTransportException e) {
          assertEquals(TTransportException.END_OF_FILE, e.getType());
          assertFalse(transport.isOpen());
          return;
        }
        Thread.yield();
      }
      fail("expected EOF");
    }
  }

  @Test
  public void readsTheNextTcpFragmentAfterApplicationDataUnderflow() throws Exception {
    CountDownLatch firstFragmentSent = new CountDownLatch(1);
    CountDownLatch releaseSecondFragment = new CountDownLatch(1);
    try (ServerSocket server = new ServerSocket(0);
        SocketChannel channel = SocketChannel.open()) {
      Thread peer =
          new Thread(
              () -> {
                try (Socket socket = server.accept()) {
                  socket.getInputStream().read();
                  socket.getOutputStream().write(1);
                  socket.getOutputStream().flush();
                  firstFragmentSent.countDown();
                  releaseSecondFragment.await();
                  socket.getOutputStream().write(2);
                  socket.getOutputStream().flush();
                } catch (Exception ignored) {
                  // The test owns the client channel and may close it after an assertion.
                }
              });
      peer.setDaemon(true);
      peer.start();

      channel.configureBlocking(false);
      channel.connect(server.getLocalSocketAddress());
      while (!channel.finishConnect()) {
        Thread.yield();
      }
      TlsNonblockingSocket transport =
          new TlsNonblockingSocket(1024, new ApplicationDataEngine(), channel, null);
      ByteBuffer destination = ByteBuffer.allocate(1);
      assertEquals(0, transport.read(destination));
      assertTrue(firstFragmentSent.await(5, TimeUnit.SECONDS));
      assertEquals(0, transport.read(destination));

      releaseSecondFragment.countDown();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (destination.position() == 0 && System.nanoTime() < deadline) {
        transport.read(destination);
        if (destination.position() == 0) {
          Thread.yield();
        }
      }
      assertEquals(1, destination.position());
      assertEquals(7, destination.get(0));
      transport.close();
    }
  }

  @Test
  public void doesNotRetryAnUnwrapThatMakesNoProgress() throws Exception {
    CountDownLatch ciphertextSent = new CountDownLatch(1);
    try (ServerSocket server = new ServerSocket(0);
        SocketChannel channel = SocketChannel.open()) {
      Thread peer =
          new Thread(
              () -> {
                try (Socket socket = server.accept()) {
                  socket.getInputStream().read();
                  socket.getOutputStream().write(new byte[] {1, 2});
                  socket.getOutputStream().flush();
                  ciphertextSent.countDown();
                } catch (IOException ignored) {
                  // The test owns the client channel.
                }
              });
      peer.setDaemon(true);
      peer.start();

      channel.configureBlocking(false);
      channel.connect(server.getLocalSocketAddress());
      while (!channel.finishConnect()) {
        Thread.yield();
      }
      ApplicationDataEngine engine = new ApplicationDataEngine(true);
      TlsNonblockingSocket transport = new TlsNonblockingSocket(1024, engine, channel, null);
      assertEquals(0, transport.read(ByteBuffer.allocate(1)));
      assertTrue(ciphertextSent.await(5, TimeUnit.SECONDS));
      if (engine.unwrapCalls == 0) {
        assertEquals(0, transport.read(ByteBuffer.allocate(1)));
      }
      assertEquals(1, engine.unwrapCalls);
      transport.close();
    }
  }

  @Test
  public void reportsTcpEofAfterHandshakeCompletes() throws Exception {
    CountDownLatch handshakeReceived = new CountDownLatch(1);
    CountDownLatch closePeer = new CountDownLatch(1);
    try (ServerSocket server = new ServerSocket(0);
        SocketChannel channel = SocketChannel.open()) {
      Thread peer =
          new Thread(
              () -> {
                try (Socket socket = server.accept()) {
                  socket.getInputStream().read();
                  handshakeReceived.countDown();
                  closePeer.await();
                } catch (Exception ignored) {
                  // The test owns the client channel.
                }
              });
      peer.setDaemon(true);
      peer.start();

      channel.configureBlocking(false);
      channel.connect(server.getLocalSocketAddress());
      while (!channel.finishConnect()) {
        Thread.yield();
      }
      TlsNonblockingSocket transport =
          new TlsNonblockingSocket(1024, new ApplicationDataEngine(), channel, null);
      assertEquals(0, transport.read(ByteBuffer.allocate(1)));
      assertTrue(handshakeReceived.await(5, TimeUnit.SECONDS));
      closePeer.countDown();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (System.nanoTime() < deadline) {
        try {
          transport.read(ByteBuffer.allocate(1));
        } catch (TTransportException e) {
          assertEquals(TTransportException.END_OF_FILE, e.getType());
          assertFalse(transport.isOpen());
          return;
        }
        Thread.yield();
      }
      fail("expected EOF after the handshake completed");
    }
  }

  private static SSLContext defaultContext() {
    try {
      return SSLContext.getDefault();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final class FragmentedHandshakeEngine extends SSLEngine {
    private final SSLEngine delegate = DEFAULT_CONTEXT.createSSLEngine();
    private boolean underflowReturned;
    private boolean handshakeComplete;

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst)
        throws SSLException {
      return new SSLEngineResult(
          SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 0, 0);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length)
        throws SSLException {
      if (!handshakeComplete) {
        if (!underflowReturned) {
          underflowReturned = true;
          return new SSLEngineResult(
              SSLEngineResult.Status.BUFFER_UNDERFLOW,
              SSLEngineResult.HandshakeStatus.NEED_UNWRAP,
              0,
              0);
        }
        if (!src.hasRemaining()) {
          return new SSLEngineResult(
              SSLEngineResult.Status.BUFFER_UNDERFLOW,
              SSLEngineResult.HandshakeStatus.NEED_UNWRAP,
              0,
              0);
        }
        src.get();
        handshakeComplete = true;
        return new SSLEngineResult(
            SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 1, 0);
      }
      if (!src.hasRemaining() || !dsts[offset].hasRemaining()) {
        return new SSLEngineResult(
            SSLEngineResult.Status.BUFFER_UNDERFLOW,
            SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
            0,
            0);
      }
      src.get();
      dsts[offset].put((byte) 7);
      return new SSLEngineResult(
          SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 1, 1);
    }

    @Override
    public Runnable getDelegatedTask() {
      return null;
    }

    @Override
    public void closeInbound() {}

    @Override
    public boolean isInboundDone() {
      return false;
    }

    @Override
    public void closeOutbound() {}

    @Override
    public boolean isOutboundDone() {
      return false;
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegate.getSupportedCipherSuites();
    }

    @Override
    public String[] getEnabledCipherSuites() {
      return delegate.getEnabledCipherSuites();
    }

    @Override
    public void setEnabledCipherSuites(String[] suites) {
      delegate.setEnabledCipherSuites(suites);
    }

    @Override
    public String[] getSupportedProtocols() {
      return delegate.getSupportedProtocols();
    }

    @Override
    public String[] getEnabledProtocols() {
      return delegate.getEnabledProtocols();
    }

    @Override
    public void setEnabledProtocols(String[] protocols) {
      delegate.setEnabledProtocols(protocols);
    }

    @Override
    public SSLSession getSession() {
      return delegate.getSession();
    }

    @Override
    public void beginHandshake() {}

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
      return handshakeComplete
          ? SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
          : SSLEngineResult.HandshakeStatus.NEED_UNWRAP;
    }

    @Override
    public void setUseClientMode(boolean mode) {
      delegate.setUseClientMode(mode);
    }

    @Override
    public boolean getUseClientMode() {
      return delegate.getUseClientMode();
    }

    @Override
    public void setNeedClientAuth(boolean need) {
      delegate.setNeedClientAuth(need);
    }

    @Override
    public boolean getNeedClientAuth() {
      return delegate.getNeedClientAuth();
    }

    @Override
    public void setWantClientAuth(boolean want) {
      delegate.setWantClientAuth(want);
    }

    @Override
    public boolean getWantClientAuth() {
      return delegate.getWantClientAuth();
    }

    @Override
    public void setEnableSessionCreation(boolean flag) {
      delegate.setEnableSessionCreation(flag);
    }

    @Override
    public boolean getEnableSessionCreation() {
      return delegate.getEnableSessionCreation();
    }
  }

  private static final class ApplicationDataEngine extends SSLEngine {
    private final SSLEngine delegate = DEFAULT_CONTEXT.createSSLEngine();
    private final boolean noProgressOnce;
    private boolean handshakeComplete;
    private int unwrapCalls;

    private ApplicationDataEngine() {
      this(false);
    }

    private ApplicationDataEngine(boolean noProgressOnce) {
      this.noProgressOnce = noProgressOnce;
    }

    @Override
    public SSLEngineResult wrap(ByteBuffer[] srcs, int offset, int length, ByteBuffer dst)
        throws SSLException {
      if (!handshakeComplete) {
        dst.put((byte) 0);
        handshakeComplete = true;
        return new SSLEngineResult(
            SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 0, 1);
      }
      return new SSLEngineResult(
          SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 0, 0);
    }

    @Override
    public SSLEngineResult unwrap(ByteBuffer src, ByteBuffer[] dsts, int offset, int length)
        throws SSLException {
      unwrapCalls++;
      if (noProgressOnce && unwrapCalls <= 2 && src.remaining() >= 2) {
        return new SSLEngineResult(
            SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 0, 0);
      }
      if (src.remaining() < 2) {
        return new SSLEngineResult(
            SSLEngineResult.Status.BUFFER_UNDERFLOW,
            SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING,
            0,
            0);
      }
      src.position(src.limit());
      dsts[offset].put((byte) 7);
      return new SSLEngineResult(
          SSLEngineResult.Status.OK, SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING, 2, 1);
    }

    @Override
    public Runnable getDelegatedTask() {
      return null;
    }

    @Override
    public void closeInbound() {}

    @Override
    public boolean isInboundDone() {
      return false;
    }

    @Override
    public void closeOutbound() {}

    @Override
    public boolean isOutboundDone() {
      return false;
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return delegate.getSupportedCipherSuites();
    }

    @Override
    public String[] getEnabledCipherSuites() {
      return delegate.getEnabledCipherSuites();
    }

    @Override
    public void setEnabledCipherSuites(String[] suites) {
      delegate.setEnabledCipherSuites(suites);
    }

    @Override
    public String[] getSupportedProtocols() {
      return delegate.getSupportedProtocols();
    }

    @Override
    public String[] getEnabledProtocols() {
      return delegate.getEnabledProtocols();
    }

    @Override
    public void setEnabledProtocols(String[] protocols) {
      delegate.setEnabledProtocols(protocols);
    }

    @Override
    public SSLSession getSession() {
      return delegate.getSession();
    }

    @Override
    public void beginHandshake() {}

    @Override
    public SSLEngineResult.HandshakeStatus getHandshakeStatus() {
      return handshakeComplete
          ? SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING
          : SSLEngineResult.HandshakeStatus.NEED_WRAP;
    }

    @Override
    public void setUseClientMode(boolean mode) {
      delegate.setUseClientMode(mode);
    }

    @Override
    public boolean getUseClientMode() {
      return delegate.getUseClientMode();
    }

    @Override
    public void setNeedClientAuth(boolean need) {
      delegate.setNeedClientAuth(need);
    }

    @Override
    public boolean getNeedClientAuth() {
      return delegate.getNeedClientAuth();
    }

    @Override
    public void setWantClientAuth(boolean want) {
      delegate.setWantClientAuth(want);
    }

    @Override
    public boolean getWantClientAuth() {
      return delegate.getWantClientAuth();
    }

    @Override
    public void setEnableSessionCreation(boolean flag) {
      delegate.setEnableSessionCreation(flag);
    }

    @Override
    public boolean getEnableSessionCreation() {
      return delegate.getEnableSessionCreation();
    }
  }
}
