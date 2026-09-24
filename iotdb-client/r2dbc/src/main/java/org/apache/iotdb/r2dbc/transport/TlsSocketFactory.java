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

import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration;
import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration.TlsConfiguration;
import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

/** Builds a TLS transport from the connection configuration. */
final class TlsSocketFactory {

  private static final String PKCS12 = "PKCS12";
  private static final String JKS = "JKS";

  private TlsSocketFactory() {}

  static TlsNonblockingSocket create(
      IoTDBConnectionConfiguration configuration, String host, int port) {
    TlsConfiguration tls = configuration.tls();
    SSLEngine engine = createContext(tls).createSSLEngine(host, port);
    engine.setUseClientMode(true);
    SSLParameters parameters = engine.getSSLParameters();
    // Verify the certificate chain and the host name during the handshake.
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    engine.setSSLParameters(parameters);
    try {
      SocketChannel channel = SocketChannel.open();
      channel.configureBlocking(false);
      channel.socket().setTcpNoDelay(true);
      channel.socket().setKeepAlive(true);
      return new TlsNonblockingSocket(
          configuration.maxFrameSize(), engine, channel, new InetSocketAddress(host, port));
    } catch (IOException | TTransportException e) {
      throw tlsFailure(e);
    }
  }

  private static SSLContext createContext(TlsConfiguration tls) {
    try {
      KeyManager[] keyManagers =
          tls.keyStore() == null
              ? null
              : keyManagers(
                  loadKeyStore(tls.keyStore(), tls.keyStorePassword()), tls.keyStorePassword());
      TrustManager[] trustManagers =
          tls.trustStore() == null
              ? null
              : trustManagers(loadKeyStore(tls.trustStore(), tls.trustStorePassword()));
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagers, trustManagers, null);
      return context;
    } catch (IoTDBR2dbcException e) {
      throw e;
    } catch (Exception e) {
      throw tlsFailure(e);
    }
  }

  private static KeyManager[] keyManagers(KeyStore keyStore, String password) throws Exception {
    char[] secret = password == null ? null : password.toCharArray();
    KeyManagerFactory factory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    factory.init(keyStore, secret);
    return factory.getKeyManagers();
  }

  private static TrustManager[] trustManagers(KeyStore keyStore) throws Exception {
    TrustManagerFactory factory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init(keyStore);
    return factory.getTrustManagers();
  }

  /** PKCS#12 is the format used by the tooling around this project; JKS is the fallback. */
  private static KeyStore loadKeyStore(String path, String password) {
    char[] secret = password == null ? null : password.toCharArray();
    for (String type : new String[] {PKCS12, JKS}) {
      try (InputStream input = Files.newInputStream(Path.of(path))) {
        KeyStore keyStore = KeyStore.getInstance(type);
        keyStore.load(input, secret);
        return keyStore;
      } catch (Exception e) {
        // try the next format
      }
    }
    throw new IoTDBR2dbcException(
        String.format(R2dbcMessages.EXCEPTION_FAILED_TO_INITIALIZE_TLS_ARG_770BBC57, path));
  }

  private static IoTDBR2dbcException tlsFailure(Throwable cause) {
    return new IoTDBR2dbcException(
        String.format(
            R2dbcMessages.EXCEPTION_FAILED_TO_INITIALIZE_TLS_ARG_770BBC57, cause.getMessage()),
        cause);
  }
}
