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

package org.apache.iotdb.r2dbc;

import org.apache.iotdb.r2dbc.client.IoTDBConnection;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.apache.thrift.async.TAsyncClientManager;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Creates IoTDB R2DBC connections.
 *
 * <p>One factory owns one shared runtime: a single Thrift selector thread and a bounded scheduler
 * that decodes pages and invokes callbacks off the selector. {@link #close()} must be called after
 * all connections are closed; it stops the selector thread.
 */
public final class IoTDBR2dbcConnectionFactory implements ConnectionFactory, AutoCloseable {

  private static final String PRODUCT_NAME = "Apache IoTDB";
  private static final int CALLBACK_THREAD_CAP = 16;
  private static final int CALLBACK_QUEUE_CAP = 100_000;
  private static final int CALLBACK_TTL_SECONDS = 60;

  private final IoTDBConnectionConfiguration configuration;
  private final TAsyncClientManager clientManager;
  private final Scheduler scheduler;
  private final ConnectionFactoryMetadata metadata = () -> PRODUCT_NAME;
  private final AtomicBoolean closed = new AtomicBoolean();

  public IoTDBR2dbcConnectionFactory(IoTDBConnectionConfiguration configuration) {
    this.configuration = configuration;
    try {
      this.clientManager = new TAsyncClientManager();
    } catch (IOException e) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_FAILED_TO_CREATE_THE_R2DBC_RUNTIME_ARG_9766FF8E,
              e.getMessage()),
          e);
    }
    this.scheduler =
        Schedulers.newBoundedElastic(
            CALLBACK_THREAD_CAP, CALLBACK_QUEUE_CAP, "iotdb-r2dbc-callbacks", CALLBACK_TTL_SECONDS);
  }

  @Override
  public Publisher<? extends Connection> create() {
    return Mono.defer(
        () -> {
          if (closed.get()) {
            return Mono.error(
                new IoTDBR2dbcException(R2dbcMessages.EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7));
          }
          return IoTDBConnection.connect(configuration, clientManager, scheduler);
        });
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return metadata;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      try {
        scheduler.dispose();
      } finally {
        clientManager.stop();
      }
    }
  }
}
