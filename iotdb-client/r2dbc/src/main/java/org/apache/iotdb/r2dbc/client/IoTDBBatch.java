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

package org.apache.iotdb.r2dbc.client;

import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.r2dbc.codec.LiteralBinder;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch of parameter free statements.
 *
 * <p>Statements run in order and stop at the first failure. IoTDB offers no batch atomicity, so a
 * failure keeps the effect of the statements that already completed.
 */
public final class IoTDBBatch implements Batch {

  private final IoTDBConnection connection;
  private final List<String> statements = new ArrayList<>();

  IoTDBBatch(IoTDBConnection connection) {
    this.connection = connection;
  }

  @Override
  public Batch add(String sql) {
    if (sql == null || sql.isEmpty()) {
      throw new IllegalArgumentException(
          R2dbcMessages.EXCEPTION_SQL_MUST_NOT_BE_NULL_OR_EMPTY_6BFC5576);
    }
    if (LiteralBinder.placeholderCount(sql) > 0) {
      throw new IoTDBR2dbcException(
          R2dbcMessages.EXCEPTION_BATCH_STATEMENTS_MUST_NOT_CONTAIN_PARAMETERS_E2027860);
    }
    statements.add(sql);
    return this;
  }

  @Override
  public Publisher<? extends Result> execute() {
    return Flux.fromIterable(statements)
        .concatMap(statement -> connection.execute(statement, List.of(), 0));
  }
}
