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
import org.apache.iotdb.r2dbc.codec.ParameterBinding;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A statement executed by one connection.
 *
 * <p>Parameters are bound by zero-based position, as required by the SPI; every {@code add()}
 * starts a new binding set and {@code execute()} produces one result per set, in order.
 */
public final class IoTDBStatement implements Statement {

  private final IoTDBConnection connection;
  private final String sql;
  private final int placeholders;
  private final List<Map<Integer, ParameterBinding>> bindingSets = new ArrayList<>();
  private int currentBindingSet;
  private int fetchSize;

  IoTDBStatement(IoTDBConnection connection, String sql) {
    if (sql == null || sql.isEmpty()) {
      throw new IllegalArgumentException(
          R2dbcMessages.EXCEPTION_SQL_MUST_NOT_BE_NULL_OR_EMPTY_6BFC5576);
    }
    this.connection = connection;
    this.sql = sql;
    this.placeholders = LiteralBinder.placeholderCount(sql);
    this.bindingSets.add(new LinkedHashMap<>());
  }

  @Override
  public Statement add() {
    if (bindingSets.get(currentBindingSet).size() < placeholders) {
      throw new IllegalStateException(R2dbcMessages.EXCEPTION_PARAMETER_ARG_IS_NOT_BOUND_F893971C);
    }
    bindingSets.add(new LinkedHashMap<>());
    currentBindingSet = bindingSets.size() - 1;
    return this;
  }

  @Override
  public Statement bind(int index, Object value) {
    requireValidIndex(index);
    bindingSets.get(currentBindingSet).put(index, ParameterBinding.of(value));
    return this;
  }

  @Override
  public Statement bind(String name, Object value) {
    requireKnownName(name);
    throw new NoSuchElementException(
        R2dbcMessages
            .EXCEPTION_NAMED_PARAMETERS_ARE_NOT_SUPPORTED_USE_POSITIONAL_PLACEHOLDERS_A7D827BE);
  }

  @Override
  public Statement bindNull(int index, Class<?> type) {
    requireValidIndex(index);
    bindingSets.get(currentBindingSet).put(index, ParameterBinding.nullOf(type));
    return this;
  }

  @Override
  public Statement bindNull(String name, Class<?> type) {
    requireKnownName(name);
    throw new NoSuchElementException(
        R2dbcMessages
            .EXCEPTION_NAMED_PARAMETERS_ARE_NOT_SUPPORTED_USE_POSITIONAL_PLACEHOLDERS_A7D827BE);
  }

  private static void requireKnownName(String name) {
    if (name == null) {
      throw new IllegalArgumentException(
          R2dbcMessages
              .EXCEPTION_BIND_REQUIRES_A_NON_NULL_VALUE_USE_BINDNULL_FOR_SQL_NULL_57886CDE);
    }
  }

  @Override
  public Statement fetchSize(int rows) {
    if (rows < 0) {
      throw new IllegalArgumentException(
          String.format(
              R2dbcMessages.EXCEPTION_FETCHSIZE_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_8B2CA91B, rows));
    }
    this.fetchSize = rows;
    return this;
  }

  @Override
  public Statement returnGeneratedValues(String... columns) {
    throw new UnsupportedOperationException(
        R2dbcMessages.EXCEPTION_RETURNGENERATEDVALUES_IS_NOT_SUPPORTED_4550147B);
  }

  @Override
  public Publisher<? extends Result> execute() {
    List<List<ParameterBinding>> executionParameters = snapshotParameters();
    List<String> statements = LiteralBinder.splitStatements(sql);
    if (statements.size() > 1 && placeholders > 0) {
      throw new IoTDBR2dbcException(
          R2dbcMessages
              .EXCEPTION_PARAMETERS_CANNOT_BE_BOUND_TO_A_STATEMENT_THAT_CONTAINS_SEVERAL_SQL_STATEMENTS_89D234C0);
    }
    // A statement with several SQL texts produces one result per text, in order. Keep the
    // binding-set/SQL product lazy so large batches do not allocate a second full execution list.
    return Flux.fromIterable(executionParameters)
        .concatMap(
            bindings ->
                Flux.fromIterable(statements)
                    .concatMap(
                        statement ->
                            connection
                                .execute(statement, bindings, fetchSize)
                                .flatMapMany(result -> result.followedBy(Flux.empty()))));
  }

  private List<List<ParameterBinding>> snapshotParameters() {
    List<List<ParameterBinding>> snapshot = new ArrayList<>(bindingSets.size());
    for (Map<Integer, ParameterBinding> bindings : bindingSets) {
      List<ParameterBinding> ordered = new ArrayList<>(placeholders);
      for (int index = 0; index < placeholders; index++) {
        ParameterBinding binding = bindings.get(index);
        if (binding == null) {
          throw new IllegalStateException(
              String.format(R2dbcMessages.EXCEPTION_PARAMETER_ARG_IS_NOT_BOUND_F893971C, index));
        }
        ordered.add(binding);
      }
      snapshot.add(ordered);
    }
    return snapshot;
  }

  private void requireValidIndex(int index) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(
          String.format(
              R2dbcMessages.EXCEPTION_PARAMETER_INDEX_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_59A7879B,
              index));
    }
    if (index >= placeholders) {
      throw new IndexOutOfBoundsException(
          String.format(
              R2dbcMessages
                  .EXCEPTION_PARAMETER_INDEX_ARG_EXCEEDS_THE_ARG_PLACEHOLDERS_OF_THE_STATEMENT_F5525F22,
              index,
              placeholders));
    }
  }
}
