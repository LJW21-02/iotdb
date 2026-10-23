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
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Result of one executed statement.
 *
 * <p>All views ({@code map}, {@code flatMap}, {@code filter}) share one consumption state: the
 * underlying page stream can be consumed once, a second attempt fails instead of silently replaying
 * or dropping rows. IoTDB does not report affected row counts, so {@code getRowsUpdated()} always
 * completes without an update count rather than fabricating {@code 0} or {@code 1}.
 */
public final class IoTDBResult implements Result {

  private final Publisher<Row> rows;
  private final Predicate<Segment> segmentFilter;
  private final AtomicBoolean consumed;
  private final Mono<Void> consumption;
  private final Runnable discardAction;

  private IoTDBResult(
      Publisher<Row> rows,
      Predicate<Segment> segmentFilter,
      AtomicBoolean consumed,
      Mono<Void> consumption,
      Runnable discardAction) {
    this.rows = rows;
    this.segmentFilter = segmentFilter;
    this.consumed = consumed;
    this.consumption = consumption;
    this.discardAction = discardAction;
  }

  public static IoTDBResult of(IoTDBQueryCursor cursor) {
    return new IoTDBResult(
        cursor, null, new AtomicBoolean(), cursor.releaseCompletion(), cursor::close);
  }

  /** Result of a statement that produced no row set, for example an insert or a DDL statement. */
  public static IoTDBResult empty() {
    return new IoTDBResult(Flux.empty(), null, new AtomicBoolean(), Mono.empty(), () -> {});
  }

  static IoTDBResult of(Publisher<Row> rows, Mono<Void> consumption, Runnable discardAction) {
    return new IoTDBResult(rows, null, new AtomicBoolean(), consumption, discardAction);
  }

  /**
   * Completes once this result has been consumed and any server-side cursor has been released.
   *
   * <p>This is used by a statement's internal result sequence to avoid starting the next execution
   * while the current query still owns the connection. It is deliberately independent of the
   * result's consumption flag because the cursor may need asynchronous cleanup after the row
   * publisher terminates.
   */
  Mono<Void> awaitConsumption() {
    return consumption.doOnCancel(discardAction);
  }

  Flux<IoTDBResult> followedBy(Publisher<IoTDBResult> next) {
    return Flux.concat(Mono.just(this), awaitConsumption().thenMany(next))
        .doOnDiscard(IoTDBResult.class, IoTDBResult::discard);
  }

  void discard() {
    discardAction.run();
  }

  @Override
  public Publisher<Long> getRowsUpdated() {
    markConsumed();
    return Flux.<Long>empty().doOnSubscribe(subscription -> discard());
  }

  @Override
  public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
    markConsumed();
    return filteredRows().map(row -> mappingFunction.apply(row, row.getMetadata()));
  }

  @Override
  public Result filter(Predicate<Segment> filter) {
    requireAvailable();
    Predicate<Segment> combined =
        segmentFilter == null
            ? filter
            : segment -> segmentFilter.test(segment) && filter.test(segment);
    return new IoTDBResult(rows, combined, consumed, consumption, discardAction);
  }

  @Override
  public <T> Publisher<T> flatMap(
      Function<Segment, ? extends Publisher<? extends T>> mappingFunction) {
    markConsumed();
    return filteredRows().flatMap(row -> mappingFunction.apply(new RowSegment(row)));
  }

  private Flux<Row> filteredRows() {
    Flux<Row> flux = Flux.from(rows);
    if (segmentFilter != null) {
      return flux.filter(row -> segmentFilter.test(new RowSegment(row)));
    }
    return flux;
  }

  /** Rows are the only segment this driver produces; update counts are never fabricated. */
  private static final class RowSegment implements Segment, Result.RowSegment {

    private final Row row;

    private RowSegment(Row row) {
      this.row = row;
    }

    @Override
    public Row row() {
      return row;
    }
  }

  private void requireAvailable() {
    if (consumed.get()) {
      throw new IoTDBR2dbcException(
          R2dbcMessages.EXCEPTION_THE_RESULT_HAS_ALREADY_BEEN_CONSUMED_1A38526E);
    }
  }

  private void markConsumed() {
    if (!consumed.compareAndSet(false, true)) {
      throw new IoTDBR2dbcException(
          R2dbcMessages.EXCEPTION_THE_RESULT_HAS_ALREADY_BEEN_CONSUMED_1A38526E);
    }
  }
}
