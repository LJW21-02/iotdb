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
import org.apache.iotdb.r2dbc.exception.ExceptionMapper;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import io.r2dbc.spi.Row;
import org.apache.tsfile.read.common.block.TsBlock;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single consumption cursor over the pages of one query, one page of TsBlocks at a time.
 *
 * <p>A new page is requested only when the currently buffered rows are exhausted and the subscriber
 * still has outstanding demand, so the driver never prefetches while a consumer is slow. The number
 * of buffered pages is at most the {@code fetchSize} driven batch that the server returned for the
 * previous request.
 */
public final class IoTDBQueryCursor implements Publisher<Row>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBQueryCursor.class);

  private final IoTDBConnection connection;
  private final String sql;
  private final long queryId;
  private final int fetchSize;
  private final String preparedStatementName;
  private final RowDecoder decoder;
  private final ZoneId zoneId;
  private final int timeFactor;
  private final Scheduler scheduler;
  private final AtomicBoolean subscribed = new AtomicBoolean();
  private final CompletableFuture<Void> releaseCompletion = new CompletableFuture<>();

  private final Deque<ByteBuffer> pages = new ArrayDeque<>();
  private TsBlock block;
  private int rowIndex;
  private boolean moreData;
  private boolean fetching;
  private boolean finished;
  private boolean cancelled;
  private boolean draining;
  private boolean released;
  private long demand;
  private RuntimeException decodeError;
  private FluxSink<Row> sink;

  private IoTDBQueryCursor(
      IoTDBConnection connection,
      String sql,
      TSExecuteStatementResp response,
      int fetchSize,
      String preparedStatementName,
      Scheduler scheduler,
      int timeFactor,
      ZoneId zoneId) {
    this.connection = connection;
    this.sql = sql;
    this.queryId = response.getQueryId();
    this.fetchSize = fetchSize;
    this.preparedStatementName = preparedStatementName;
    this.scheduler = scheduler;
    this.timeFactor = timeFactor;
    this.zoneId = zoneId;
    this.decoder = RowDecoder.of(response, zoneId, timeFactor);
    if (response.getQueryResult() != null) {
      pages.addAll(response.getQueryResult());
    }
    this.moreData = response.isMoreData();
  }

  public static IoTDBQueryCursor from(
      IoTDBConnection connection,
      String sql,
      TSExecuteStatementResp response,
      int fetchSize,
      String preparedStatementName,
      Scheduler scheduler,
      int timeFactor,
      ZoneId zoneId) {
    return new IoTDBQueryCursor(
        connection, sql, response, fetchSize, preparedStatementName, scheduler, timeFactor, zoneId);
  }

  Mono<Void> releaseCompletion() {
    return Mono.fromFuture(releaseCompletion, true);
  }

  @Override
  public void subscribe(Subscriber<? super Row> subscriber) {
    if (!subscribed.compareAndSet(false, true)) {
      Flux.<Row>error(
              new IoTDBR2dbcException(
                  R2dbcMessages.EXCEPTION_THE_RESULT_HAS_ALREADY_BEEN_CONSUMED_1A38526E))
          .subscribe(subscriber);
      return;
    }
    Flux.<Row>create(this::start, FluxSink.OverflowStrategy.ERROR).subscribe(subscriber);
  }

  private void start(FluxSink<Row> sink) {
    this.sink = sink;
    sink.onRequest(this::request);
    sink.onCancel(this::cancel);
    sink.onDispose(this::dispose);
    drain();
  }

  private void request(long requested) {
    if (requested <= 0) {
      return;
    }
    synchronized (this) {
      demand = demand + requested < 0 ? Long.MAX_VALUE : demand + requested;
    }
    drain();
  }

  private void drain() {
    synchronized (this) {
      if (draining) {
        return;
      }
      draining = true;
    }
    try {
      while (true) {
        Row row = null;
        boolean fetch = false;
        boolean complete = false;
        boolean completeAfterRow = false;
        synchronized (this) {
          if (cancelled || finished) {
            return;
          }
          if (demand <= 0 && (hasBufferedRows() || moreData || fetching)) {
            return;
          }
          if (demand <= 0) {
            finished = true;
            complete = true;
          } else {
            try {
              row = nextRow();
            } catch (RuntimeException e) {
              decodeError = e;
              finished = true;
            }
          }
          if (decodeError != null) {
            // Release before signalling completion so a new statement on the same connection
            // cannot observe a cursor that is still registered.
            close();
            sink.error(decodeError);
            return;
          }
          if (complete) {
            // The response already proved there are no more rows, so completion must not wait
            // for another request after the final demanded row.
          } else if (row != null) {
            demand--;
            // A terminal page may contain exactly the requested number of rows. Complete in
            // this drain pass instead of waiting for a subsequent request that will never come.
            completeAfterRow = demand == 0 && !hasBufferedRows() && !moreData && !fetching;
          } else if (fetching) {
            return;
          } else if (moreData) {
            fetching = true;
            fetch = true;
          } else {
            finished = true;
            complete = true;
          }
        }
        if (complete) {
          // Queue cursor cleanup before notifying the consumer; later statements wait for it.
          finish();
          return;
        }
        if (fetch) {
          fetchNextPage();
          return;
        }
        sink.next(row);
        if (completeAfterRow) {
          finish();
          return;
        }
      }
    } finally {
      synchronized (this) {
        draining = false;
      }
    }
  }

  private Row nextRow() {
    while (true) {
      if (block != null && rowIndex < block.getPositionCount()) {
        return decoder.row(block, rowIndex++);
      }
      block = null;
      ByteBuffer page = pages.poll();
      if (page == null) {
        return null;
      }
      block = decoder.decode(page);
      rowIndex = 0;
    }
  }

  private boolean hasBufferedRows() {
    return (block != null && rowIndex < block.getPositionCount()) || !pages.isEmpty();
  }

  private void finish() {
    close();
    sink.complete();
  }

  private void fetchNextPage() {
    connection
        .fetchNextPage(queryId, sql, fetchSize)
        .subscribe(
            response -> {
              synchronized (this) {
                fetching = false;
                if (cancelled || finished) {
                  return;
                }
                // A page without a result set ends the query, even when the server still reports
                // moreData; this mirrors the reference session client.
                moreData = response.isHasResultSet() && response.isMoreData();
                if (response.isHasResultSet() && response.getQueryResult() != null) {
                  pages.addAll(response.getQueryResult());
                }
              }
              scheduleDrain();
            },
            error -> {
              synchronized (this) {
                fetching = false;
                finished = true;
              }
              close();
              sink.error(ExceptionMapper.mapPageFailure(error));
            });
  }

  /** Keeps page decoding and row callbacks off the shared Thrift selector thread. */
  private void scheduleDrain() {
    try {
      scheduler.schedule(this::drain);
    } catch (RuntimeException e) {
      synchronized (this) {
        if (cancelled || finished) {
          return;
        }
        finished = true;
      }
      // Rejection must not turn the selector into a page-decoding or row-callback thread.
      // Release the cursor before reporting failure, even if its callback scheduler is disposed.
      close();
      RuntimeException failure = ExceptionMapper.mapPageFailure(e);
      try {
        // Error notification only: this shared, fixed-size scheduler never decodes a page.
        Schedulers.parallel().schedule(() -> sink.error(failure));
      } catch (RuntimeException terminalRejection) {
        // During global scheduler shutdown no asynchronous notification lane may remain. Always
        // terminate the sink rather than retaining an already-released query indefinitely.
        failure.addSuppressed(terminalRejection);
        sink.error(failure);
      }
    }
  }

  private void cancel() {
    synchronized (this) {
      cancelled = true;
    }
    close();
  }

  private void dispose() {
    close();
  }

  /**
   * Releases the server side query handle exactly once.
   *
   * <p>Also called when the connection is closed; the connection queues the close request behind
   * any request that is still in flight.
   */
  @Override
  public void close() {
    synchronized (this) {
      if (released) {
        return;
      }
      released = true;
      cancelled = true;
      pages.clear();
      block = null;
    }
    connection
        .releaseQuery(queryId, preparedStatementName)
        .doFinally(signal -> releaseCompletion.complete(null))
        .subscribe(
            ignored -> {},
            error ->
                LOGGER.warn(R2dbcMessages.LOG_CLEANING_UP_THE_QUERY_HANDLE_FAILED_DE915B4D, error));
  }
}
