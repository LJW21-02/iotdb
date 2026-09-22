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
import org.apache.iotdb.r2dbc.codec.IoTDBTypeMapper;
import org.apache.iotdb.r2dbc.exception.ExceptionMapper;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import io.r2dbc.spi.Row;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single consumption cursor that decodes one page of TsBlocks at a time.
 *
 * <p>A new page is requested only when the currently buffered rows are exhausted and the subscriber
 * still has outstanding demand, so the driver never prefetches while a consumer is slow. The number
 * of buffered pages is at most the {@code fetchSize} driven batch that the server returned for the
 * previous request.
 */
public final class IoTDBQueryCursor implements Publisher<Row>, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBQueryCursor.class);
  private static final TsBlockSerde SERDE = new TsBlockSerde();
  private static final String TIME_COLUMN_NAME = "Time";

  private final IoTDBConnection connection;
  private final String sql;
  private final long queryId;
  private final int fetchSize;
  private final String preparedStatementName;
  private final IoTDBRowMetadata metadata;
  private final TSDataType[] columnTypes;
  private final int[] tsBlockIndexes;
  private final ZoneId zoneId;
  private final int timeFactor;
  private final Scheduler scheduler;
  private final AtomicBoolean subscribed = new AtomicBoolean();

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
    Columns columns = Columns.from(response);
    this.columnTypes = columns.types.toArray(new TSDataType[0]);
    this.tsBlockIndexes = columns.tsBlockIndexes;
    this.metadata = IoTDBRowMetadata.of(columns.names, columns.types);
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

  /** Whether the response carries a result set that must be paged and closed. */
  public static boolean hasResultSet(TSExecuteStatementResp response) {
    return response.getColumns() != null && !response.getColumns().isEmpty();
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
        synchronized (this) {
          if (cancelled || finished) {
            return;
          }
          if (demand <= 0) {
            return;
          }
          try {
            row = nextRow();
          } catch (RuntimeException e) {
            decodeError = e;
            finished = true;
          }
          if (decodeError != null) {
            // Release before signalling completion so a new statement on the same connection
            // cannot observe a cursor that is still registered.
            close();
            sink.error(decodeError);
            return;
          }
          if (row != null) {
            demand--;
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
          // Complete the server side cursor before the consumer is unblocked, so the next
          // statement waits for the cleanup instead of racing with it.
          close();
          sink.complete();
          return;
        }
        if (fetch) {
          fetchNextPage();
          return;
        }
        sink.next(row);
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
        return new IoTDBRow(
            block, rowIndex++, metadata, zoneId, timeFactor, columnTypes, tsBlockIndexes);
      }
      block = null;
      ByteBuffer page = pages.poll();
      if (page == null) {
        return null;
      }
      try {
        block = SERDE.deserialize(page.duplicate());
      } catch (RuntimeException e) {
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages.EXCEPTION_FAILED_TO_DECODE_A_TSBLOCK_ARG_050F52E4, e.getMessage()),
            e);
      }
      rowIndex = 0;
    }
  }

  private void fetchNextPage() {
    connection
        .fetchNextPage(queryId, sql, fetchSize)
        .subscribe(
            response -> {
              synchronized (this) {
                fetching = false;
                moreData = response.isMoreData();
                if (response.getQueryResult() != null) {
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
              sink.error(
                  ExceptionMapper.mapTransportFailure(
                      String.format(
                          R2dbcMessages.EXCEPTION_READING_THE_NEXT_PAGE_FAILED_ARG_74678706,
                          error.getMessage()),
                      error));
            });
  }

  /** Keeps user callbacks and decoding off the shared Thrift selector thread. */
  private void scheduleDrain() {
    try {
      scheduler.schedule(this::drain);
    } catch (RuntimeException e) {
      drain();
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
    }
    connection
        .releaseQuery(queryId, preparedStatementName)
        .subscribe(
            ignored -> {},
            error ->
                LOGGER.warn(R2dbcMessages.LOG_CLEANING_UP_THE_QUERY_HANDLE_FAILED_DE915B4D, error));
  }

  private static final class Columns {

    private final List<String> names;
    private final List<TSDataType> types;
    private final int[] tsBlockIndexes;

    private Columns(List<String> names, List<TSDataType> types, int[] tsBlockIndexes) {
      this.names = names;
      this.types = types;
      this.tsBlockIndexes = tsBlockIndexes;
    }

    private static Columns from(TSExecuteStatementResp response) {
      List<String> serverColumns =
          response.getColumns() == null ? List.of() : response.getColumns();
      List<String> serverTypes =
          response.getDataTypeList() == null ? List.of() : response.getDataTypeList();
      if (serverTypes.size() != serverColumns.size()) {
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages.EXCEPTION_UNEXPECTED_ARG_RESPONSE_FROM_IOTDB_6A4FBFB0,
                "executeStatement"));
      }
      boolean ignoreTimeStamp = response.isIgnoreTimeStamp();
      List<String> names = new ArrayList<>(serverColumns.size() + 1);
      List<TSDataType> types = new ArrayList<>(serverColumns.size() + 1);
      if (!ignoreTimeStamp) {
        // Tree model responses omit the time column; it is prepended like the session client does.
        names.add(TIME_COLUMN_NAME);
        types.add(TSDataType.TIMESTAMP);
      }
      for (int i = 0; i < serverColumns.size(); i++) {
        names.add(serverColumns.get(i));
        types.add(IoTDBTypeMapper.parse(serverTypes.get(i)));
      }
      int[] tsBlockIndexes = new int[names.size()];
      List<Integer> serverMapping = response.getColumnIndex2TsBlockColumnIndexList();
      int offset = ignoreTimeStamp ? 0 : 1;
      if (!ignoreTimeStamp) {
        tsBlockIndexes[0] = -1;
      }
      for (int i = 0; i < serverColumns.size(); i++) {
        int mapped =
            serverMapping != null && serverMapping.size() == serverColumns.size()
                ? serverMapping.get(i)
                : i;
        tsBlockIndexes[offset + i] = mapped;
      }
      return new Columns(names, types, tsBlockIndexes);
    }
  }
}
