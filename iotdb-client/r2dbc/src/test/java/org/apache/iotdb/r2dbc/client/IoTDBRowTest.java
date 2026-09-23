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

import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class IoTDBRowTest {

  private static final long TICKS = 1_700_000_000_000L;
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final LocalDate DATE_VALUE = LocalDate.of(2026, 9, 22);
  private static final int DATE_INT = DateUtils.parseDateExpressionToInt(DATE_VALUE);

  @Test
  public void readsDefaultJavaTypes() {
    IoTDBRow row = row(0, UTC, 1_000, new int[] {0, 1, 2, 3, 4, 5});
    assertEquals(Integer.valueOf(7), row.get(0, Integer.class));
    assertEquals("hello", row.get(1, String.class));
    assertEquals(Double.valueOf(1.5D), row.get(2, Double.class));
    assertEquals(3, ((ByteBuffer) row.get(3)).remaining());
    assertEquals(DATE_VALUE, row.get(4, LocalDate.class));
    assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(TICKS), UTC), row.get(5));
  }

  @Test
  public void resolvesColumnsByNameIgnoringCase() {
    IoTDBRow row = row(0, UTC, 1_000, new int[] {0, 1, 2, 3, 4, 5});
    assertEquals(Integer.valueOf(7), row.get("v1", Integer.class));
    assertEquals("hello", row.get("V2", String.class));
  }

  @Test
  public void supportsExplicitTimestampAndBinaryConversions() {
    IoTDBRow row = row(0, UTC, 1_000, new int[] {0, 1, 2, 3, 4, 5});
    assertEquals(Long.valueOf(TICKS), row.get(5, Long.class));
    assertEquals(Instant.ofEpochMilli(TICKS), row.get(5, Instant.class));
    byte[] bytes = row.get(3, byte[].class);
    assertArrayEquals(new byte[] {1, 2, 3}, bytes);
    assertNotSame(row.get(3, ByteBuffer.class), bytes);
  }

  @Test
  public void convertsTicksUsingTheSessionPrecisionAndZone() {
    long micros = 1_700_000_000_000_123L;
    TsBlockBuilder builder = new TsBlockBuilder(List.of(TSDataType.TIMESTAMP));
    builder.getTimeColumnBuilder().writeLong(micros);
    builder.getColumnBuilder(0).writeLong(micros);
    builder.declarePosition();
    IoTDBRow row =
        new IoTDBRow(
            builder.build(),
            0,
            IoTDBRowMetadata.of(List.of("ts"), List.of(TSDataType.TIMESTAMP)),
            ZoneId.of("Asia/Shanghai"),
            1_000_000,
            new TSDataType[] {TSDataType.TIMESTAMP},
            new int[] {0});
    assertEquals(Long.valueOf(micros), row.get(0, Long.class));
    assertEquals(
        LocalDateTime.ofInstant(
            Instant.ofEpochSecond(1_700_000_000L, 123_000L), ZoneId.of("Asia/Shanghai")),
        row.get(0));
  }

  @Test
  public void readsTheTreeModelTimeColumn() {
    TsBlockBuilder builder = new TsBlockBuilder(List.of(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(TICKS);
    builder.getColumnBuilder(0).writeInt(7);
    builder.declarePosition();
    IoTDBRow row =
        new IoTDBRow(
            builder.build(),
            0,
            IoTDBRowMetadata.of(
                List.of("Time", "v1"), List.of(TSDataType.TIMESTAMP, TSDataType.INT32)),
            UTC,
            1_000,
            new TSDataType[] {TSDataType.TIMESTAMP, TSDataType.INT32},
            new int[] {-1, 0});
    assertEquals(Long.valueOf(TICKS), row.get(0, Long.class));
    assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(TICKS), UTC), row.get(0));
    assertEquals(Integer.valueOf(7), row.get(1, Integer.class));
  }

  @Test
  public void returnsNullForNullValues() {
    IoTDBRow row = row(1, UTC, 1_000, new int[] {0, 1, 2, 3, 4, 5});
    assertNull(row.get(0));
    assertNull(row.get("v2"));
    assertNull(row.get(3, byte[].class));
  }

  @Test
  public void rejectsOutOfBoundsAndUnsupportedConversions() {
    IoTDBRow row = row(0, UTC, 1_000, new int[] {0, 1, 2, 3, 4, 5});
    assertThrows(IndexOutOfBoundsException.class, () -> row.get(6));
    assertThrows(NoSuchElementException.class, () -> row.get("missing", String.class));
    assertThrows(IoTDBR2dbcException.class, () -> row.get(0, String.class));
    assertThrows(IoTDBR2dbcException.class, () -> row.get(5, String.class));
    assertThrows(IllegalArgumentException.class, () -> row.get(0, null));
  }

  private static IoTDBRow row(int position, ZoneId zoneId, int timeFactor, int[] indexes) {
    TsBlockBuilder builder =
        new TsBlockBuilder(
            List.of(
                TSDataType.INT32,
                TSDataType.TEXT,
                TSDataType.DOUBLE,
                TSDataType.BLOB,
                TSDataType.DATE,
                TSDataType.TIMESTAMP));
    for (int i = 0; i <= position; i++) {
      boolean nullRow = i == 1;
      builder.getTimeColumnBuilder().writeLong(TICKS);
      if (nullRow) {
        builder.getColumnBuilder(0).appendNull();
        builder.getColumnBuilder(1).appendNull();
        builder.getColumnBuilder(2).appendNull();
        builder.getColumnBuilder(3).appendNull();
        builder.getColumnBuilder(4).appendNull();
        builder.getColumnBuilder(5).appendNull();
      } else {
        builder.getColumnBuilder(0).writeInt(7);
        builder.getColumnBuilder(1).writeBinary(new Binary("hello", StandardCharsets.UTF_8));
        builder.getColumnBuilder(2).writeDouble(1.5D);
        builder.getColumnBuilder(3).writeBinary(new Binary(new byte[] {1, 2, 3}));
        builder.getColumnBuilder(4).writeInt(DATE_INT);
        builder.getColumnBuilder(5).writeLong(TICKS);
      }
      builder.declarePosition();
    }
    TsBlock block = builder.build();
    IoTDBRowMetadata metadata =
        IoTDBRowMetadata.of(
            List.of("v1", "v2", "v3", "v4", "v5", "v6"),
            List.of(
                TSDataType.INT32,
                TSDataType.TEXT,
                TSDataType.DOUBLE,
                TSDataType.BLOB,
                TSDataType.DATE,
                TSDataType.TIMESTAMP));
    TSDataType[] types = {
      TSDataType.INT32,
      TSDataType.TEXT,
      TSDataType.DOUBLE,
      TSDataType.BLOB,
      TSDataType.DATE,
      TSDataType.TIMESTAMP
    };
    return new IoTDBRow(block, position, metadata, zoneId, timeFactor, types, indexes);
  }
}
