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

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A row backed by the {@link TsBlock} of the current page.
 *
 * <p>Values are decoded on access, so a row stays valid as long as its block is not released; the
 * SPI only requires a row to be readable inside a mapping function.
 */
public final class IoTDBRow implements Row {

  private final TsBlock block;
  private final int rowIndex;
  private final IoTDBRowMetadata metadata;
  private final ZoneId zoneId;
  private final int timeFactor;
  private final TSDataType[] columnTypes;
  private final int[] tsBlockIndexes;

  IoTDBRow(
      TsBlock block,
      int rowIndex,
      IoTDBRowMetadata metadata,
      ZoneId zoneId,
      int timeFactor,
      TSDataType[] columnTypes,
      int[] tsBlockIndexes) {
    this.block = block;
    this.rowIndex = rowIndex;
    this.metadata = metadata;
    this.zoneId = zoneId;
    this.timeFactor = timeFactor;
    this.columnTypes = columnTypes;
    this.tsBlockIndexes = tsBlockIndexes;
  }

  @Override
  public RowMetadata getMetadata() {
    return metadata;
  }

  @Override
  public <T> T get(int index, Class<T> type) {
    if (type == null) {
      throw new IllegalArgumentException(R2dbcMessages.EXCEPTION_TYPE_MUST_NOT_BE_NULL_3C9753B3);
    }
    return type.cast(read(index, type));
  }

  @Override
  public <T> T get(String name, Class<T> type) {
    if (name == null) {
      throw new IllegalArgumentException(R2dbcMessages.EXCEPTION_NAME_MUST_NOT_BE_NULL_CA324245);
    }
    Integer index = metadata.index(name);
    if (index == null) {
      throw new NoSuchElementException(
          String.format(R2dbcMessages.EXCEPTION_COLUMN_ARG_DOES_NOT_EXIST_FF67AE82, name));
    }
    return get(index, type);
  }

  private Object read(int index, Class<?> requestedType) {
    if (index < 0 || index >= columnTypes.length) {
      throw new IndexOutOfBoundsException(
          String.format(
              R2dbcMessages
                  .EXCEPTION_COLUMN_INDEX_ARG_IS_OUT_OF_BOUNDS_THE_ROW_HAS_ARG_COLUMN_S_98C75AD7,
              index,
              columnTypes.length));
    }
    TSDataType dataType = columnTypes[index];
    int blockIndex = tsBlockIndexes[index];
    if (blockIndex < 0) {
      // Tree model time column: the value is not part of the value columns of the TsBlock.
      return fromTicks(block.getTimeByIndex(rowIndex), dataType, requestedType);
    }
    Column column = block.getColumn(blockIndex);
    if (column.isNull(rowIndex)) {
      return null;
    }
    switch (dataType) {
      case TIMESTAMP:
        return fromTicks(column.getLong(rowIndex), dataType, requestedType);
      case BLOB:
        return fromBinary(column.getBinary(rowIndex), requestedType);
      case BOOLEAN:
        return convertPrimitive(column.getBoolean(rowIndex), dataType, requestedType);
      case INT32:
        return convertPrimitive(column.getInt(rowIndex), dataType, requestedType);
      case INT64:
        return convertPrimitive(column.getLong(rowIndex), dataType, requestedType);
      case FLOAT:
        return convertPrimitive(column.getFloat(rowIndex), dataType, requestedType);
      case DOUBLE:
        return convertPrimitive(column.getDouble(rowIndex), dataType, requestedType);
      case TEXT:
      case STRING:
        return convertText(
            column.getBinary(rowIndex).getStringValue(TSFileConfig.STRING_CHARSET), requestedType);
      case DATE:
        return convertDate(DateUtils.parseIntToLocalDate(column.getInt(rowIndex)), requestedType);
      default:
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages.EXCEPTION_UNSUPPORTED_IOTDB_DATA_TYPE_ARG_69C453DA, dataType));
    }
  }

  private Object fromTicks(long ticks, TSDataType dataType, Class<?> requestedType) {
    if (Object.class.equals(requestedType) || LocalDateTime.class.equals(requestedType)) {
      return LocalDateTime.ofInstant(toInstant(ticks), zoneId);
    }
    if (Long.class.equals(requestedType)) {
      return ticks;
    }
    if (Instant.class.equals(requestedType)) {
      return toInstant(ticks);
    }
    throw cannotConvert(dataType, requestedType);
  }

  /** Converts epoch ticks in the session precision into an exact instant. */
  private Instant toInstant(long ticks) {
    long seconds = Math.floorDiv(ticks, timeFactor);
    long remainder = Math.floorMod(ticks, timeFactor);
    return Instant.ofEpochSecond(seconds, remainder * (1_000_000_000L / timeFactor));
  }

  private static Object fromBinary(Binary binary, Class<?> requestedType) {
    if (Object.class.equals(requestedType) || ByteBuffer.class.equals(requestedType)) {
      return ByteBuffer.wrap(binary.getValues());
    }
    if (byte[].class.equals(requestedType)) {
      return Arrays.copyOf(binary.getValues(), binary.getLength());
    }
    throw cannotConvert(TSDataType.BLOB, requestedType);
  }

  private static Object convertPrimitive(
      Object value, TSDataType dataType, Class<?> requestedType) {
    if (Object.class.equals(requestedType) || requestedType.isInstance(value)) {
      return value;
    }
    if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
      if (Long.class.equals(requestedType)) {
        return ((Number) value).longValue();
      }
    }
    if (value instanceof Float && Double.class.equals(requestedType)) {
      return ((Float) value).doubleValue();
    }
    throw cannotConvert(dataType, requestedType);
  }

  private static Object convertText(String value, Class<?> requestedType) {
    if (Object.class.equals(requestedType) || String.class.equals(requestedType)) {
      return value;
    }
    throw cannotConvert(TSDataType.TEXT, requestedType);
  }

  private static Object convertDate(LocalDate value, Class<?> requestedType) {
    if (Object.class.equals(requestedType) || LocalDate.class.equals(requestedType)) {
      return value;
    }
    throw cannotConvert(TSDataType.DATE, requestedType);
  }

  private static IoTDBR2dbcException cannotConvert(TSDataType dataType, Class<?> requestedType) {
    return new IoTDBR2dbcException(
        String.format(
            R2dbcMessages.EXCEPTION_CANNOT_CONVERT_ARG_TO_ARG_D88C8FE8,
            dataType,
            requestedType.getName()));
  }
}
