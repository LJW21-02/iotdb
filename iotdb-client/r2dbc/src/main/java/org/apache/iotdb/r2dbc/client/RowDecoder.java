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
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns the pages of one server result into rows.
 *
 * <p>The column layout is fixed by the response that opened the result; the decoder then maps
 * server bytes to {@link IoTDBRow} values. It holds no transport state, so a result can be decoded
 * by the cursor, by a test, or by another client that received the same protocol response.
 */
final class RowDecoder {

  private static final TsBlockSerde SERDE = new TsBlockSerde();
  private static final String TIME_COLUMN_NAME = "Time";

  private final IoTDBRowMetadata metadata;
  private final TSDataType[] columnTypes;
  private final int[] tsBlockIndexes;
  private final ZoneId zoneId;
  private final int timeFactor;

  private RowDecoder(
      IoTDBRowMetadata metadata,
      TSDataType[] columnTypes,
      int[] tsBlockIndexes,
      ZoneId zoneId,
      int timeFactor) {
    this.metadata = metadata;
    this.columnTypes = columnTypes;
    this.tsBlockIndexes = tsBlockIndexes;
    this.zoneId = zoneId;
    this.timeFactor = timeFactor;
  }

  /** Builds the decoder of the result a response opened. */
  static RowDecoder of(TSExecuteStatementResp response, ZoneId zoneId, int timeFactor) {
    Columns columns = Columns.from(response);
    return new RowDecoder(
        IoTDBRowMetadata.of(columns.names, columns.types),
        columns.types.toArray(new TSDataType[0]),
        columns.tsBlockIndexes,
        zoneId,
        timeFactor);
  }

  /** Whether the response carries a result set that has to be paged and closed. */
  static boolean hasResultSet(TSExecuteStatementResp response) {
    return response.getColumns() != null && !response.getColumns().isEmpty();
  }

  IoTDBRowMetadata metadata() {
    return metadata;
  }

  /** Deserializes one page of TsBlock bytes. */
  TsBlock decode(ByteBuffer page) {
    TsBlock block;
    try {
      block = SERDE.deserialize(page.duplicate());
    } catch (RuntimeException e) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_FAILED_TO_DECODE_A_TSBLOCK_ARG_050F52E4, e.getMessage()),
          e);
    }
    validateBlockIndexes(block);
    return block;
  }

  /** The row at {@code rowIndex} of a decoded page. */
  IoTDBRow row(TsBlock block, int rowIndex) {
    return new IoTDBRow(block, rowIndex, metadata, zoneId, timeFactor, columnTypes, tsBlockIndexes);
  }

  private void validateBlockIndexes(TsBlock block) {
    int valueColumnCount = block.getValueColumnCount();
    for (int i = 0; i < tsBlockIndexes.length; i++) {
      int mapped = tsBlockIndexes[i];
      if (mapped >= valueColumnCount) {
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages
                    .EXCEPTION_IOTDB_COLUMN_MAPPING_INDEX_ARG_FOR_COLUMN_ARG_EXCEEDS_THE_ARG_TSBLOCK_VALUE_COLUMNS_BB5F2189,
                mapped,
                i,
                valueColumnCount));
      }
    }
  }

  /** Column names, types and TsBlock positions of one server response. */
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
      boolean hasMapping = serverMapping != null && !serverMapping.isEmpty();
      if (hasMapping && serverMapping.size() != serverColumns.size()) {
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages.EXCEPTION_IOTDB_COLUMN_MAPPING_HAS_ARG_ENTRIES_EXPECTED_ARG_E16BC4F0,
                serverMapping.size(),
                serverColumns.size()));
      }
      int offset = ignoreTimeStamp ? 0 : 1;
      if (!ignoreTimeStamp) {
        tsBlockIndexes[0] = -1;
      }
      for (int i = 0; i < serverColumns.size(); i++) {
        Integer mapped = hasMapping ? serverMapping.get(i) : Integer.valueOf(i);
        if (mapped == null || mapped < 0) {
          throw new IoTDBR2dbcException(
              String.format(
                  R2dbcMessages
                      .EXCEPTION_INVALID_IOTDB_COLUMN_MAPPING_INDEX_ARG_FOR_COLUMN_ARG_853658EC,
                  mapped,
                  i));
        }
        tsBlockIndexes[offset + i] = mapped;
      }
      return new Columns(names, types, tsBlockIndexes);
    }
  }
}
