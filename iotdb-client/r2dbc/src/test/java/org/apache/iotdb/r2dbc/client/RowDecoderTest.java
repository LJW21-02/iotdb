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
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class RowDecoderTest {

  private static final ZoneId UTC = ZoneId.of("UTC");

  @Test
  public void rejectsShortAndLongNonemptyMappings() {
    assertThrows(IoTDBR2dbcException.class, () -> RowDecoder.of(response(List.of(0)), UTC, 1_000));
    assertThrows(
        IoTDBR2dbcException.class, () -> RowDecoder.of(response(List.of(0, 1, 2)), UTC, 1_000));
  }

  @Test
  public void missingAndEmptyMappingsUseTheIdentityLayout() throws Exception {
    for (List<Integer> mapping : Arrays.asList(null, List.<Integer>of())) {
      RowDecoder decoder = RowDecoder.of(response(mapping), UTC, 1_000);
      TsBlock block = decoder.decode(page(7, 11));
      assertEquals(Integer.valueOf(7), decoder.row(block, 0).get(0, Integer.class));
      assertEquals(Integer.valueOf(11), decoder.row(block, 0).get(1, Integer.class));
    }
  }

  @Test
  public void rejectsNullAndNegativeMappingEntries() {
    for (List<Integer> mapping :
        Arrays.asList(Arrays.asList(null, 0), List.of(-1, 0), List.of(-2, 0))) {
      assertThrows(IoTDBR2dbcException.class, () -> RowDecoder.of(response(mapping), UTC, 1_000));
    }
  }

  @Test
  public void preservesReorderedAndRepeatedMappings() throws Exception {
    RowDecoder reordered = RowDecoder.of(response(List.of(1, 0)), UTC, 1_000);
    IoTDBRow reorderedRow = reordered.row(reordered.decode(page(7, 11)), 0);
    assertEquals(Integer.valueOf(11), reorderedRow.get(0, Integer.class));
    assertEquals(Integer.valueOf(7), reorderedRow.get(1, Integer.class));

    RowDecoder repeated = RowDecoder.of(response(List.of(0, 0)), UTC, 1_000);
    IoTDBRow repeatedRow = repeated.row(repeated.decode(page(7)), 0);
    assertEquals(Integer.valueOf(7), repeatedRow.get(0, Integer.class));
    assertEquals(Integer.valueOf(7), repeatedRow.get(1, Integer.class));
  }

  @Test
  public void acceptsAnUnusedPhysicalValueColumn() throws Exception {
    RowDecoder decoder = RowDecoder.of(response(List.of(2, 0)), UTC, 1_000);
    IoTDBRow row = decoder.row(decoder.decode(page(7, 11, 19)), 0);
    assertEquals(Integer.valueOf(19), row.get(0, Integer.class));
    assertEquals(Integer.valueOf(7), row.get(1, Integer.class));
  }

  @Test
  public void rejectsMappingOutsideTheDecodedValueColumns() throws Exception {
    RowDecoder decoder = RowDecoder.of(response(List.of(0, 2)), UTC, 1_000);
    ByteBuffer page = page(7, 11);
    assertThrows(IoTDBR2dbcException.class, () -> decoder.decode(page));
  }

  @Test
  public void rejectsAnIdentityMappingWhenThePageHasTooFewColumns() throws Exception {
    RowDecoder decoder = RowDecoder.of(response(null), UTC, 1_000);
    ByteBuffer page = page(7);
    assertThrows(IoTDBR2dbcException.class, () -> decoder.decode(page));
  }

  @Test
  public void validatesTheMappingForEveryPage() throws Exception {
    RowDecoder decoder = RowDecoder.of(response(List.of(0, 1)), UTC, 1_000);
    decoder.decode(page(7, 11));
    ByteBuffer malformedLaterPage = page(19);
    assertThrows(IoTDBR2dbcException.class, () -> decoder.decode(malformedLaterPage));
  }

  @Test
  public void treeTimeColumnDoesNotCountAsAValueColumn() throws Exception {
    TSExecuteStatementResp response = response(List.of(1, 0)).setIgnoreTimeStamp(false);
    RowDecoder decoder = RowDecoder.of(response, UTC, 1_000);
    IoTDBRow row = decoder.row(decoder.decode(page(7, 11)), 0);
    assertEquals(Long.valueOf(123), row.get(0, Long.class));
    assertEquals(Integer.valueOf(11), row.get(1, Integer.class));
    assertEquals(Integer.valueOf(7), row.get(2, Integer.class));
  }

  private static TSExecuteStatementResp response(List<Integer> mapping) {
    return new TSExecuteStatementResp()
        .setColumns(List.of("first", "second"))
        .setDataTypeList(List.of("INT32", "INT32"))
        .setIgnoreTimeStamp(true)
        .setColumnIndex2TsBlockColumnIndexList(mapping);
  }

  private static ByteBuffer page(int... values) throws Exception {
    TsBlockBuilder builder =
        new TsBlockBuilder(Collections.nCopies(values.length, TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(123);
    for (int i = 0; i < values.length; i++) {
      builder.getColumnBuilder(i).writeInt(values[i]);
    }
    builder.declarePosition();
    return new TsBlockSerde().serialize(builder.build());
  }
}
