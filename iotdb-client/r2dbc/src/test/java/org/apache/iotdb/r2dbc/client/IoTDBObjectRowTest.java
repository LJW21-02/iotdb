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

import io.r2dbc.spi.R2dbcType;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class IoTDBObjectRowTest {

  @Test
  public void objectMetadataDoesNotPreventReadingOtherColumns() throws Exception {
    IoTDBRow row = row(new Binary(objectReference(123)));
    assertEquals(Integer.valueOf(7), row.get(1, Integer.class));
    assertEquals(R2dbcType.VARCHAR, row.getMetadata().getColumnMetadata(0).getType());
    assertEquals(String.class, row.getMetadata().getColumnMetadata(0).getJavaType());
    assertEquals(TSDataType.OBJECT, row.getMetadata().getColumnMetadata(0).getNativeTypeMetadata());
  }

  @Test
  public void objectReadsReturnTheReferencedSizeDescription() throws Exception {
    IoTDBRow row = row(new Binary(objectReference(123)));
    assertEquals("(Object) 123 B", row.get(0));
    assertEquals("(Object) 123 B", row.get("object", String.class));
    assertThrows(IoTDBR2dbcException.class, () -> row.get(0, ByteBuffer.class));
    assertThrows(IoTDBR2dbcException.class, () -> row.get(0, byte[].class));
  }

  @Test
  public void nullObjectsReturnNull() throws Exception {
    IoTDBRow row = row(null);
    assertNull(row.get(0));
    assertNull(row.get(0, String.class));
    assertEquals(Integer.valueOf(7), row.get(1, Integer.class));
  }

  @Test
  public void malformedObjectReferencesFailWithAnR2dbcException() throws Exception {
    for (byte[] reference : new byte[][] {new byte[0], new byte[7], objectReference(-1)}) {
      IoTDBRow row = row(new Binary(reference));
      assertEquals(Integer.valueOf(7), row.get(1, Integer.class));
      assertThrows(IoTDBR2dbcException.class, () -> row.get(0));
    }
  }

  private static byte[] objectReference(long size) {
    return ByteBuffer.allocate(Long.BYTES + 4).putLong(size).putInt(123).array();
  }

  private static IoTDBRow row(Binary reference) throws Exception {
    TSExecuteStatementResp response =
        new TSExecuteStatementResp()
            .setColumns(List.of("object", "number"))
            .setDataTypeList(List.of("OBJECT", "INT32"))
            .setIgnoreTimeStamp(true)
            .setColumnIndex2TsBlockColumnIndexList(List.of(0, 1));
    RowDecoder decoder = RowDecoder.of(response, ZoneId.of("UTC"), 1_000);
    TsBlockBuilder builder = new TsBlockBuilder(List.of(TSDataType.OBJECT, TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(123);
    if (reference == null) {
      builder.getColumnBuilder(0).appendNull();
    } else {
      builder.getColumnBuilder(0).writeBinary(reference);
    }
    builder.getColumnBuilder(1).writeInt(7);
    builder.declarePosition();
    TsBlock block = decoder.decode(new TsBlockSerde().serialize(builder.build()));
    return decoder.row(block, 0);
  }
}
