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

package org.apache.iotdb.r2dbc.codec;

import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.R2dbcType;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class IoTDBTypeMapperTest {

  @Test
  public void mapsIoTDBTypesToTheDocumentedDefaults() {
    assertEquals(R2dbcType.BOOLEAN, IoTDBTypeMapper.toR2dbcType(TSDataType.BOOLEAN));
    assertEquals(R2dbcType.INTEGER, IoTDBTypeMapper.toR2dbcType(TSDataType.INT32));
    assertEquals(R2dbcType.BIGINT, IoTDBTypeMapper.toR2dbcType(TSDataType.INT64));
    assertEquals(R2dbcType.REAL, IoTDBTypeMapper.toR2dbcType(TSDataType.FLOAT));
    assertEquals(R2dbcType.DOUBLE, IoTDBTypeMapper.toR2dbcType(TSDataType.DOUBLE));
    assertEquals(R2dbcType.VARCHAR, IoTDBTypeMapper.toR2dbcType(TSDataType.TEXT));
    assertEquals(R2dbcType.VARCHAR, IoTDBTypeMapper.toR2dbcType(TSDataType.STRING));
    assertEquals(R2dbcType.DATE, IoTDBTypeMapper.toR2dbcType(TSDataType.DATE));
    assertEquals(R2dbcType.TIMESTAMP, IoTDBTypeMapper.toR2dbcType(TSDataType.TIMESTAMP));
    assertEquals(R2dbcType.VARBINARY, IoTDBTypeMapper.toR2dbcType(TSDataType.BLOB));
    assertEquals(R2dbcType.VARCHAR, IoTDBTypeMapper.toR2dbcType(TSDataType.OBJECT));
  }

  @Test
  public void defaultJavaTypesFollowR2dbcType() {
    assertEquals(Float.class, IoTDBTypeMapper.toR2dbcType(TSDataType.FLOAT).getJavaType());
    assertEquals(
        LocalDateTime.class, IoTDBTypeMapper.toR2dbcType(TSDataType.TIMESTAMP).getJavaType());
    assertEquals(LocalDate.class, IoTDBTypeMapper.toR2dbcType(TSDataType.DATE).getJavaType());
    assertEquals(ByteBuffer.class, IoTDBTypeMapper.toR2dbcType(TSDataType.BLOB).getJavaType());
    assertEquals(String.class, IoTDBTypeMapper.toR2dbcType(TSDataType.OBJECT).getJavaType());
  }

  @Test
  public void parsesServerTypeNames() {
    assertEquals(TSDataType.INT32, IoTDBTypeMapper.parse("INT32"));
    assertThrows(IoTDBR2dbcException.class, () -> IoTDBTypeMapper.parse("MAGIC"));
  }

  @Test
  public void rejectsTypesWithoutAVerifiedMapping() {
    assertThrows(IoTDBR2dbcException.class, () -> IoTDBTypeMapper.toR2dbcType(TSDataType.UNKNOWN));
  }
}
