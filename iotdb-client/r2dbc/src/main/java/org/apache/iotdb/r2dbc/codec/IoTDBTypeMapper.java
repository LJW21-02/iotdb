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

import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import io.r2dbc.spi.R2dbcType;
import org.apache.tsfile.enums.TSDataType;

import java.util.EnumMap;
import java.util.Map;

/** Maps IoTDB data types to R2DBC types and back. */
public final class IoTDBTypeMapper {

  private static final Map<TSDataType, R2dbcType> R2DBC_TYPES = new EnumMap<>(TSDataType.class);

  static {
    R2DBC_TYPES.put(TSDataType.BOOLEAN, R2dbcType.BOOLEAN);
    R2DBC_TYPES.put(TSDataType.INT32, R2dbcType.INTEGER);
    R2DBC_TYPES.put(TSDataType.INT64, R2dbcType.BIGINT);
    R2DBC_TYPES.put(TSDataType.FLOAT, R2dbcType.REAL);
    R2DBC_TYPES.put(TSDataType.DOUBLE, R2dbcType.DOUBLE);
    R2DBC_TYPES.put(TSDataType.TEXT, R2dbcType.VARCHAR);
    R2DBC_TYPES.put(TSDataType.STRING, R2dbcType.VARCHAR);
    R2DBC_TYPES.put(TSDataType.DATE, R2dbcType.DATE);
    R2DBC_TYPES.put(TSDataType.TIMESTAMP, R2dbcType.TIMESTAMP);
    R2DBC_TYPES.put(TSDataType.BLOB, R2dbcType.VARBINARY);
    // OBJECT cells carry a file reference; expose its size description like the RPC client.
    R2DBC_TYPES.put(TSDataType.OBJECT, R2dbcType.VARCHAR);
  }

  private IoTDBTypeMapper() {}

  /**
   * Returns the R2DBC type reported for an IoTDB column.
   *
   * <p>The mapping follows the documented driver contract: {@code FLOAT} is {@code REAL}/{@code
   * Float}, {@code TIMESTAMP} is {@code TIMESTAMP}/{@code LocalDateTime} and binary data defaults
   * to {@code VARBINARY}/{@code ByteBuffer}.
   */
  public static R2dbcType toR2dbcType(TSDataType dataType) {
    R2dbcType type = R2DBC_TYPES.get(dataType);
    if (type == null) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_UNSUPPORTED_IOTDB_DATA_TYPE_ARG_69C453DA, dataType));
    }
    return type;
  }

  /** Parses the type name reported by the server in {@code TSExecuteStatementResp}. */
  public static TSDataType parse(String typeName) {
    try {
      return TSDataType.valueOf(typeName);
    } catch (IllegalArgumentException e) {
      throw new IoTDBR2dbcException(
          String.format(R2dbcMessages.EXCEPTION_UNSUPPORTED_IOTDB_DATA_TYPE_ARG_69C453DA, typeName),
          e);
    }
  }
}
