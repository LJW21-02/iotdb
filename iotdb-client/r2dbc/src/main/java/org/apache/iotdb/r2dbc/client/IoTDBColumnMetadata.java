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

import org.apache.iotdb.r2dbc.codec.IoTDBTypeMapper;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import org.apache.tsfile.enums.TSDataType;

/** Column description derived from the executed statement response. */
public final class IoTDBColumnMetadata implements ColumnMetadata {

  private final String name;
  private final TSDataType dataType;
  private final R2dbcType r2dbcType;

  public IoTDBColumnMetadata(String name, TSDataType dataType) {
    this.name = name;
    this.dataType = dataType;
    this.r2dbcType = IoTDBTypeMapper.toR2dbcType(dataType);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Type getType() {
    return r2dbcType;
  }

  @Override
  public Class<?> getJavaType() {
    return r2dbcType.getJavaType();
  }

  @Override
  public Object getNativeTypeMetadata() {
    return dataType;
  }

  @Override
  public String toString() {
    return "IoTDBColumnMetadata{name='" + name + "', dataType=" + dataType + '}';
  }
}
