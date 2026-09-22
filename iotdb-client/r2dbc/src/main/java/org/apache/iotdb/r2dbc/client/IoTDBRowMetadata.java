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

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import org.apache.tsfile.enums.TSDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Immutable row description shared by every row of one result. */
public final class IoTDBRowMetadata implements RowMetadata {

  private final List<ColumnMetadata> columns;
  private final Map<String, Integer> indexByName;

  private IoTDBRowMetadata(List<ColumnMetadata> columns, Map<String, Integer> indexByName) {
    this.columns = Collections.unmodifiableList(columns);
    this.indexByName = indexByName;
  }

  /**
   * Creates the metadata for one result.
   *
   * <p>Duplicate column names keep every index; name lookup resolves to the first occurrence, which
   * is the behaviour readers can rely on for {@code SELECT *} style projections.
   */
  public static IoTDBRowMetadata of(List<String> names, List<TSDataType> types) {
    List<ColumnMetadata> columns = new ArrayList<>(names.size());
    Map<String, Integer> indexByName = new HashMap<>();
    for (int i = 0; i < names.size(); i++) {
      columns.add(new IoTDBColumnMetadata(names.get(i), types.get(i)));
      indexByName.putIfAbsent(names.get(i).toLowerCase(Locale.ROOT), i);
    }
    return new IoTDBRowMetadata(columns, indexByName);
  }

  @Override
  public ColumnMetadata getColumnMetadata(int index) {
    return columns.get(index);
  }

  @Override
  public ColumnMetadata getColumnMetadata(String name) {
    Integer index = index(name);
    if (index == null) {
      throw new IoTDBR2dbcException(
          String.format(R2dbcMessages.EXCEPTION_COLUMN_ARG_DOES_NOT_EXIST_FF67AE82, name));
    }
    return columns.get(index);
  }

  @Override
  public List<? extends ColumnMetadata> getColumnMetadatas() {
    return columns;
  }

  /** Returns the column index of a name, or {@code null} if the result has no such column. */
  public Integer index(String name) {
    return indexByName.get(name.toLowerCase(Locale.ROOT));
  }
}
