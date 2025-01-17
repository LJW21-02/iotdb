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

package org.apache.iotdb.jdbc.relational;

import org.apache.iotdb.jdbc.Field;
import org.apache.iotdb.jdbc.IoTDBAbstractDatabaseMetadata;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBJDBCResultSet;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBRelationalDatabaseMetadata extends IoTDBAbstractDatabaseMetadata {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBRelationalDatabaseMetadata.class);

  private static final String DATABASE_VERSION =
      IoTDBRelationalDatabaseMetadata.class.getPackage().getImplementationVersion() != null
          ? IoTDBRelationalDatabaseMetadata.class.getPackage().getImplementationVersion()
          : "UNKNOWN";

  public static final String SHOW_TABLES_ERROR_MSG = "Show tables error: {}";

  public IoTDBRelationalDatabaseMetadata(
      IoTDBConnection connection, IClientRPCService.Iface client, long sessionId, ZoneId zoneId) {
    super(connection, client, sessionId, zoneId);
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return DATABASE_VERSION;
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    LOGGER.info(
        "GetTables:: catalog:{}, schemaPattern:{}, tableNamePattern:{}, types:{}",
        catalog,
        schemaPattern,
        tableNamePattern,
        types);

    Statement stmt = this.connection.createStatement();

    ResultSet rs;
    String sql = "SHOW TABLES IN " + schemaPattern;
    try {
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      stmt.close();
      LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());
      throw e;
    }

    // Setup Fields
    Field[] fields = new Field[3];
    fields[0] = new Field("", TABLE_NAME, "TEXT");
    fields[1] = new Field("", TABLE_TYPE, "TEXT");
    fields[2] = new Field("", REMARKS, "TEXT");
    List<TSDataType> tsDataTypeList =
        Arrays.asList(TSDataType.TEXT, TSDataType.TEXT, TSDataType.TEXT);
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    // Extract Values
    boolean hasResultSet = false;
    while (rs.next()) {
      hasResultSet = true;
      List<Object> valueInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i == 0) {
          valueInRow.add(rs.getString(1));
        } else if (i == 1) {
          valueInRow.add("TABLE");
        } else {
          valueInRow.add("TTL(ms): " + rs.getString(2));
        }
      }
      valuesList.add(valueInRow);
    }

    // Convert Values to ByteBuffer
    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      LOGGER.error(CONVERT_ERROR_MSG, e.getMessage());
    } finally {
      close(rs, stmt);
    }

    return hasResultSet
        ? new IoTDBJDBCResultSet(
            stmt,
            columnNameList,
            columnTypeList,
            columnNameIndex,
            true,
            client,
            null,
            -1,
            sessionId,
            Collections.singletonList(tsBlock),
            null,
            (long) 60 * 1000,
            false,
            zoneId)
        : null;
  }

  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {

    LOGGER.info(
        "GetColumns:: catalog:{}, schemaPattern:{}, tableNamePattern:{}, columnNamePattern:{}",
        catalog,
        schemaPattern,
        tableNamePattern,
        columnNamePattern);

    Statement stmt = this.connection.createStatement();
    ResultSet rs;

    // Get Table Metadata
    String sql = "DESC " + schemaPattern + "." + tableNamePattern;
    try {
      rs = stmt.executeQuery(sql);
    } catch (SQLException e) {
      stmt.close();
      LOGGER.error(SHOW_TABLES_ERROR_MSG, e.getMessage());
      throw e;
    }

    // Setup Fields
    Field[] fields = new Field[8];
    fields[0] = new Field("", COLUMN_NAME, "TEXT");
    fields[1] = new Field("", ORDINAL_POSITION, INT32);
    fields[2] = new Field("", DATA_TYPE, INT32);
    fields[3] = new Field("", REMARKS, "TEXT");
    fields[4] = new Field("", TYPE_NAME, "TEXT");
    fields[5] = new Field("", IS_AUTOINCREMENT, "TEXT");
    fields[6] = new Field("", IS_NULLABLE, "TEXT");
    fields[7] = new Field("", NULLABLE, INT32);
    List<TSDataType> tsDataTypeList =
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.INT32,
            TSDataType.INT32,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.INT32);
    List<String> columnNameList = new ArrayList<>();
    List<String> columnTypeList = new ArrayList<>();
    Map<String, Integer> columnNameIndex = new HashMap<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (int i = 0; i < fields.length; i++) {
      columnNameList.add(fields[i].getName());
      columnTypeList.add(fields[i].getSqlType());
      columnNameIndex.put(fields[i].getName(), i);
    }

    // Extract Metadata
    int count = 1;
    while (rs.next()) {
      String columnName = rs.getString(1);
      String type = rs.getString(2);
      List<Object> valueInRow = new ArrayList<>();
      for (int i = 0; i < fields.length; i++) {
        if (i == 0) {
          valueInRow.add(columnName);
        } else if (i == 1) {
          valueInRow.add(count++);
        } else if (i == 2) {
          valueInRow.add(getSQLType(type));
        } else if (i == 3) {
          String columnCategory = rs.getString(3);
          if (columnCategory.equals("ID")) {
            columnCategory = "TAG";
          } else if (columnCategory.equals("MEASUREMENT")) {
            columnCategory = "FIELD";
          }
          valueInRow.add("ColumnCategory: " + columnCategory);
        } else if (i == 4) {
          valueInRow.add(type);
        } else if (i == 5) {
          valueInRow.add("");
        } else {
          if (!columnName.equals("time")) {
            valueInRow.add("YES");
            valueInRow.add(ResultSetMetaData.columnNullableUnknown);
          } else {
            valueInRow.add("NO");
            valueInRow.add(ResultSetMetaData.columnNoNulls);
          }
          break;
        }
      }
      valuesList.add(valueInRow);
    }

    // Convert Values to ByteBuffer
    ByteBuffer tsBlock = null;
    try {
      tsBlock = convertTsBlock(valuesList, tsDataTypeList);
    } catch (IOException e) {
      LOGGER.error(CONVERT_ERROR_MSG, e.getMessage());
    } finally {
      close(rs, stmt);
    }

    return new IoTDBJDBCResultSet(
        stmt,
        columnNameList,
        columnTypeList,
        columnNameIndex,
        true,
        client,
        null,
        -1,
        sessionId,
        Collections.singletonList(tsBlock),
        null,
        (long) 60 * 1000,
        false,
        zoneId);
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return true;
  }
}
