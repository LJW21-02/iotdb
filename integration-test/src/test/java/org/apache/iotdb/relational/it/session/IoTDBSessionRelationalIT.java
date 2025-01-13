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
package org.apache.iotdb.relational.it.session;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.record.Tablet.ColumnCategory;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBSessionRelationalIT {

  @BeforeClass
  public static void classSetUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @Before
  public void setUp() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db1");
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db2");
      session.executeNonQueryStatement("CREATE DATABASE IF NOT EXISTS db3");
    }
  }

  @After
  public void tearDown() throws Exception {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS db1");
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS db2");
      session.executeNonQueryStatement("DROP DATABASE IF EXISTS db3");
    }
  }

  @AfterClass
  public static void classTearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // for manual debugging
  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("CREATE DATABASE \"db1\"");
      session.executeNonQueryStatement("CREATE DATABASE \"db2\"");
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table10 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");
    }
    // insert without db
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      long timestamp;

      // no db in session and sql
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table10 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                      0, "tag:" + 0, "attr:" + 0, 0 * 1.0)));

      // specify db in sql
      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from db1.table10 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  private static void insertRelationalTabletPerformanceTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table1 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table1",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table1 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
      }
    }
  }

  @Test
  public void insertRelationalSqlTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table1 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      long timestamp;

      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      // without specifying column name
      for (long row = 30; row < 40; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      // auto data type conversion
      for (long row = 40; row < 50; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table1 VALUES (%d, '%s', '%s', %d)",
                row, "tag:" + row, "attr:" + row, row));
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table1 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(50, cnt);

      // sql cannot create column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 (tag1, tag2, attr1, m1) VALUES ('%s', '%s', '%s', %f)",
                      "tag:" + 100, "tag:" + 100, "attr:" + 100, 100 * 1.0)));

      // fewer columns than defined
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ( '%s', %f)", "attr:" + 100, 100 * 1.0)));

      // more columns than defined
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%s', '%s', '%s', '%s', %f)",
                      "tag:" + 100, "tag:" + 100, "tag:" + 100, "attr:" + 100, 100 * 1.0)));

      // Invalid conversion - tag column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%d', '%s', '%s', %f)",
                      100, 100, "attr:" + 100, 100 * 1.0)));

      // Invalid conversion - attr column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%d', '%s', '%s', %f)",
                      100, "tag:" + 100, 100, 100 * 1.0)));

      // Invalid conversion - field column
      assertThrows(
          StatementExecutionException.class,
          () ->
              session.executeNonQueryStatement(
                  String.format(
                      "INSERT INTO table1 VALUES ('%d', '%s', '%s', %s)",
                      100, "tag:" + 100, "attr:" + 100, "field" + (100 * 1.0))));
    }
  }

  @Test
  public void partialInsertSQLTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // disable auto-creation only for this test
      session.executeNonQueryStatement("SET CONFIGURATION \"enable_auto_create_schema\"=\"false\"");
    }
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // the table is missing column "m2"
      session.executeNonQueryStatement(
          "CREATE TABLE table2_2 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");
      try {
        session.executeNonQueryStatement(
            "INSERT INTO table2_2 (time, tag1, attr1, m1, m2) values (1, '1', '1', 1.0, 2.0)");
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals(
            "616: Unknown column category for m2. Cannot auto create column.", e.getMessage());
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement(
            "SET CONFIGURATION \"enable_auto_create_schema\"=\"true\"");
      }
    }
  }

  @Test
  public void partialInsertRelationalTabletTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      // disable auto-creation only for this test
      session.executeNonQueryStatement("SET CONFIGURATION \"enable_auto_create_schema\"=\"false\"");
    }
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // the table is missing column "m2"
      session.executeNonQueryStatement(
          "CREATE TABLE table4 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      // the insertion contains "m2"
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("m2", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.TAG,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.FIELD,
              ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table4",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        tablet.addValue("m2", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          try {
            session.insert(tablet);
          } catch (StatementExecutionException e) {
            // a partial insertion should be reported
            assertEquals(
                "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]",
                e.getMessage());
          }
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        try {
          session.insert(tablet);
        } catch (StatementExecutionException e) {
          if (!e.getMessage()
              .equals(
                  "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]")) {
            throw e;
          }
        }
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        tablet.addValue("m2", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          try {
            session.insert(tablet);
          } catch (StatementExecutionException e) {
            if (!e.getMessage()
                .equals(
                    "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]")) {
              throw e;
            }
          }
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        try {
          session.insert(tablet);
        } catch (StatementExecutionException e) {
          if (!e.getMessage()
              .equals(
                  "507: Fail to insert measurements [m2] caused by [Column m2 does not exists or fails to be created]")) {
            throw e;
          }
        }
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table4 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        // "m2" should not be present
        assertEquals(4, rowRecord.getFields().size());
        cnt++;
      }
      assertEquals(30, cnt);

      // partial insert is disabled
      session.executeNonQueryStatement("SET CONFIGURATION enable_partial_insert='false'");
      int rowIndex = 0;
      tablet.addTimestamp(rowIndex, timestamp + rowIndex);
      tablet.addValue("tag1", rowIndex, "tag:" + rowIndex);
      tablet.addValue("attr1", rowIndex, "attr:" + rowIndex);
      tablet.addValue("m1", rowIndex, rowIndex * 1.0);
      tablet.addValue("m2", rowIndex, rowIndex * 1.0);
      try {
        session.insert(tablet);
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("616: Missing columns [m2].", e.getMessage());
      }

    } finally {
      try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement("SET CONFIGURATION \"enable_partial_insert\"=\"true\"");
        session.executeNonQueryStatement(
            "SET CONFIGURATION \"enable_auto_create_schema\"=\"true\"");
      }
    }
  }

  @Test
  public void insertRelationalTabletTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table5 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table5",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      int cnt = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table5 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void insertRelationalTabletWithCacheLeaderTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table5 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table5",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        // cache leader should work for devices that have inserted before
        tablet.addValue("tag1", rowIndex, "tag:" + (row - 15));
        tablet.addValue("attr1", rowIndex, "attr:" + (row - 15));
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      int cnt = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table5 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals(
            "tag:" + (timestamp < 15 ? timestamp : timestamp - 15),
            rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals(
            "attr:" + (timestamp < 15 ? timestamp : timestamp - 15),
            rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void autoCreateNontagColumnTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // only one column in this table, and others should be auto-created
      session.executeNonQueryStatement("CREATE TABLE table7 (tag1 string tag)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table7",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
        if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
          session.insert(tablet);
          tablet.reset();
        }
      }

      if (tablet.getRowSize() != 0) {
        session.insert(tablet);
        tablet.reset();
      }

      SessionDataSet dataSet = session.executeQueryStatement("select * from table7 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void autoCreateTableTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // no table created here

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table6",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (int row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLush");

      for (int row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag1", rowIndex, "tag:" + row);
        tablet.addValue("attr1", rowIndex, "attr:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      int cnt = 0;
      SessionDataSet dataSet = session.executeQueryStatement("select * from table6 order by time");
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void autoCreateTagColumnTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // only one column in this table, and others should be auto-created
      session.executeNonQueryStatement("CREATE TABLE table8 (tag1 string tag)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag2", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.ATTRIBUTE, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table8",
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (int row = 0; row < 15; row++) {
        tablet.addTimestamp(row, timestamp);
        tablet.addValue("tag2", row, "tag:" + timestamp);
        tablet.addValue("attr1", row, "attr:" + timestamp);
        tablet.addValue("m1", row, timestamp * 1.0);
        timestamp++;
      }

      session.insert(tablet);
      tablet.reset();

      SessionDataSet dataSet = session.executeQueryStatement("select * from table8 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        long t = rowRecord.getFields().get(0).getLongV();
        // tag 1 should be null
        assertNull(rowRecord.getFields().get(1).getDataType());
        assertEquals("tag:" + t, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals("attr:" + t, rowRecord.getFields().get(3).getBinaryV().toString());
        assertEquals(t * 1.0, rowRecord.getFields().get(4).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(15, cnt);

      session.executeNonQueryStatement("FLush");

      for (int row = 0; row < 15; row++) {
        tablet.addTimestamp(row, timestamp);
        tablet.addValue("tag2", row, "tag:" + timestamp);
        tablet.addValue("attr1", row, "attr:" + timestamp);
        tablet.addValue("m1", row, timestamp * 1.0);
        timestamp++;
      }

      session.insert(tablet);
      tablet.reset();

      dataSet = session.executeQueryStatement("select * from table8 order by time");
      cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        long t = rowRecord.getFields().get(0).getLongV();
        // tag 1 should be null
        assertNull(rowRecord.getFields().get(1).getDataType());
        assertEquals("tag:" + t, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals("attr:" + t, rowRecord.getFields().get(3).getBinaryV().toString());
        assertEquals(t * 1.0, rowRecord.getFields().get(4).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void autoAdjustTagTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // the tag order in the table is (tag1, tag2)
      session.executeNonQueryStatement(
          "CREATE TABLE table9 (tag1 string tag, tag2 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");

      // the tag order in the row is (tag2, tag1)
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag2", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("attr1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", TSDataType.DOUBLE));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(
              ColumnCategory.TAG,
              ColumnCategory.TAG,
              ColumnCategory.ATTRIBUTE,
              ColumnCategory.FIELD);
      List<String> fieldtags = IMeasurementSchema.getMeasurementNameList(schemaList);
      List<TSDataType> dataTypes = IMeasurementSchema.getDataTypeList(schemaList);

      long timestamp = 0;
      Tablet tablet = new Tablet("table9", fieldtags, dataTypes, columnTypes, 15);

      for (long row = 0; row < 15; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag2", rowIndex, "tag2:" + row);
        tablet.addValue("tag1", rowIndex, "tag1:" + row);
        tablet.addValue("attr1", rowIndex, "attr1:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        int rowIndex = tablet.getRowSize();
        tablet.addTimestamp(rowIndex, timestamp + row);
        tablet.addValue("tag2", rowIndex, "tag2:" + row);
        tablet.addValue("tag1", rowIndex, "tag1:" + row);
        tablet.addValue("attr1", rowIndex, "attr1:" + row);
        tablet.addValue("m1", rowIndex, row * 1.0);
      }
      session.insert(tablet);
      tablet.reset();

      SessionDataSet dataSet = session.executeQueryStatement("select * from table9 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag1:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("tag2:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals("attr1:" + timestamp, rowRecord.getFields().get(3).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(4).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void insertRelationalSqlWithoutDBTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table10 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");
    }
    // insert without db
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      long timestamp;

      // no db in session and sql
      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table10 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "tag:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("701: database is not specified", e.getMessage());
      }

      // specify db in sql
      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table10 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from db1.table10 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void insertRelationalSqlAnotherDBTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement(
          "CREATE TABLE table11 (tag1 string tag, attr1 string attribute, "
              + "m1 double "
              + "field)");
    }
    // use db2 but insert db1
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      long timestamp;
      session.executeNonQueryStatement("USE \"db2\"");

      // specify db in sql
      for (long row = 0; row < 15; row++) {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db1.table11 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      session.executeNonQueryStatement("FLush");

      for (long row = 15; row < 30; row++) {
        // check case sensitivity
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO DB1.TaBle11 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                row, "tag:" + row, "attr:" + row, row * 1.0));
      }

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from db1.table11 order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        timestamp = rowRecord.getFields().get(0).getLongV();
        assertEquals("tag:" + timestamp, rowRecord.getFields().get(1).getBinaryV().toString());
        assertEquals("attr:" + timestamp, rowRecord.getFields().get(2).getBinaryV().toString());
        assertEquals(timestamp * 1.0, rowRecord.getFields().get(3).getDoubleV(), 0.0001);
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }

  @Test
  public void insertNonExistTableTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");

      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO table13 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "tag:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("550: Table 'db1.table13' does not exist.", e.getMessage());
      }

      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db2.table13 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "tag:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("550: Table 'db2.table13' does not exist.", e.getMessage());
      }
    }
  }

  @Test
  public void insertNonExistDBTest() throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");

      try {
        session.executeNonQueryStatement(
            String.format(
                "INSERT INTO db3.table13 (time, tag1, attr1, m1) VALUES (%d, '%s', '%s', %f)",
                0, "tag:" + 0, "attr:" + 0, 0 * 1.0));
        fail("Exception expected");
      } catch (StatementExecutionException e) {
        assertEquals("550: Table 'db3.table13' does not exist.", e.getMessage());
      }
    }
  }

  @Test
  public void insertWithoutMeasurementTest()
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      session.executeNonQueryStatement("create table tb (a string tag, b string field)");
      session.executeNonQueryStatement("insert into tb(a) values ('w')");
      SessionDataSet dataSet = session.executeQueryStatement("select * from tb");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals("w", rowRecord.getFields().get(1).getBinaryV().toString());
        assertNull(rowRecord.getFields().get(2).getDataType());
        cnt++;
      }
      assertEquals(1, cnt);

      session.executeNonQueryStatement("flush");

      dataSet = session.executeQueryStatement("select * from tb");
      cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        assertEquals("w", rowRecord.getFields().get(1).getBinaryV().toString());
        assertNull(rowRecord.getFields().get(2).getDataType());
        cnt++;
      }
      assertEquals(1, cnt);
    }
  }

  private void testOneCastWithTablet(
      TSDataType from, TSDataType to, int testNum, boolean partialInsert)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // create a column with type of "to"
      session.executeNonQueryStatement(
          "CREATE TABLE table"
              + testNum
              + " (tag1 string tag,"
              + "m1 "
              + to.toString()
              + " field)");
      if (partialInsert) {
        session.executeNonQueryStatement("SET CONFIGURATION enable_partial_insert='true'");
      } else {
        session.executeNonQueryStatement("SET CONFIGURATION enable_partial_insert='false'");
      }

      // insert a tablet with type of "from"
      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag1", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("m1", from));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);
      Tablet tablet =
          new Tablet(
              "table" + testNum,
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      tablet.addTimestamp(0, 0);
      tablet.addTimestamp(1, 1);
      tablet.addValue(0, 0, "d1");
      tablet.addValue(1, 0, "d1");
      // the field in the first row is null
      tablet.addValue("m1", 1, genValue(from, 1));
      if (to.isCompatible(from)) {
        // can cast, insert and check the result
        session.insert(tablet);
        // time, tag1, m1
        SessionDataSet dataSet =
            session.executeQueryStatement("select * from table" + testNum + " order by time");
        RowRecord rec = dataSet.next();
        assertEquals(0, rec.getFields().get(0).getLongV());
        assertEquals("d1", rec.getFields().get(1).toString());
        assertNull(rec.getFields().get(2).getDataType());
        rec = dataSet.next();
        assertEquals(1, rec.getFields().get(0).getLongV());
        assertEquals("d1", rec.getFields().get(1).toString());
        if (to == TSDataType.BLOB) {
          assertEquals(genValue(to, 1), rec.getFields().get(2).getBinaryV());
        } else if (to == TSDataType.DATE) {
          assertEquals(genValue(to, 1), rec.getFields().get(2).getDateV());
        } else {
          assertEquals(genValue(to, 1).toString(), rec.getFields().get(2).toString());
        }
        assertFalse(dataSet.hasNext());
      } else {
        if (partialInsert) {
          // cannot cast, but partial insert
          try {
            session.insert(tablet);
            fail("Exception expected: from=" + from + ", to=" + to);
          } catch (StatementExecutionException e) {
            assertEquals(
                "507: Fail to insert measurements [m1] caused by [Incompatible data type of column m1: "
                    + from
                    + "/"
                    + to
                    + "]",
                e.getMessage());
          }
          // time, tag1, m1
          SessionDataSet dataSet =
              session.executeQueryStatement("select * from table" + testNum + " order by time");
          RowRecord rec = dataSet.next();
          assertEquals(0, rec.getFields().get(0).getLongV());
          assertEquals("d1", rec.getFields().get(1).toString());
          assertNull(rec.getFields().get(2).getDataType());
          rec = dataSet.next();
          assertEquals(1, rec.getFields().get(0).getLongV());
          assertEquals("d1", rec.getFields().get(1).toString());
          assertNull(rec.getFields().get(2).getDataType());
          assertFalse(dataSet.hasNext());
        } else {
          // cannot cast, expect an exception
          try {
            session.insert(tablet);
            fail("Exception expected");
          } catch (StatementExecutionException e) {
            assertEquals(
                "614: Incompatible data type of column m1: " + from + "/" + to, e.getMessage());
          }
        }
      }

      session.executeNonQueryStatement("DROP TABLE table" + testNum);
    }
  }

  private void testOneCastWithRow(
      TSDataType from, TSDataType to, int testNum, boolean partialInsert)
      throws IoTDBConnectionException, StatementExecutionException {
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // create a column with type of "to"
      session.executeNonQueryStatement(
          "CREATE TABLE table"
              + testNum
              + " (tag1 string tag,"
              + "m1 "
              + to.toString()
              + " field)");
      if (partialInsert) {
        session.executeNonQueryStatement("SET CONFIGURATION enable_partial_insert='true'");
      } else {
        session.executeNonQueryStatement("SET CONFIGURATION enable_partial_insert='false'");
      }

      // insert a tablet with type of "from"
      String sql =
          String.format(
              "INSERT INTO table"
                  + testNum
                  + " (time, tag1, m1) VALUES (0, 'd1', null),(1,'d1', %s)",
              genValue(from, 1));
      if (to.isCompatible(from)) {
        // can cast, insert and check the result
        session.executeNonQueryStatement(sql);
        // time, tag1, m1
        SessionDataSet dataSet =
            session.executeQueryStatement("select * from table" + testNum + " order by time");
        RowRecord rec = dataSet.next();
        assertEquals(0, rec.getFields().get(0).getLongV());
        assertEquals("d1", rec.getFields().get(1).toString());
        assertNull(rec.getFields().get(2).getDataType());
        rec = dataSet.next();
        assertEquals(1, rec.getFields().get(0).getLongV());
        assertEquals("d1", rec.getFields().get(1).toString());
        if (to == TSDataType.BLOB) {
          assertEquals(genValue(to, 1), rec.getFields().get(2).getBinaryV());
        } else if (to == TSDataType.DATE) {
          assertEquals(genValue(to, 1), rec.getFields().get(2).getDateV());
        } else {
          assertEquals(genValue(to, 1).toString(), rec.getFields().get(2).toString());
        }
        assertFalse(dataSet.hasNext());
      } else {
        if (partialInsert) {
          // cannot cast, but partial insert
          try {
            session.executeNonQueryStatement(sql);
            fail("Exception expected: from=" + from + ", to=" + to);
          } catch (StatementExecutionException e) {
            assertEquals(
                "507: Fail to insert measurements [m1] caused by [Incompatible data type of column m1: "
                    + from
                    + "/"
                    + to
                    + "]",
                e.getMessage());
          }
          // time, tag1, m1
          SessionDataSet dataSet =
              session.executeQueryStatement("select * from table" + testNum + " order by time");
          RowRecord rec = dataSet.next();
          assertEquals(0, rec.getFields().get(0).getLongV());
          assertEquals("d1", rec.getFields().get(1).toString());
          assertNull(rec.getFields().get(2).getDataType());
          rec = dataSet.next();
          assertEquals(1, rec.getFields().get(0).getLongV());
          assertEquals("d1", rec.getFields().get(1).toString());
          assertNull(rec.getFields().get(2).getDataType());
          assertFalse(dataSet.hasNext());
        } else {
          // cannot cast, expect an exception
          try {
            session.executeNonQueryStatement(sql);
            fail("Exception expected");
          } catch (StatementExecutionException e) {
            assertEquals(
                "614: Incompatible data type of column m1: " + from + "/" + to, e.getMessage());
          }
        }
      }

      session.executeNonQueryStatement("DROP TABLE table" + testNum);
    }
  }

  @SuppressWarnings("SameParameterValue")
  private Object genValue(TSDataType dataType, int i) {
    switch (dataType) {
      case INT32:
        return i;
      case DATE:
        return LocalDate.ofEpochDay(i);
      case TIMESTAMP:
      case INT64:
        return (long) i;
      case BOOLEAN:
        return i % 2 == 0;
      case FLOAT:
        return i * 1.0f;
      case DOUBLE:
        return i * 1.0;
      case STRING:
      case TEXT:
      case BLOB:
        return new Binary(Integer.toString(i), StandardCharsets.UTF_8);
      case UNKNOWN:
      case VECTOR:
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }

  @Test
  public void insertRelationalTabletWithAutoCastTest()
      throws IoTDBConnectionException, StatementExecutionException {
    int testNum = 14;
    Set<TSDataType> dataTypes = new HashSet<>();
    Collections.addAll(dataTypes, TSDataType.values());
    dataTypes.remove(TSDataType.VECTOR);
    dataTypes.remove(TSDataType.UNKNOWN);

    for (TSDataType from : dataTypes) {
      for (TSDataType to : dataTypes) {
        System.out.println("from: " + from + ", to: " + to);
        testOneCastWithTablet(from, to, testNum, false);
        System.out.println("partial insert");
        testOneCastWithTablet(from, to, testNum, true);
      }
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION \"enable_partial_insert\"=\"true\"");
    }
  }

  @Test
  public void deleteTableAndWriteDifferentTypeTest()
      throws IoTDBConnectionException, StatementExecutionException {
    int testNum = 15;
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE db1");

      session.executeNonQueryStatement(
          "CREATE TABLE table" + testNum + " (tag1 string tag, m1 int32 field)");
      session.executeNonQueryStatement(
          "INSERT INTO table" + testNum + " (time, tag1, m1) VALUES (1, 'd1', 1)");

      session.executeNonQueryStatement("DROP TABLE table" + testNum);

      session.executeNonQueryStatement(
          "CREATE TABLE table" + testNum + " (tag1 string tag, m1 double field)");
      session.executeNonQueryStatement(
          "INSERT INTO table" + testNum + " (time, tag1, m1) VALUES (2, 'd2', 2)");

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from table" + testNum + " order by time");
      RowRecord rec = dataSet.next();
      assertEquals(2, rec.getFields().get(0).getLongV());
      assertEquals("d2", rec.getFields().get(1).toString());
      assertEquals(2.0, rec.getFields().get(2).getDoubleV(), 0.1);
      assertFalse(dataSet.hasNext());
    }
  }

  @Test
  public void dropTableOfTheSameNameTest()
      throws IoTDBConnectionException, StatementExecutionException {
    int testNum = 16;
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE db1");

      session.executeNonQueryStatement(
          "CREATE TABLE db1.table" + testNum + " (tag1 string tag, m1 int32 field)");
      session.executeNonQueryStatement(
          "INSERT INTO db1.table" + testNum + " (time, tag1, m1) VALUES (1, 'd1', 1)");

      session.executeNonQueryStatement(
          "CREATE TABLE db2.table" + testNum + " (tag1 string tag, m1 double field)");
      session.executeNonQueryStatement(
          "INSERT INTO db2.table" + testNum + " (time, tag1, m1) VALUES (2, 'd2', 2)");

      session.executeNonQueryStatement("DROP TABLE db2.table" + testNum);

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from db1.table" + testNum + " order by time");
      RowRecord rec = dataSet.next();
      assertEquals(1, rec.getFields().get(0).getLongV());
      assertEquals("d1", rec.getFields().get(1).toString());
      assertEquals(1, rec.getFields().get(2).getIntV());
      assertFalse(dataSet.hasNext());

      try {
        session.executeQueryStatement("select * from db2.table" + testNum + " order by time");
        fail("expected exception");
      } catch (StatementExecutionException e) {
        assertEquals("550: Table 'db2.table16' does not exist.", e.getMessage());
      }
    }
  }

  @Test
  public void insertRelationalRowWithAutoCastTest()
      throws IoTDBConnectionException, StatementExecutionException {
    int testNum = 17;
    Set<TSDataType> dataTypes = new HashSet<>();
    Collections.addAll(dataTypes, TSDataType.values());
    dataTypes.remove(TSDataType.VECTOR);
    dataTypes.remove(TSDataType.UNKNOWN);

    for (TSDataType from : dataTypes) {
      for (TSDataType to : dataTypes) {
        System.out.println("from: " + from + ", to: " + to);
        testOneCastWithTablet(from, to, testNum, false);
        System.out.println("partial insert");
        testOneCastWithTablet(from, to, testNum, true);
      }
    }

    try (ISession session = EnvFactory.getEnv().getSessionConnection()) {
      session.executeNonQueryStatement("SET CONFIGURATION \"enable_partial_insert\"=\"true\"");
    }
  }

  @Test
  public void autoCreateTagColumnTest2()
      throws IoTDBConnectionException, StatementExecutionException {
    int testNum = 18;
    try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
      session.executeNonQueryStatement("USE \"db1\"");
      // only one column in this table, and others should be auto-created
      session.executeNonQueryStatement(
          "CREATE TABLE table" + testNum + " (tag1 string tag, s1 text field)");

      List<IMeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("tag2", TSDataType.STRING));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      final List<ColumnCategory> columnTypes =
          Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

      long timestamp = 0;
      Tablet tablet =
          new Tablet(
              "table" + testNum,
              IMeasurementSchema.getMeasurementNameList(schemaList),
              IMeasurementSchema.getDataTypeList(schemaList),
              columnTypes,
              15);

      for (int row = 0; row < 15; row++) {
        tablet.addTimestamp(row, timestamp);
        tablet.addValue("tag2", row, "string");
        tablet.addValue("s2", row, timestamp);
        timestamp++;
      }

      session.insert(tablet);
      tablet.reset();

      SessionDataSet dataSet =
          session.executeQueryStatement("select * from table" + testNum + " order by time");
      int cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        long t = rowRecord.getFields().get(0).getLongV();
        // tag 1 should be null
        assertNull(rowRecord.getFields().get(1).getDataType());
        // s1 should be null
        assertNull(rowRecord.getFields().get(2).getDataType());
        assertEquals("string", rowRecord.getFields().get(3).getBinaryV().toString());
        assertEquals(t, rowRecord.getFields().get(4).getLongV());
        cnt++;
      }
      assertEquals(15, cnt);

      session.executeNonQueryStatement("FLush");

      for (int row = 0; row < 15; row++) {
        tablet.addTimestamp(row, timestamp);
        tablet.addValue("tag2", row, "string");
        tablet.addValue("s2", row, timestamp);
        timestamp++;
      }

      session.insert(tablet);
      tablet.reset();

      dataSet = session.executeQueryStatement("select * from table" + testNum + " order by time");
      cnt = 0;
      while (dataSet.hasNext()) {
        RowRecord rowRecord = dataSet.next();
        long t = rowRecord.getFields().get(0).getLongV();
        // tag 1 should be null
        assertNull(rowRecord.getFields().get(1).getDataType());
        // s1 should be null
        assertNull(rowRecord.getFields().get(2).getDataType());
        assertEquals("string", rowRecord.getFields().get(3).getBinaryV().toString());
        assertEquals(t, rowRecord.getFields().get(4).getLongV());
        cnt++;
      }
      assertEquals(30, cnt);
    }
  }
}