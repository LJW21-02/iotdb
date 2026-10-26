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

import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration.SqlDialect;
import org.apache.iotdb.r2dbc.R2dbcMessages;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class IoTDBStatementTest {

  @Test
  public void addReportsTheFirstMissingIndexWithoutAdvancingTheBindingSet() {
    IoTDBStatement statement =
        new IoTDBStatement(null, "SELECT ?, ?, ? FROM probe", SqlDialect.TABLE);
    statement.bind(2, 30);
    assertMissingParameter(statement, 0);

    statement.bind(0, 10);
    assertMissingParameter(statement, 1);

    statement.bind(1, 20).add();
    statement.bind(2, 60);
    assertMissingParameter(statement, 0);
    statement.bind(0, 40).bind(1, 50);
    assertNotNull(statement.execute());
  }

  private static void assertMissingParameter(IoTDBStatement statement, int index) {
    String expected =
        String.format(R2dbcMessages.EXCEPTION_PARAMETER_ARG_IS_NOT_BOUND_F893971C, index);
    IllegalStateException addFailure = assertThrows(IllegalStateException.class, statement::add);
    IllegalStateException executeFailure =
        assertThrows(IllegalStateException.class, statement::execute);
    assertEquals(expected, addFailure.getMessage());
    assertEquals(expected, executeFailure.getMessage());
    assertFalse(addFailure.getMessage().contains("%d"));
  }
}
