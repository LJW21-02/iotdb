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

import io.r2dbc.spi.Result;
import org.junit.Test;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertThrows;

public class IoTDBResultTest {

  @Test
  public void emptyResultCompletesWithoutFabricatingAnUpdateCount() {
    StepVerifier.create(IoTDBResult.empty().getRowsUpdated()).verifyComplete();
  }

  @Test
  public void aResultCanBeConsumedOnlyOnce() {
    IoTDBResult result = IoTDBResult.empty();
    result.getRowsUpdated();
    assertThrows(IoTDBR2dbcException.class, () -> result.map((row, metadata) -> row));
  }

  @Test
  public void filterReturnsANewUsableView() {
    IoTDBResult result = IoTDBResult.empty();
    Result filtered = result.filter(segment -> true);
    StepVerifier.create(filtered.map((row, metadata) -> row)).verifyComplete();
    assertThrows(IoTDBR2dbcException.class, () -> result.map((row, metadata) -> row));
  }

  @Test
  public void consumingAFilteredViewAlsoConsumesTheParent() {
    IoTDBResult result = IoTDBResult.empty();
    Result filtered = result.filter(segment -> true);
    filtered.getRowsUpdated();
    assertThrows(IoTDBR2dbcException.class, () -> result.map((row, metadata) -> row));
    assertThrows(IoTDBR2dbcException.class, () -> filtered.map((row, metadata) -> row));
  }
}
