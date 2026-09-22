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

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ParameterBindingTest {

  @Test
  public void mapsSupportedValuesToInternalJdbcTypes() {
    assertEquals(Types.BOOLEAN, ParameterBinding.of(true).jdbcType());
    assertEquals(Types.INTEGER, ParameterBinding.of(1).jdbcType());
    assertEquals(Types.BIGINT, ParameterBinding.of(1L).jdbcType());
    assertEquals(Types.FLOAT, ParameterBinding.of(1.0F).jdbcType());
    assertEquals(Types.DOUBLE, ParameterBinding.of(1.0D).jdbcType());
    assertEquals(Types.VARCHAR, ParameterBinding.of("text").jdbcType());
    assertEquals(Types.VARCHAR, ParameterBinding.nullOf(String.class).jdbcType());
  }

  @Test
  public void onlyVerifiedTypesUseTheTypedPath() {
    assertTrue(ParameterBinding.of("text").supportsTypedEncoding());
    assertTrue(ParameterBinding.of(1).supportsTypedEncoding());
    assertFalse(ParameterBinding.of(new BigDecimal("1.25")).supportsTypedEncoding());
  }

  @Test
  public void rendersSqlLiterals() {
    assertEquals("'o''brien'", literal(ParameterBinding.of("o'brien")));
    assertEquals("42", literal(ParameterBinding.of(42)));
    assertEquals("42", literal(ParameterBinding.of(42L)));
    assertEquals("1.5", literal(ParameterBinding.of(1.5D)));
    assertEquals("1.25", literal(ParameterBinding.of(new BigDecimal("1.25"))));
    assertEquals("TRUE", literal(ParameterBinding.of(true)));
    assertEquals("NULL", literal(ParameterBinding.nullOf(Integer.class)));
  }

  @Test
  public void nullValuesCarryTheDeclaredType() {
    ParameterBinding binding = ParameterBinding.nullOf(Integer.class);
    assertTrue(binding.isNull());
    assertNull(binding.encodedValue());
    assertEquals(Types.INTEGER, binding.jdbcType());
  }

  @Test
  public void rejectsNullValueForBind() {
    assertThrows(IllegalArgumentException.class, () -> ParameterBinding.of(null));
  }

  @Test
  public void rejectsUnsupportedValueAndDeclaredTypes() {
    assertThrows(IoTDBR2dbcException.class, () -> ParameterBinding.of(LocalDate.of(2026, 9, 22)));
    assertThrows(IoTDBR2dbcException.class, () -> ParameterBinding.of(new byte[] {1, 2}));
    assertThrows(IoTDBR2dbcException.class, () -> ParameterBinding.nullOf(LocalDate.class));
    assertThrows(IllegalArgumentException.class, () -> ParameterBinding.nullOf(null));
  }

  @Test
  public void rejectsNaNAsLiteral() {
    assertThrows(IoTDBR2dbcException.class, () -> literal(ParameterBinding.of(Double.NaN)));
  }

  private static String literal(ParameterBinding binding) {
    StringBuilder sql = new StringBuilder();
    binding.appendLiteral(sql);
    return sql.toString();
  }
}
