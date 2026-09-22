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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class LiteralBinderTest {

  private static final String BACKSLASH = Character.toString((char) 92);

  @Test
  public void countsOnlyValuePlaceholders() {
    assertEquals(2, LiteralBinder.placeholderCount("INSERT INTO t(a, b) VALUES (?, ?)"));
    assertEquals(0, LiteralBinder.placeholderCount("SELECT '?' FROM t"));
    assertEquals(0, LiteralBinder.placeholderCount("SELECT \"?\" FROM t"));
    assertEquals(0, LiteralBinder.placeholderCount("SELECT `?` FROM t"));
    assertEquals(1, LiteralBinder.placeholderCount("SELECT a FROM t WHERE a = ? -- ? trailing"));
    assertEquals(1, LiteralBinder.placeholderCount("SELECT a FROM t /* ? */ WHERE a = ?"));
    assertEquals(0, LiteralBinder.placeholderCount("SELECT 'it''s ?' FROM t"));
  }

  @Test
  public void bindsValuesAndEscapesStrings() {
    String bound =
        LiteralBinder.bind(
            "INSERT INTO t(a, b, c) VALUES (?, ?, ?)",
            List.of(
                ParameterBinding.of(42),
                ParameterBinding.of("o'brien"),
                ParameterBinding.of(true)));
    assertEquals("INSERT INTO t(a, b, c) VALUES (42, 'o''brien', TRUE)", bound);
  }

  @Test
  public void bindsNumericAndNullLiterals() {
    String bound =
        LiteralBinder.bind(
            "INSERT INTO t(a, b, c, d) VALUES (?, ?, ?, ?)",
            List.of(
                ParameterBinding.of(1L),
                ParameterBinding.of(1.5D),
                ParameterBinding.of(new BigDecimal("1.25")),
                ParameterBinding.nullOf(String.class)));
    assertEquals("INSERT INTO t(a, b, c, d) VALUES (1, 1.5, 1.25, NULL)", bound);
  }

  @Test
  public void neverRewritesLiteralsIdentifiersOrComments() {
    String sql = "SELECT '?', \"?\" FROM t /* ? */ WHERE a = ? -- ?";
    assertEquals(
        "SELECT '?', \"?\" FROM t /* ? */ WHERE a = 7 -- ?",
        LiteralBinder.bind(sql, List.of(ParameterBinding.of(7))));
  }

  @Test
  public void handlesBackslashEscapedQuotes() {
    String sql = "SELECT 'a" + BACKSLASH + "'?' FROM t WHERE a = ?";
    assertEquals(1, LiteralBinder.placeholderCount(sql));
  }

  @Test
  public void rejectsPlaceholderAndParameterCountMismatch() {
    assertThrows(
        IoTDBR2dbcException.class,
        () -> LiteralBinder.bind("SELECT * FROM t WHERE a = ?", List.of()));
    assertThrows(
        IoTDBR2dbcException.class,
        () -> LiteralBinder.bind("SELECT * FROM t", List.of(ParameterBinding.of(1))));
  }
}
