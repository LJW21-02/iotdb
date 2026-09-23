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

import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TimeValueConverterTest {

  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");

  @Test
  public void convertsInstantsInTheSessionPrecision() {
    assertEquals(
        1_700_000_000_123L,
        TimeValueConverter.toEpochTicks(Instant.ofEpochMilli(1_700_000_000_123L), 1_000, UTC));
    assertEquals(
        1_123_456L,
        TimeValueConverter.toEpochTicks(Instant.ofEpochSecond(1, 123_456_000L), 1_000_000, UTC));
    assertEquals(
        1_000_000_123L,
        TimeValueConverter.toEpochTicks(Instant.ofEpochSecond(1, 123L), 1_000_000_000, UTC));
  }

  @Test
  public void convertsNegativeInstantsWithoutLosingTheFraction() {
    assertEquals(
        -500L,
        TimeValueConverter.toEpochTicks(Instant.ofEpochSecond(-1, 500_000_000L), 1_000, UTC));
    assertEquals(
        -1L, TimeValueConverter.toEpochTicks(Instant.ofEpochSecond(-1, 999_000_000L), 1_000, UTC));
  }

  @Test
  public void convertsLocalDateTimeUsingTheConnectionZone() {
    LocalDateTime utcTime = LocalDateTime.of(2026, 9, 22, 10, 0, 0);
    assertEquals(
        utcTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(),
        TimeValueConverter.toEpochTicks(utcTime, 1_000, UTC));
    LocalDateTime shanghaiTime = LocalDateTime.of(2026, 9, 22, 18, 0, 0);
    // 18:00 in UTC+8 is the same instant as 10:00 UTC.
    assertEquals(
        utcTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(),
        TimeValueConverter.toEpochTicks(shanghaiTime, 1_000, SHANGHAI));
  }

  @Test
  public void rejectsPrecisionLoss() {
    assertThrows(
        IoTDBR2dbcException.class,
        () -> TimeValueConverter.toEpochTicks(Instant.ofEpochMilli(1).plusNanos(1), 1_000, UTC));
    assertThrows(
        IoTDBR2dbcException.class,
        () -> TimeValueConverter.toEpochTicks(Instant.ofEpochSecond(1, 1L), 1_000_000, UTC));
  }

  @Test
  public void rejectsAmbiguousAndInvalidLocalTimes() {
    ZoneId newYork = ZoneId.of("America/New_York");
    // 2026-11-01 01:30 exists twice (DST overlap).
    assertThrows(
        IoTDBR2dbcException.class,
        () ->
            TimeValueConverter.toEpochTicks(LocalDateTime.of(2026, 11, 1, 1, 30), 1_000, newYork));
    // 2026-03-08 02:30 does not exist (DST gap).
    assertThrows(
        IoTDBR2dbcException.class,
        () -> TimeValueConverter.toEpochTicks(LocalDateTime.of(2026, 3, 8, 2, 30), 1_000, newYork));
  }

  @Test
  public void rejectsOverflowAndUnsupportedValues() {
    assertThrows(
        IoTDBR2dbcException.class,
        () -> TimeValueConverter.toEpochTicks(Instant.MAX, 1_000_000_000, UTC));
    assertThrows(
        IoTDBR2dbcException.class, () -> TimeValueConverter.toEpochTicks("2026-09-22", 1_000, UTC));
  }

  @Test
  public void temporalParametersUseTheTypedBigintPath() {
    ParameterBinding binding = ParameterBinding.of(LocalDateTime.of(2026, 9, 22, 10, 0));
    assertTrue(binding.isTemporal());
    assertTrue(binding.supportsTypedEncoding());
    assertEquals(Types.BIGINT, binding.jdbcType());

    ParameterBinding wire = TimeValueConverter.toWireParameter(binding, 1_000, UTC);
    assertFalse(wire.isTemporal());
    assertEquals(Types.BIGINT, wire.jdbcType());
    assertEquals(1_790_071_200_000L, ((Number) wire.encodedValue()).longValue());

    ParameterBinding scalar = ParameterBinding.of(7);
    assertEquals(scalar, TimeValueConverter.toWireParameter(scalar, 1_000, UTC));
  }

  @Test
  public void temporalParametersCannotBeRenderedWithoutConversion() {
    ParameterBinding binding = ParameterBinding.of(Instant.ofEpochMilli(1L));
    assertThrows(IoTDBR2dbcException.class, () -> binding.appendLiteral(new StringBuilder()));
  }
}
