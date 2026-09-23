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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Converts temporal parameters into the form verified against the server.
 *
 * <p>IoTDB stores time as epoch ticks in the session precision and accepts them as 64 bit integers:
 * {@code WHERE time > ?} with a BIGINT parameter returns the expected rows, while an ISO-8601
 * string is rejected with {@code Cannot apply operator: TIMESTAMP < STRING}. Temporal values are
 * therefore converted to ticks before the parameter is encoded, and an ambiguous or precision
 * losing conversion fails instead of being rounded silently.
 */
public final class TimeValueConverter {

  private static final long NANOS_PER_SECOND = 1_000_000_000L;

  private TimeValueConverter() {}

  /** Returns the parameter unchanged unless it carries a temporal value. */
  public static ParameterBinding toWireParameter(
      ParameterBinding parameter, int timeFactor, ZoneId zoneId) {
    if (!parameter.isTemporal()) {
      return parameter;
    }
    return ParameterBinding.of(toEpochTicks(parameter.rawValue(), timeFactor, zoneId));
  }

  /**
   * Converts a {@link LocalDateTime} or {@link Instant} into epoch ticks of the session precision.
   */
  public static long toEpochTicks(Object value, int timeFactor, ZoneId zoneId) {
    Instant instant;
    if (value instanceof Instant) {
      instant = (Instant) value;
    } else if (value instanceof LocalDateTime) {
      LocalDateTime localDateTime = (LocalDateTime) value;
      List<ZoneOffset> offsets = zoneId.getRules().getValidOffsets(localDateTime);
      if (offsets.size() != 1) {
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages
                    .EXCEPTION_LOCAL_DATE_TIME_ARG_IS_AMBIGUOUS_OR_INVALID_IN_TIME_ZONE_ARG_665B6E7F,
                value,
                zoneId));
      }
      instant = localDateTime.atOffset(offsets.get(0)).toInstant();
    } else {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_CANNOT_CONVERT_ARG_TO_ARG_D88C8FE8, value, "TIMESTAMP"));
    }
    long nanosPerTick = NANOS_PER_SECOND / timeFactor;
    if (instant.getNano() % nanosPerTick != 0) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_CANNOT_CONVERT_ARG_TO_ARG_D88C8FE8,
              value,
              "epoch ticks without precision loss"));
    }
    try {
      return Math.addExact(
          Math.multiplyExact(instant.getEpochSecond(), timeFactor),
          instant.getNano() / nanosPerTick);
    } catch (ArithmeticException e) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages.EXCEPTION_CANNOT_CONVERT_ARG_TO_ARG_D88C8FE8, value, "epoch ticks"),
          e);
    }
  }
}
