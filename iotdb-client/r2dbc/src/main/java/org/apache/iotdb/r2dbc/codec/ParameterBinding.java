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

import java.math.BigDecimal;
import java.sql.Types;

/**
 * One validated statement parameter.
 *
 * <p>Only types whose encoding is defined for this driver are accepted: strings, booleans, the
 * integral types, floating point numbers and {@link BigDecimal}. Time values, binary values and
 * anything else are rejected before a request is sent, because neither the typed nor the literal
 * encoding has been verified for them.
 */
public final class ParameterBinding {

  private final Object value;
  private final Class<?> declaredType;
  private final boolean nullValue;

  private ParameterBinding(Object value, Class<?> declaredType, boolean nullValue) {
    this.value = value;
    this.declaredType = declaredType;
    this.nullValue = nullValue;
  }

  public static ParameterBinding of(Object value) {
    if (value == null) {
      throw new IllegalArgumentException(
          R2dbcMessages
              .EXCEPTION_BIND_REQUIRES_A_NON_NULL_VALUE_USE_BINDNULL_FOR_SQL_NULL_57886CDE);
    }
    requireSupported(value);
    return new ParameterBinding(value, value.getClass(), false);
  }

  public static ParameterBinding nullOf(Class<?> type) {
    if (type == null) {
      throw new IllegalArgumentException(
          R2dbcMessages.EXCEPTION_BINDNULL_REQUIRES_A_NON_NULL_TYPE_CFFB310F);
    }
    requireSupportedType(type);
    return new ParameterBinding(null, type, true);
  }

  public boolean isNull() {
    return nullValue;
  }

  /** Internal Java SQL type used only to bridge into the server side parameter codec. */
  public int jdbcType() {
    Class<?> type = nullValue ? declaredType : value.getClass();
    if (Boolean.class.equals(type)) {
      return Types.BOOLEAN;
    }
    if (Byte.class.equals(type) || Short.class.equals(type) || Integer.class.equals(type)) {
      return Types.INTEGER;
    }
    if (Long.class.equals(type)) {
      return Types.BIGINT;
    }
    if (Float.class.equals(type)) {
      return Types.FLOAT;
    }
    if (Double.class.equals(type)) {
      return Types.DOUBLE;
    }
    if (CharSequence.class.isAssignableFrom(type)) {
      return Types.VARCHAR;
    }
    throw unsupported(type);
  }

  /** Value handed to the server side parameter codec; {@code null} for SQL NULL. */
  public Object encodedValue() {
    if (nullValue) {
      return null;
    }
    if (value instanceof Character) {
      return value.toString();
    }
    return value;
  }

  /** Whether the typed prepare path knows how to encode this parameter. */
  public boolean supportsTypedEncoding() {
    if (nullValue) {
      return true;
    }
    return !(value instanceof BigDecimal);
  }

  /** Renders the parameter as a SQL literal for the fallback path. */
  public void appendLiteral(StringBuilder sql) {
    if (nullValue) {
      sql.append("NULL");
      return;
    }
    if (value instanceof CharSequence || value instanceof Character) {
      sql.append('\'').append(value.toString().replace("'", "''")).append('\'');
      return;
    }
    if (value instanceof Boolean) {
      sql.append(((Boolean) value) ? "TRUE" : "FALSE");
      return;
    }
    if (value instanceof Float || value instanceof Double) {
      double number = ((Number) value).doubleValue();
      if (Double.isNaN(number) || Double.isInfinite(number)) {
        throw new IoTDBR2dbcException(
            R2dbcMessages
                .EXCEPTION_NAN_AND_INFINITY_ARE_NOT_SUPPORTED_AS_STATEMENT_PARAMETERS_AE049503);
      }
      sql.append(value);
      return;
    }
    if (value instanceof BigDecimal) {
      sql.append(((BigDecimal) value).toPlainString());
      return;
    }
    sql.append(value);
  }

  @Override
  public String toString() {
    return nullValue ? "NULL" : String.valueOf(value);
  }

  private static void requireSupported(Object value) {
    requireSupportedType(value.getClass());
  }

  private static void requireSupportedType(Class<?> type) {
    if (Boolean.class.equals(type)
        || Byte.class.equals(type)
        || Short.class.equals(type)
        || Integer.class.equals(type)
        || Long.class.equals(type)
        || Float.class.equals(type)
        || Double.class.equals(type)
        || BigDecimal.class.equals(type)
        || CharSequence.class.isAssignableFrom(type)) {
      return;
    }
    throw unsupported(type);
  }

  private static IoTDBR2dbcException unsupported(Class<?> type) {
    return new IoTDBR2dbcException(
        String.format(
            R2dbcMessages
                .EXCEPTION_VALUES_OF_TYPE_ARG_ARE_NOT_SUPPORTED_AS_STATEMENT_PARAMETERS_F4A3A5F4,
            type.getName()));
  }
}
