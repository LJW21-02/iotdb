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

import java.util.List;

/**
 * Replaces positional {@code ?} placeholders with SQL literals.
 *
 * <p>Only value positions are rewritten: string literals, quoted identifiers and comments are
 * copied verbatim, so a {@code ?} inside them never consumes a parameter. Table and column names
 * are never bound. Values are validated by {@link ParameterBinding}, which rejects every type whose
 * encoding is not verified.
 */
public final class LiteralBinder {

  private static final char BACKSLASH = '\\';

  private interface PlaceholderWriter {
    void write(StringBuilder sql, int index);
  }

  private LiteralBinder() {}

  /** Returns the number of value placeholders outside literals, identifiers and comments. */
  public static int placeholderCount(String sql) {
    return scan(sql, null, null);
  }

  /** Returns {@code sql} with every placeholder replaced by the matching parameter literal. */
  public static String bind(String sql, List<ParameterBinding> parameters) {
    StringBuilder bound = new StringBuilder(sql.length() + 16);
    int placeholders =
        scan(
            sql,
            bound,
            (output, index) -> {
              if (index >= parameters.size()) {
                throw new IoTDBR2dbcException(
                    String.format(
                        R2dbcMessages
                            .EXCEPTION_THE_STATEMENT_HAS_ARG_PLACEHOLDERS_BUT_ARG_PARAMETERS_WERE_SUPPLIED_1295275A,
                        index + 1,
                        parameters.size()));
              }
              parameters.get(index).appendLiteral(output);
            });
    if (placeholders != parameters.size()) {
      throw new IoTDBR2dbcException(
          String.format(
              R2dbcMessages
                  .EXCEPTION_THE_STATEMENT_HAS_ARG_PLACEHOLDERS_BUT_ARG_PARAMETERS_WERE_SUPPLIED_1295275A,
              placeholders,
              parameters.size()));
    }
    return bound.toString();
  }

  private static int scan(String sql, StringBuilder output, PlaceholderWriter writer) {
    int placeholders = 0;
    int index = 0;
    while (index < sql.length()) {
      char current = sql.charAt(index);
      switch (current) {
        case '\'':
          index = copyQuoted(sql, index, output, '\'', true);
          break;
        case '"':
        case '`':
          index = copyQuoted(sql, index, output, current, false);
          break;
        case '-':
          if (index + 1 < sql.length() && sql.charAt(index + 1) == '-') {
            index = copyLineComment(sql, index, output);
          } else {
            append(output, current);
            index++;
          }
          break;
        case '/':
          if (index + 1 < sql.length() && sql.charAt(index + 1) == '*') {
            index = copyBlockComment(sql, index, output);
          } else {
            append(output, current);
            index++;
          }
          break;
        case '?':
          if (writer != null) {
            writer.write(output, placeholders);
          }
          placeholders++;
          index++;
          break;
        default:
          append(output, current);
          index++;
      }
    }
    return placeholders;
  }

  /** Copies a quoted section, honouring doubled quotes and backslash escapes in strings. */
  private static int copyQuoted(
      String sql, int start, StringBuilder output, char quote, boolean backslashEscapes) {
    int index = start;
    append(output, sql.charAt(index++));
    while (index < sql.length()) {
      char current = sql.charAt(index);
      append(output, current);
      index++;
      if (current == quote) {
        if (index < sql.length() && sql.charAt(index) == quote) {
          append(output, sql.charAt(index++));
          continue;
        }
        break;
      }
      if (backslashEscapes && current == BACKSLASH && index < sql.length()) {
        append(output, sql.charAt(index++));
      }
    }
    return index;
  }

  private static int copyLineComment(String sql, int start, StringBuilder output) {
    int index = start;
    while (index < sql.length() && sql.charAt(index) != '\n') {
      append(output, sql.charAt(index++));
    }
    return index;
  }

  private static int copyBlockComment(String sql, int start, StringBuilder output) {
    int index = start;
    append(output, sql.charAt(index++));
    append(output, sql.charAt(index++));
    while (index < sql.length()) {
      char current = sql.charAt(index);
      append(output, current);
      index++;
      if (current == '*' && index < sql.length() && sql.charAt(index) == '/') {
        append(output, sql.charAt(index++));
        break;
      }
    }
    return index;
  }

  private static void append(StringBuilder output, char value) {
    if (output != null) {
      output.append(value);
    }
  }
}
