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

import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration.SqlDialect;
import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;

import java.util.ArrayList;
import java.util.List;

/**
 * Replaces positional {@code ?} placeholders with SQL literals.
 *
 * <p>Only value positions are rewritten: string literals, quoted identifiers and dialect comments
 * are copied verbatim, so a {@code ?} inside them never consumes a parameter. Table and column
 * names are never bound. Values are validated by {@link ParameterBinding}, which rejects every type
 * whose encoding is not verified.
 */
public final class LiteralBinder {

  private interface PlaceholderWriter {
    void write(StringBuilder sql, int index);
  }

  private LiteralBinder() {}

  /** Returns the number of value placeholders using Table SQL rules. */
  public static int placeholderCount(String sql) {
    return placeholderCount(sql, SqlDialect.TABLE);
  }

  /** Returns the number of value placeholders using the server SQL dialect's lexical rules. */
  public static int placeholderCount(String sql, SqlDialect dialect) {
    return scan(sql, dialect, null, null);
  }

  /** Returns {@code sql} with every placeholder replaced by the matching parameter literal. */
  public static String bind(String sql, List<ParameterBinding> parameters) {
    return bind(sql, parameters, SqlDialect.TABLE);
  }

  /** Returns {@code sql} with placeholders replaced according to the server SQL dialect. */
  public static String bind(String sql, List<ParameterBinding> parameters, SqlDialect dialect) {
    StringBuilder bound = new StringBuilder(sql.length() + 16);
    int placeholders =
        scan(
            sql,
            dialect,
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

  /**
   * Splits a SQL string on top level semicolons.
   *
   * <p>Strings, quoted identifiers and comments are copied verbatim, so a semicolon inside them
   * does not start a new statement. Empty statements and Table comment-only fragments are dropped.
   */
  public static List<String> splitStatements(String sql) {
    return splitStatements(sql, SqlDialect.TABLE);
  }

  /** Splits statements using the server SQL dialect's lexical rules. */
  public static List<String> splitStatements(String sql, SqlDialect dialect) {
    List<String> statements = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    boolean hasContent = false;
    int index = 0;
    while (index < sql.length()) {
      char character = sql.charAt(index);
      switch (character) {
        case '\'':
          hasContent = true;
          index = copyQuoted(sql, index, current, '\'');
          break;
        case '"':
        case '`':
          hasContent = true;
          index = copyQuoted(sql, index, current, character);
          break;
        case '-':
          if (dialect == SqlDialect.TABLE
              && index + 1 < sql.length()
              && sql.charAt(index + 1) == '-') {
            index = copyLineComment(sql, index, current);
          } else {
            hasContent = true;
            current.append(character);
            index++;
          }
          break;
        case '/':
          if (dialect == SqlDialect.TABLE && startsBlockComment(sql, index)) {
            index = copyBlockComment(sql, index, current);
          } else {
            hasContent = true;
            current.append(character);
            index++;
          }
          break;
        case ';':
          addStatement(statements, current, hasContent);
          current.setLength(0);
          hasContent = false;
          index++;
          break;
        default:
          // Match trim() below: non-ASCII whitespace remains SQL for the server to validate.
          hasContent |= character > ' ';
          current.append(character);
          index++;
      }
    }
    addStatement(statements, current, hasContent);
    return statements;
  }

  private static void addStatement(
      List<String> statements, StringBuilder statement, boolean hasContent) {
    if (hasContent) {
      statements.add(statement.toString().trim());
    }
  }

  private static int scan(
      String sql, SqlDialect dialect, StringBuilder output, PlaceholderWriter writer) {
    int placeholders = 0;
    int index = 0;
    while (index < sql.length()) {
      char current = sql.charAt(index);
      switch (current) {
        case '\'':
          index = copyQuoted(sql, index, output, '\'');
          break;
        case '"':
        case '`':
          index = copyQuoted(sql, index, output, current);
          break;
        case '-':
          if (dialect == SqlDialect.TABLE
              && index + 1 < sql.length()
              && sql.charAt(index + 1) == '-') {
            index = copyLineComment(sql, index, output);
          } else {
            append(output, current);
            index++;
          }
          break;
        case '/':
          if (dialect == SqlDialect.TABLE && startsBlockComment(sql, index)) {
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

  /** Copies a quoted section, honouring the doubled-quote rule used by both server grammars. */
  private static int copyQuoted(String sql, int start, StringBuilder output, char quote) {
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
    }
    return index;
  }

  private static int copyLineComment(String sql, int start, StringBuilder output) {
    int index = start;
    while (index < sql.length() && sql.charAt(index) != '\r' && sql.charAt(index) != '\n') {
      append(output, sql.charAt(index++));
    }
    if (index < sql.length() && sql.charAt(index) == '\r') {
      append(output, sql.charAt(index++));
    }
    if (index < sql.length() && sql.charAt(index) == '\n') {
      append(output, sql.charAt(index++));
    }
    return index;
  }

  private static boolean startsBlockComment(String sql, int index) {
    return index + 1 < sql.length()
        && sql.charAt(index + 1) == '*'
        && sql.indexOf("*/", index + 2) >= 0;
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
