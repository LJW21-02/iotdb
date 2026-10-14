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

package org.apache.iotdb.r2dbc.exception;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.rpc.TSStatusCode;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import io.r2dbc.spi.R2dbcTimeoutException;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.concurrent.TimeoutException;

/** Maps IoTDB statuses and transport failures to R2DBC exceptions. */
public final class ExceptionMapper {

  private ExceptionMapper() {}

  /** Verifies a server status, throwing the mapped exception for every failure. */
  public static void verifySuccess(TSStatus status, String sql) {
    if (status == null) {
      throw new IoTDBR2dbcException(R2dbcMessages.EXCEPTION_IOTDB_RETURNED_A_NULL_STATUS_1200AD6C);
    }
    int code = status.getCode();
    if (code == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return;
    }
    if (code == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
      List<TSStatus> subStatuses = status.getSubStatus();
      if (subStatuses != null) {
        for (TSStatus subStatus : subStatuses) {
          verifySuccess(subStatus, sql);
        }
      }
      return;
    }
    if (code == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      // Both execution paths follow a redirection before they verify, so a recommendation that
      // arrives here named no node to follow.
      throw new IoTDBR2dbcException(
          R2dbcMessages
              .EXCEPTION_THE_SERVER_RECOMMENDED_A_REDIRECTION_WITHOUT_NAMING_A_TARGET_NODE_0F043C12);
    }
    throw mapStatus(status, sql);
  }

  /** Maps a failing server status onto the closest standard R2DBC exception type. */
  public static R2dbcException mapStatus(TSStatus status, String sql) {
    int code = status.getCode();
    String message =
        String.format(
            R2dbcMessages.MESSAGE_IOTDB_SERVER_ERROR_ARG_ADA08C89,
            status.getMessage() == null ? code : status.getMessage());
    TSStatusCode statusCode = TSStatusCode.representOf(code);
    if (statusCode != null) {
      switch (statusCode) {
        case SQL_PARSE_ERROR:
        case SEMANTIC_ERROR:
        case ILLEGAL_PATH:
        case DATABASE_NOT_EXIST:
        case TABLE_NOT_EXISTS:
        case COLUMN_NOT_EXISTS:
        case QUERY_NOT_ALLOWED:
        case UNSUPPORTED_SQL_DIALECT:
        case DATA_TYPE_MISMATCH:
          return new R2dbcBadGrammarException(message, null, code, sql);
        case NO_PERMISSION:
        case NOT_HAS_PRIVILEGE:
        case NOT_LOGIN:
        case WRONG_LOGIN_PASSWORD:
        case USER_NOT_EXIST:
        case ILLEGAL_PASSWORD:
        case USER_LOGIN_LOCKED:
        case INIT_AUTH_ERROR:
          return new R2dbcPermissionDeniedException(message, null, code, sql);
        case QUERY_TIMEOUT:
          return new R2dbcTimeoutException(message, null, code, sql);
        case ILLEGAL_PARAMETER:
        case NUMERIC_VALUE_OUT_OF_RANGE:
        case DIVISION_BY_ZERO:
        case DATE_OUT_OF_RANGE:
          return new R2dbcDataIntegrityViolationException(message, null, code, sql);
        default:
          break;
      }
    }
    return new IoTDBR2dbcException(message, null, code, sql, null);
  }

  /**
   * Maps a failure that happened before a status was received.
   *
   * <p>A protocol level timeout is reported as {@link R2dbcTimeoutException}; everything else keeps
   * the original cause. The caller is responsible for destroying a connection whose protocol state
   * is unknown.
   */
  public static R2dbcException mapTransportFailure(String message, Throwable cause) {
    return isTimeout(cause)
        ? new R2dbcTimeoutException(message, cause)
        : new IoTDBR2dbcException(message, cause);
  }

  /**
   * Maps a failure that happened while reading the next page of an active query.
   *
   * <p>A failure the server reported keeps its own type so that the page phase reports the same
   * exception as the execute phase for the same status; wrapping it would hide the status code and
   * the transient classification from the caller. A transport failure carries no message of its
   * own, so it keeps the page context and the usual timeout classification.
   */
  public static R2dbcException mapPageFailure(Throwable error) {
    if (error instanceof R2dbcException) {
      return (R2dbcException) error;
    }
    return mapTransportFailure(
        String.format(
            R2dbcMessages.EXCEPTION_READING_THE_NEXT_PAGE_FAILED_ARG_74678706, error.getMessage()),
        error);
  }

  /**
   * Whether a failure was caused by running out of time.
   *
   * <p>Both shapes are covered because the transport reports them differently: the JDK throws
   * {@link TimeoutException} (the statement budget), while Thrift reports its own timeout as a
   * transport exception with the {@code TIMED_OUT} type (the per RPC budget).
   */
  public static boolean isTimeout(Throwable cause) {
    Throwable current = cause;
    while (current != null) {
      if (current instanceof TimeoutException
          || (current instanceof TTransportException
              && ((TTransportException) current).getType() == TTransportException.TIMED_OUT)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
