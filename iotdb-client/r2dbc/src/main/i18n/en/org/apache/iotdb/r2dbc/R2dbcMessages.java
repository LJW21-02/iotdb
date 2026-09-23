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


package org.apache.iotdb.r2dbc;

public final class R2dbcMessages {

  private R2dbcMessages() {}

  public static final String EXCEPTION_PARAMETERS_CANNOT_BE_BOUND_TO_A_STATEMENT_THAT_CONTAINS_SEVERAL_SQL_STATEMENTS_89D234C0 = "Parameters cannot be bound to a statement that contains several SQL statements";

  public static final String EXCEPTION_TEMPORAL_PARAMETERS_MUST_BE_CONVERTED_WITH_THE_SESSION_PRECISION_AND_TIME_ZONE_BEFORE_BINDING_90DAA184 = "Temporal parameters must be converted with the session precision and time zone before binding";
  public static final String EXCEPTION_LOCAL_DATE_TIME_ARG_IS_AMBIGUOUS_OR_INVALID_IN_TIME_ZONE_ARG_665B6E7F = "Local date time %s is ambiguous or invalid in time zone %s";

  public static final String EXCEPTION_BATCH_STATEMENTS_MUST_NOT_CONTAIN_PARAMETERS_E2027860 = "Batch statements must not contain parameters";
  public static final String EXCEPTION_BINDNULL_REQUIRES_A_NON_NULL_TYPE_CFFB310F = "bindNull requires a non-null type";
  public static final String EXCEPTION_BIND_REQUIRES_A_NON_NULL_VALUE_USE_BINDNULL_FOR_SQL_NULL_57886CDE = "bind requires a non-null value; use bindNull for SQL NULL";
  public static final String EXCEPTION_CANNOT_CONVERT_ARG_TO_ARG_D88C8FE8 = "Cannot convert %s to %s";
  public static final String EXCEPTION_COLUMN_ARG_DOES_NOT_EXIST_FF67AE82 = "Column '%s' does not exist";
  public static final String EXCEPTION_COLUMN_INDEX_ARG_IS_OUT_OF_BOUNDS_THE_ROW_HAS_ARG_COLUMN_S_98C75AD7 = "Column index %d is out of bounds; the row has %d column(s)";
  public static final String EXCEPTION_FAILED_TO_CONNECT_TO_IOTDB_AT_ARG_ARG_2A934FC1 = "Failed to connect to IoTDB at %s:%d";
  public static final String EXCEPTION_FAILED_TO_CREATE_THE_R2DBC_RUNTIME_ARG_9766FF8E = "Failed to create the R2DBC runtime: %s";
  public static final String EXCEPTION_FAILED_TO_DECODE_A_TSBLOCK_ARG_050F52E4 = "Failed to decode a TsBlock: %s";
  public static final String EXCEPTION_FETCHSIZE_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_8B2CA91B = "fetchSize must not be negative, but was %d";
  public static final String EXCEPTION_IOTDB_RETURNED_A_NULL_STATUS_1200AD6C = "IoTDB returned a null status";
  public static final String EXCEPTION_LOCK_WAIT_TIMEOUTS_ARE_NOT_SUPPORTED_BY_IOTDB_B93F52F8 = "Lock wait timeouts are not supported by IoTDB";
  public static final String EXCEPTION_NAMED_PARAMETERS_ARE_NOT_SUPPORTED_USE_POSITIONAL_PLACEHOLDERS_A7D827BE = "Named parameters are not supported; use positional '?' placeholders";
  public static final String EXCEPTION_NAME_MUST_NOT_BE_NULL_CA324245 = "name must not be null";
  public static final String EXCEPTION_NAN_AND_INFINITY_ARE_NOT_SUPPORTED_AS_STATEMENT_PARAMETERS_AE049503 = "NaN and infinity are not supported as statement parameters";
  public static final String EXCEPTION_OPTION_ARG_MUST_BE_AN_INTEGER_BUT_WAS_ARG_1CAFFF31 = "Option '%s' must be an integer, but was '%s'";
  public static final String EXCEPTION_OPTION_ARG_MUST_BE_A_POSITIVE_DURATION_BUT_WAS_ARG_9DD4E8F1 = "Option '%s' must be a positive Duration, but was '%s'";
  public static final String EXCEPTION_OPTION_ARG_MUST_BE_A_STRING_BUT_WAS_ARG_817A77EE = "Option '%s' must be a string, but was '%s'";
  public static final String EXCEPTION_OPTION_ARG_MUST_NOT_BE_EMPTY_C5532E9C = "Option '%s' must not be empty";
  public static final String EXCEPTION_OPTION_ARG_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_28DA7D3A = "Option '%s' must not be negative, but was '%s'";
  public static final String EXCEPTION_OPTION_IOTDB_FRAMECOMPRESSION_ONLY_ACCEPTS_NONE_SNAPPY_HAS_NOT_BEEN_VERIFIED_AND_IS_REJECTED_EA6F4787 = "Option 'iotdb.frameCompression' only accepts 'none'; 'snappy' has not been verified and is rejected";
  public static final String EXCEPTION_OPTION_IOTDB_PROTOCOL_MUST_BE_BINARY_OR_COMPACT_BUT_WAS_ARG_53E9FC70 = "Option 'iotdb.protocol' must be 'binary' or 'compact', but was '%s'";
  public static final String EXCEPTION_OPTION_IOTDB_SQLDIALECT_MUST_BE_TABLE_OR_TREE_BUT_WAS_ARG_BE870BD5 = "Option 'iotdb.sqlDialect' must be 'table' or 'tree', but was '%s'";
  public static final String EXCEPTION_OPTION_IOTDB_TIMEZONE_IS_NOT_A_VALID_TIME_ZONE_ARG_6C010057 = "Option 'iotdb.timeZone' is not a valid time zone: '%s'";
  public static final String EXCEPTION_PARAMETER_ARG_IS_NOT_BOUND_F893971C = "Parameter %d is not bound";
  public static final String EXCEPTION_PARAMETER_INDEX_ARG_EXCEEDS_THE_ARG_PLACEHOLDERS_OF_THE_STATEMENT_F5525F22 = "Parameter index %d exceeds the %d placeholders of the statement";
  public static final String EXCEPTION_PARAMETER_INDEX_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_59A7879B = "Parameter index must not be negative, but was %d";
  public static final String EXCEPTION_PORT_MUST_BE_BETWEEN_1_AND_65535_BUT_WAS_ARG_348A6B97 = "Port must be between 1 and 65535, but was %d";
  public static final String EXCEPTION_QUERY_REDIRECTION_IS_NOT_SUPPORTED_BY_THIS_DRIVER_VERSION_AEDD09FA = "Query redirection is not supported by this driver version";
  public static final String EXCEPTION_READING_THE_NEXT_PAGE_FAILED_ARG_74678706 = "Reading the next page failed: %s";
  public static final String EXCEPTION_RECEIVED_AN_INVALID_FRAME_SIZE_ARG_280DE971 = "Received an invalid frame size %d";
  public static final String EXCEPTION_RECEIVED_A_FRAME_OF_ARG_BYTES_WHICH_EXCEEDS_THE_CONFIGURED_LIMIT_OF_ARG_BYTES_FEBB2338 = "Received a frame of %d bytes which exceeds the configured limit of %d bytes";
  public static final String EXCEPTION_RETURNGENERATEDVALUES_IS_NOT_SUPPORTED_4550147B = "returnGeneratedValues is not supported";
  public static final String EXCEPTION_SAVEPOINTS_ARE_NOT_SUPPORTED_BY_IOTDB_9BA2D2AD = "Savepoints are not supported by IoTDB";
  public static final String EXCEPTION_SETAUTOCOMMIT_FALSE_IS_NOT_SUPPORTED_BECAUSE_IOTDB_HAS_NO_CROSS_STATEMENT_TRANSACTIONS_962579D8 = "setAutoCommit(false) is not supported because IoTDB has no cross-statement transactions";
  public static final String EXCEPTION_SQL_MUST_NOT_BE_NULL_OR_EMPTY_6BFC5576 = "SQL must not be null or empty";
  public static final String EXCEPTION_SSL_MUST_BE_ENABLED_WITH_BOOLEAN_TRUE_THE_STRING_TRUE_CASE_INSENSITIVE_TRIMMED_OR_THE_R2DBCS_SCHEME_THE_STRING_FALSE_AND_ALL_OTHER_VALUES_ARE_REJECTED_3BD83E9E = "SSL must be enabled with Boolean.TRUE, the string 'true' (case-insensitive, trimmed), or the r2dbcs scheme; the string 'false' and all other values are rejected";
  public static final String EXCEPTION_STATEMENT_EXECUTION_FAILED_ARG_D1EB77EA = "Statement execution failed: %s";
  public static final String EXCEPTION_THE_CONNECTION_ATTEMPT_TIMED_OUT_AFTER_ARG_EB558B68 = "The connection attempt timed out after %s";
  public static final String EXCEPTION_THE_CONNECTION_CAN_ONLY_RUN_ONE_STATEMENT_AT_A_TIME_18CE7BB2 = "The connection can only run one statement at a time";
  public static final String EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7 = "The connection is closed";
  public static final String EXCEPTION_THE_CONNECTION_STILL_HAS_AN_ACTIVE_RESULT_CLOSE_IT_BEFORE_STARTING_ANOTHER_OPERATION_4D9EA1CA = "The connection still has an active result; close it before starting another operation";
  public static final String EXCEPTION_THE_RESULT_HAS_ALREADY_BEEN_CONSUMED_1A38526E = "The result has already been consumed";
  public static final String EXCEPTION_THE_RPC_TIMED_OUT_AFTER_ARG_6B50D3BF = "The RPC timed out after %s";
  public static final String EXCEPTION_THE_SERVER_DID_NOT_REPORT_ITS_TIMESTAMP_PRECISION_730DC973 = "The server did not report its timestamp precision";
  public static final String EXCEPTION_THE_SERVER_DID_NOT_RETURN_A_SESSION_ID_F39EF954 = "The server did not return a session id";
  public static final String EXCEPTION_THE_SERVER_REPORTED_AN_UNSUPPORTED_TIMESTAMP_PRECISION_ARG_478A6D95 = "The server reported an unsupported timestamp precision: %s";
  public static final String EXCEPTION_THE_STATEMENT_HAS_ARG_PLACEHOLDERS_BUT_ARG_PARAMETERS_WERE_SUPPLIED_1295275A = "The statement has %d placeholders but %d parameters were supplied";
  public static final String EXCEPTION_THE_STATEMENT_TIMED_OUT_AFTER_ARG_37D4D030 = "The statement timed out after %s";
  public static final String EXCEPTION_THE_STATEMENT_TIMEOUT_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_74BD2508 = "The statement timeout must not be negative, but was %s";
  public static final String EXCEPTION_TLS_IS_NOT_SUPPORTED_BY_THIS_DRIVER_VERSION_79D86451 = "TLS is not supported by this driver version";
  public static final String EXCEPTION_TRANSACTIONS_ARE_NOT_SUPPORTED_BY_IOTDB_C248F946 = "Transactions are not supported by IoTDB";
  public static final String EXCEPTION_TRANSACTION_ISOLATION_LEVELS_CANNOT_BE_CHANGED_BECAUSE_IOTDB_HAS_NO_CROSS_STATEMENT_TRANSACTIONS_54D87733 = "Transaction isolation levels cannot be changed because IoTDB has no cross-statement transactions";
  public static final String EXCEPTION_TYPE_MUST_NOT_BE_NULL_3C9753B3 = "type must not be null";
  public static final String EXCEPTION_UNEXPECTED_ARG_RESPONSE_FROM_IOTDB_6A4FBFB0 = "Unexpected %s response from IoTDB";
  public static final String EXCEPTION_UNSUPPORTED_IOTDB_DATA_TYPE_ARG_69C453DA = "Unsupported IoTDB data type: %s";
  public static final String EXCEPTION_VALUES_OF_TYPE_ARG_ARE_NOT_SUPPORTED_AS_STATEMENT_PARAMETERS_F4A3A5F4 = "Values of type %s are not supported as statement parameters";
  public static final String LOG_CLEANING_UP_THE_QUERY_HANDLE_FAILED_DE915B4D = "Cleaning up the query handle failed";
  public static final String LOG_CLOSING_THE_R2DBC_CONNECTION_FAILED_D2D86252 = "Closing the R2DBC connection failed";
  public static final String LOG_IOTDB_R2DBC_CONNECTION_CLOSED_FAA1CE20 = "IoTDB R2DBC connection closed";
  public static final String MESSAGE_IOTDB_SERVER_ERROR_ARG_ADA08C89 = "IoTDB server error: %s";
}
