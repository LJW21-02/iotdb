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

  public static final String EXCEPTION_A_PARTIALLY_REDIRECTED_MULTI_DEVICE_RESPONSE_CANNOT_BE_REPLAYED_SAFELY_162D6946 = "多设备响应发生部分重定向，无法安全重放";

  public static final String EXCEPTION_THE_REDIRECTION_BUDGET_OF_ARG_HOPS_IS_EXHAUSTED_344A9A03 = "%d 次重定向预算已用尽";

  public static final String EXCEPTION_FAILED_TO_INITIALIZE_TLS_ARG_770BBC57 = "初始化 TLS 失败：%s";

  public static final String EXCEPTION_PARAMETERS_CANNOT_BE_BOUND_TO_A_STATEMENT_THAT_CONTAINS_SEVERAL_SQL_STATEMENTS_89D234C0 = "包含多条 SQL 语句的 Statement 不能绑定参数";

  public static final String EXCEPTION_TEMPORAL_PARAMETERS_MUST_BE_CONVERTED_WITH_THE_SESSION_PRECISION_AND_TIME_ZONE_BEFORE_BINDING_90DAA184 = "时间类型参数必须先按会话精度和时区转换后再绑定";
  public static final String EXCEPTION_LOCAL_DATE_TIME_ARG_IS_AMBIGUOUS_OR_INVALID_IN_TIME_ZONE_ARG_665B6E7F = "本地时间 %s 在时区 %s 下存在歧义或非法";

  public static final String EXCEPTION_BATCH_STATEMENTS_MUST_NOT_CONTAIN_PARAMETERS_E2027860 = "批量语句不能包含参数";
  public static final String EXCEPTION_BINDNULL_REQUIRES_A_NON_NULL_TYPE_CFFB310F = "bindNull 的类型参数不能为空";
  public static final String EXCEPTION_BIND_REQUIRES_A_NON_NULL_VALUE_USE_BINDNULL_FOR_SQL_NULL_57886CDE = "bind 的值不能为空；SQL NULL 请使用 bindNull";
  public static final String EXCEPTION_CANNOT_CONVERT_ARG_TO_ARG_D88C8FE8 = "无法将 %s 转换为 %s";
  public static final String EXCEPTION_COLUMN_ARG_DOES_NOT_EXIST_FF67AE82 = "列 '%s' 不存在";
  public static final String EXCEPTION_COLUMN_INDEX_ARG_IS_OUT_OF_BOUNDS_THE_ROW_HAS_ARG_COLUMN_S_98C75AD7 = "列下标 %d 越界，当前行有 %d 列";
  public static final String EXCEPTION_FAILED_TO_CONNECT_TO_IOTDB_AT_ARG_ARG_2A934FC1 = "无法连接到 IoTDB：%s:%d";

  public static final String EXCEPTION_FAILED_TO_CONNECT_TO_IOTDB_AT_ARG_ARG_OVER_TLS_DD4D72A8 = "无法通过 TLS 连接到 IoTDB：%s:%d";
  public static final String EXCEPTION_FAILED_TO_CREATE_THE_R2DBC_RUNTIME_ARG_9766FF8E = "创建 R2DBC 运行时失败：%s";
  public static final String EXCEPTION_FAILED_TO_DECODE_A_TSBLOCK_ARG_050F52E4 = "TsBlock 解码失败：%s";
  public static final String EXCEPTION_FETCHSIZE_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_8B2CA91B = "fetchSize 不能为负数，实际为 %d";
  public static final String EXCEPTION_IOTDB_RETURNED_A_NULL_STATUS_1200AD6C = "IoTDB 返回了空状态";
  public static final String EXCEPTION_LOCK_WAIT_TIMEOUTS_ARE_NOT_SUPPORTED_BY_IOTDB_B93F52F8 = "IoTDB 不支持锁等待超时";
  public static final String EXCEPTION_NAMED_PARAMETERS_ARE_NOT_SUPPORTED_USE_POSITIONAL_PLACEHOLDERS_A7D827BE = "不支持命名参数，请使用按位置的 '?' 占位符";
  public static final String EXCEPTION_NAME_MUST_NOT_BE_NULL_CA324245 = "name 不能为空";
  public static final String EXCEPTION_NAN_AND_INFINITY_ARE_NOT_SUPPORTED_AS_STATEMENT_PARAMETERS_AE049503 = "语句参数不支持 NaN 和无穷大";
  public static final String EXCEPTION_OPTION_ARG_MUST_BE_AN_INTEGER_BUT_WAS_ARG_1CAFFF31 = "选项 '%s' 必须为整数，实际为 '%s'";
  public static final String EXCEPTION_OPTION_ARG_MUST_BE_A_POSITIVE_DURATION_BUT_WAS_ARG_9DD4E8F1 = "选项 '%s' 必须为正的 Duration，实际为 '%s'";
  public static final String EXCEPTION_OPTION_ARG_MUST_BE_A_POSITIVE_INTEGER_BUT_WAS_ARG_389AAC64 = "选项 '%s' 必须是正整数，实际为 '%s'";
  public static final String EXCEPTION_OPTION_ARG_MUST_BE_A_STRING_BUT_WAS_ARG_817A77EE = "选项 '%s' 必须为字符串，实际为 '%s'";
  public static final String EXCEPTION_OPTION_ARG_MUST_NOT_BE_EMPTY_C5532E9C = "选项 '%s' 不能为空";
  public static final String EXCEPTION_OPTION_ARG_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_28DA7D3A = "选项 '%s' 不能为负数，实际为 '%s'";
  public static final String EXCEPTION_OPTION_IOTDB_FRAMECOMPRESSION_ONLY_ACCEPTS_NONE_SNAPPY_HAS_NOT_BEEN_VERIFIED_AND_IS_REJECTED_EA6F4787 = "选项 'iotdb.frameCompression' 仅接受 'none'；'snappy' 尚未验证，直接拒绝";
  public static final String EXCEPTION_OPTION_IOTDB_PROTOCOL_MUST_BE_BINARY_OR_COMPACT_BUT_WAS_ARG_53E9FC70 = "选项 'iotdb.protocol' 必须是 'binary' 或 'compact'，实际为 '%s'";
  public static final String EXCEPTION_OPTION_IOTDB_SQLDIALECT_MUST_BE_TABLE_OR_TREE_BUT_WAS_ARG_BE870BD5 = "选项 'iotdb.sqlDialect' 必须是 'table' 或 'tree'，实际为 '%s'";
  public static final String EXCEPTION_OPTION_IOTDB_TIMEZONE_IS_NOT_A_VALID_TIME_ZONE_ARG_6C010057 = "选项 'iotdb.timeZone' 不是合法的时区：'%s'";
  public static final String EXCEPTION_PARAMETER_ARG_IS_NOT_BOUND_F893971C = "参数 %d 未绑定";
  public static final String EXCEPTION_PARAMETER_INDEX_ARG_EXCEEDS_THE_ARG_PLACEHOLDERS_OF_THE_STATEMENT_F5525F22 = "参数下标 %d 超出语句的 %d 个占位符";
  public static final String EXCEPTION_PARAMETER_INDEX_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_59A7879B = "参数下标不能为负数，实际为 %d";
  public static final String EXCEPTION_PORT_MUST_BE_BETWEEN_1_AND_65535_BUT_WAS_ARG_348A6B97 = "端口必须在 1 到 65535 之间，实际为 %d";
  public static final String EXCEPTION_THE_SERVER_RECOMMENDED_A_REDIRECTION_WITHOUT_NAMING_A_TARGET_NODE_0F043C12 = "服务端建议重定向，但没有指定目标节点";
  public static final String EXCEPTION_READING_THE_NEXT_PAGE_FAILED_ARG_74678706 = "读取下一页失败：%s";
  public static final String EXCEPTION_RECEIVED_AN_INVALID_FRAME_SIZE_ARG_280DE971 = "收到非法的帧长度 %d";
  public static final String EXCEPTION_RECEIVED_A_FRAME_OF_ARG_BYTES_WHICH_EXCEEDS_THE_CONFIGURED_LIMIT_OF_ARG_BYTES_FEBB2338 = "收到的帧为 %d 字节，超过配置上限 %d 字节";
  public static final String EXCEPTION_RETURNGENERATEDVALUES_IS_NOT_SUPPORTED_4550147B = "不支持 returnGeneratedValues";
  public static final String EXCEPTION_SAVEPOINTS_ARE_NOT_SUPPORTED_BY_IOTDB_9BA2D2AD = "IoTDB 不支持保存点";
  public static final String EXCEPTION_SETAUTOCOMMIT_FALSE_IS_NOT_SUPPORTED_BECAUSE_IOTDB_HAS_NO_CROSS_STATEMENT_TRANSACTIONS_962579D8 = "IoTDB 没有跨语句事务，不支持 setAutoCommit(false)";
  public static final String EXCEPTION_SQL_MUST_NOT_BE_NULL_OR_EMPTY_6BFC5576 = "SQL 不能为空";
  public static final String EXCEPTION_SSL_MUST_BE_ENABLED_WITH_BOOLEAN_TRUE_THE_STRING_TRUE_CASE_INSENSITIVE_TRIMMED_OR_THE_R2DBCS_SCHEME_THE_STRING_FALSE_AND_ALL_OTHER_VALUES_ARE_REJECTED_3BD83E9E = "SSL 只能使用 Boolean.TRUE、字符串 'true'（忽略大小写并去除空白）或 r2dbcs scheme 启用；字符串 'false' 及其他取值一律拒绝";
  public static final String EXCEPTION_STATEMENT_EXECUTION_FAILED_ARG_D1EB77EA = "语句执行失败：%s";
  public static final String EXCEPTION_THE_CONNECTION_ATTEMPT_TIMED_OUT_AFTER_ARG_EB558B68 = "建连在 %s 后超时";
  public static final String EXCEPTION_THE_CONNECTION_CAN_ONLY_RUN_ONE_STATEMENT_AT_A_TIME_18CE7BB2 = "一个连接同时只能执行一条语句";
  public static final String EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7 = "连接已关闭";
  public static final String EXCEPTION_THE_CONNECTION_STILL_HAS_AN_ACTIVE_RESULT_CLOSE_IT_BEFORE_STARTING_ANOTHER_OPERATION_4D9EA1CA = "连接仍有未结束的结果，请先关闭再执行其他操作";
  public static final String EXCEPTION_THE_RESULT_HAS_ALREADY_BEEN_CONSUMED_1A38526E = "结果已被消费";
  public static final String EXCEPTION_THE_RPC_TIMED_OUT_AFTER_ARG_6B50D3BF = "RPC 在 %s 后超时";
  public static final String EXCEPTION_THE_SERVER_DID_NOT_REPORT_ITS_TIMESTAMP_PRECISION_730DC973 = "服务端未返回时间戳精度";
  public static final String EXCEPTION_THE_SERVER_DID_NOT_RETURN_A_SESSION_ID_F39EF954 = "服务端未返回 session id";
  public static final String EXCEPTION_THE_SERVER_REPORTED_AN_UNSUPPORTED_TIMESTAMP_PRECISION_ARG_478A6D95 = "服务端返回了不支持的时间戳精度：%s";
  public static final String EXCEPTION_THE_STATEMENT_HAS_ARG_PLACEHOLDERS_BUT_ARG_PARAMETERS_WERE_SUPPLIED_1295275A = "语句有 %d 个占位符，但提供了 %d 个参数";
  public static final String EXCEPTION_THE_STATEMENT_TIMED_OUT_AFTER_ARG_37D4D030 = "语句在 %s 后超时";
  public static final String EXCEPTION_THE_STATEMENT_TIMEOUT_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_74BD2508 = "语句超时不能为负数，实际为 %s";
  public static final String EXCEPTION_TRANSACTIONS_ARE_NOT_SUPPORTED_BY_IOTDB_C248F946 = "IoTDB 不支持事务";
  public static final String EXCEPTION_TRANSACTION_ISOLATION_LEVELS_CANNOT_BE_CHANGED_BECAUSE_IOTDB_HAS_NO_CROSS_STATEMENT_TRANSACTIONS_54D87733 = "IoTDB 没有跨语句事务，无法修改事务隔离级别";
  public static final String EXCEPTION_TYPE_MUST_NOT_BE_NULL_3C9753B3 = "type 不能为空";
  public static final String EXCEPTION_UNEXPECTED_ARG_RESPONSE_FROM_IOTDB_6A4FBFB0 = "IoTDB 返回了非预期的 %s 响应";
  public static final String EXCEPTION_UNSUPPORTED_IOTDB_DATA_TYPE_ARG_69C453DA = "不支持的 IoTDB 数据类型：%s";
  public static final String EXCEPTION_VALUES_OF_TYPE_ARG_ARE_NOT_SUPPORTED_AS_STATEMENT_PARAMETERS_F4A3A5F4 = "不支持类型为 %s 的语句参数";
  public static final String LOG_CLEANING_UP_THE_QUERY_HANDLE_FAILED_DE915B4D = "清理查询句柄失败";
  public static final String LOG_CLOSING_THE_R2DBC_CONNECTION_FAILED_D2D86252 = "关闭 R2DBC 连接失败";
  public static final String LOG_IOTDB_R2DBC_CONNECTION_CLOSED_FAA1CE20 = "IoTDB R2DBC 连接已关闭";
  public static final String MESSAGE_IOTDB_SERVER_ERROR_ARG_ADA08C89 = "IoTDB 服务端错误：%s";
}
