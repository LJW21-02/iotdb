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

package org.apache.iotdb.r2dbc.client;

import org.apache.iotdb.r2dbc.IoTDBConnectionConfiguration;
import org.apache.iotdb.r2dbc.R2dbcMessages;
import org.apache.iotdb.r2dbc.codec.LiteralBinder;
import org.apache.iotdb.r2dbc.codec.ParameterBinding;
import org.apache.iotdb.r2dbc.codec.TimeValueConverter;
import org.apache.iotdb.r2dbc.exception.ExceptionMapper;
import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;
import org.apache.iotdb.r2dbc.transport.IoTDBAsyncClient;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.stmt.PreparedParameterSerde;
import org.apache.iotdb.service.rpc.thrift.TSCloseOperationReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSDeallocatePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecutePreparedReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsReq;
import org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSPrepareReq;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Lifecycle;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TTransportException;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * One non-blocking IoTDB session.
 *
 * <p>The connection serialises requests: a new request is chained behind the previous one and only
 * one result may be active at a time. Cleanup of a query handle therefore always completes before
 * the connection is used again, and a request whose outcome is unknown destroys the channel instead
 * of returning an out-of-sync connection to a pool.
 */
public final class IoTDBConnection implements Connection, Lifecycle {

  public static final String DATABASE_PRODUCT_NAME = "Apache IoTDB";

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBConnection.class);
  private static final String CLIENT_VERSION = "V_1_0";
  private static final String PRECISION_MILLISECOND = "ms";
  private static final String PRECISION_MICROSECOND = "us";
  private static final String PRECISION_NANOSECOND = "ns";

  private final IoTDBConnectionConfiguration configuration;
  private final IoTDBAsyncClient client;
  private final Scheduler scheduler;
  private final long sessionId;
  private final long statementId;
  private final int timeFactor;
  private final String serverVersion;
  private final AtomicBoolean open = new AtomicBoolean(true);
  private final AtomicBoolean statementActive = new AtomicBoolean();
  private final AtomicLong preparedStatementCounter = new AtomicLong();
  private final Object rpcLock = new Object();

  private CompletableFuture<Void> rpcTail = CompletableFuture.completedFuture(null);
  private volatile CompletableFuture<Void> cleanupTail = CompletableFuture.completedFuture(null);
  private volatile Duration statementTimeout = Duration.ZERO;
  private volatile IoTDBQueryCursor activeCursor;

  private IoTDBConnection(
      IoTDBConnectionConfiguration configuration,
      IoTDBAsyncClient client,
      Scheduler scheduler,
      Handshake handshake) {
    this.configuration = configuration;
    this.client = client;
    this.scheduler = scheduler;
    this.sessionId = handshake.sessionId;
    this.statementId = handshake.statementId;
    this.timeFactor = handshake.timeFactor;
    this.serverVersion = handshake.serverVersion;
  }

  /**
   * Opens a session: TCP connect, {@code openSession}, statement id allocation and server
   * properties, all inside the configured connect timeout.
   */
  public static Mono<IoTDBConnection> connect(
      IoTDBConnectionConfiguration configuration,
      TAsyncClientManager clientManager,
      Scheduler scheduler) {
    IoTDBAsyncClient client = new IoTDBAsyncClient(configuration, clientManager);
    return Mono.fromCompletionStage(() -> handshake(configuration, client))
        .timeout(configuration.connectTimeout())
        .onErrorMap(
            TimeoutException.class,
            error ->
                new R2dbcTimeoutException(
                    String.format(
                        R2dbcMessages.EXCEPTION_THE_CONNECTION_ATTEMPT_TIMED_OUT_AFTER_ARG_EB558B68,
                        configuration.connectTimeout())))
        .doOnError(error -> client.close())
        .map(handshake -> new IoTDBConnection(configuration, client, scheduler, handshake));
  }

  private static CompletableFuture<Handshake> handshake(
      IoTDBConnectionConfiguration configuration, IoTDBAsyncClient client) {
    TSOpenSessionReq request =
        new TSOpenSessionReq()
            .setUsername(configuration.user())
            .setPassword(configuration.password())
            .setZoneId(configuration.zoneId().getId());
    Map<String, String> connectConfiguration = new HashMap<>();
    connectConfiguration.put("version", CLIENT_VERSION);
    connectConfiguration.put(
        "sql_dialect",
        configuration.sqlDialect() == IoTDBConnectionConfiguration.SqlDialect.TABLE
            ? "table"
            : "tree");
    if (configuration.database() != null) {
      connectConfiguration.put("db", configuration.database());
    }
    request.setConfiguration(connectConfiguration);

    return client
        .openSession(request)
        .thenCompose(
            response -> {
              ExceptionMapper.verifySuccess(response.getStatus(), null);
              if (response.getSessionId() <= 0) {
                throw new IoTDBR2dbcException(
                    R2dbcMessages.EXCEPTION_THE_SERVER_DID_NOT_RETURN_A_SESSION_ID_F39EF954);
              }
              Handshake handshake = new Handshake(response);
              return client
                  .requestStatementId(handshake.sessionId)
                  .thenCompose(
                      statementId -> {
                        handshake.statementId = statementId;
                        return client.getProperties();
                      })
                  .thenApply(
                      properties -> {
                        handshake.serverVersion = properties.getVersion();
                        client.limitFrameSize(properties.getThriftMaxFrameSize());
                        return handshake;
                      });
            });
  }

  /** Reads the session timestamp precision; an unknown precision must not be assumed to be ms. */
  private static int timeFactor(TSOpenSessionResp response) {
    String precision =
        response.isSetConfiguration()
            ? response.getConfiguration().get(RpcUtils.TIME_PRECISION)
            : null;
    if (precision == null) {
      throw new IoTDBR2dbcException(
          R2dbcMessages.EXCEPTION_THE_SERVER_DID_NOT_REPORT_ITS_TIMESTAMP_PRECISION_730DC973);
    }
    switch (precision) {
      case PRECISION_MILLISECOND:
        return 1_000;
      case PRECISION_MICROSECOND:
        return 1_000_000;
      case PRECISION_NANOSECOND:
        return 1_000_000_000;
      default:
        throw new IoTDBR2dbcException(
            String.format(
                R2dbcMessages
                    .EXCEPTION_THE_SERVER_REPORTED_AN_UNSUPPORTED_TIMESTAMP_PRECISION_ARG_478A6D95,
                precision));
    }
  }

  /**
   * Executes one statement with the parameters of a single binding set.
   *
   * <p>Table model queries with supported parameter types use the typed prepare RPC. Everything
   * else falls back to validated literal binding, because the server only prepares queries and does
   * not support prepared statements for the tree dialect at all.
   */
  Mono<IoTDBResult> execute(String sql, List<ParameterBinding> parameters, int fetchSize) {
    return Mono.defer(
        () -> {
          if (!open.get()) {
            return Mono.error(
                new IoTDBR2dbcException(R2dbcMessages.EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7));
          }
          if (statementActive.compareAndSet(false, true)) {
            return runExecution(sql, parameters, fetchSize);
          }
          if (activeCursor != null) {
            return Mono.error(stillHasActiveResult());
          }
          // The previous result was already released; its cleanup is queued, so wait for it
          // instead of failing the next statement.
          return Mono.fromCompletionStage(cleanupTail)
              .then(
                  Mono.defer(
                      () ->
                          statementActive.compareAndSet(false, true)
                              ? runExecution(sql, parameters, fetchSize)
                              : Mono.error(stillHasActiveResult())));
        });
  }

  private Mono<IoTDBResult> runExecution(
      String sql, List<ParameterBinding> parameters, int fetchSize) {
    return Mono.defer(
        () -> {
          int effectiveFetchSize = effectiveFetchSize(fetchSize);
          List<ParameterBinding> wireParameters = toWireParameters(parameters);
          Mono<TSExecuteStatementResp> execution;
          String preparedStatementName = null;
          if (wireParameters.isEmpty()) {
            execution = executeStatement(sql, effectiveFetchSize);
          } else if (canUsePreparedStatement(sql, wireParameters)) {
            preparedStatementName = preparedName();
            execution =
                executePreparedStatement(
                    sql, wireParameters, effectiveFetchSize, preparedStatementName);
          } else {
            // The tree dialect has no server side prepare; literals are the only verified path.
            execution =
                executeStatement(LiteralBinder.bind(sql, wireParameters), effectiveFetchSize);
          }
          String preparedName = preparedStatementName;
          Duration timeout = statementTimeout;
          Mono<IoTDBResult> result =
              execution.map(response -> buildResult(sql, response, fetchSize, preparedName));
          if (!timeout.isZero()) {
            result = result.timeout(timeout);
          }
          return result.onErrorResume(
              error -> {
                statementActive.set(false);
                if (error instanceof R2dbcException) {
                  return Mono.error(error);
                }
                if (error instanceof TimeoutException) {
                  abort();
                  return Mono.error(
                      new R2dbcTimeoutException(
                          String.format(
                              R2dbcMessages.EXCEPTION_THE_STATEMENT_TIMED_OUT_AFTER_ARG_37D4D030,
                              timeout)));
                }
                // The protocol state is unknown after a transport failure.
                abort();
                String message =
                    isTimeout(error)
                        ? String.format(
                            R2dbcMessages.EXCEPTION_THE_RPC_TIMED_OUT_AFTER_ARG_6B50D3BF,
                            configuration.rpcTimeout())
                        : String.format(
                            R2dbcMessages.EXCEPTION_STATEMENT_EXECUTION_FAILED_ARG_D1EB77EA,
                            error.getMessage());
                return Mono.error(ExceptionMapper.mapTransportFailure(message, error));
              });
        });
  }

  /** Reads the next page of an active query; only called while the previous page is exhausted. */
  Mono<TSFetchResultsResp> fetchNextPage(long queryId, String sql, int fetchSize) {
    return Mono.defer(
        () -> {
          if (!open.get()) {
            return Mono.error(
                new IoTDBR2dbcException(R2dbcMessages.EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7));
          }
          TSFetchResultsReq request =
              new TSFetchResultsReq(sessionId, sql, effectiveFetchSize(fetchSize), queryId, true)
                  .setStatementId(statementId);
          long serverTimeout = serverQueryTimeoutMillis();
          if (serverTimeout > 0) {
            request.setTimeout(serverTimeout);
          }
          // An empty page (hasResultSet=false) is the end-of-results signal used by the
          // reference client; the cursor stops paging on it.
          return call(client -> client.fetchResults(request))
              .doOnNext(response -> ExceptionMapper.verifySuccess(response.getStatus(), sql));
        });
  }

  private Mono<TSExecuteStatementResp> executeStatement(String sql, int fetchSize) {
    TSExecuteStatementReq request =
        new TSExecuteStatementReq(sessionId, sql, statementId)
            .setFetchSize(fetchSize)
            .setEnableRedirectQuery(false);
    long serverTimeout = serverQueryTimeoutMillis();
    if (serverTimeout > 0) {
      request.setTimeout(serverTimeout);
    }
    return call(client -> client.executeStatement(request))
        .doOnNext(response -> ExceptionMapper.verifySuccess(response.getStatus(), sql));
  }

  private Mono<TSExecuteStatementResp> executePreparedStatement(
      String sql, List<ParameterBinding> parameters, int fetchSize, String statementName) {
    return call(client -> client.prepareStatement(new TSPrepareReq(sessionId, sql, statementName)))
        .doOnNext(
            response -> {
              ExceptionMapper.verifySuccess(response.getStatus(), sql);
              int parameterCount =
                  response.isSetParameterCount() ? response.getParameterCount() : 0;
              if (parameterCount != parameters.size()) {
                throw new IoTDBR2dbcException(
                    String.format(
                        R2dbcMessages
                            .EXCEPTION_THE_STATEMENT_HAS_ARG_PLACEHOLDERS_BUT_ARG_PARAMETERS_WERE_SUPPLIED_1295275A,
                        parameterCount,
                        parameters.size()));
              }
            })
        .flatMap(
            ignored -> {
              TSExecutePreparedReq request =
                  new TSExecutePreparedReq()
                      .setSessionId(sessionId)
                      .setStatementName(statementName)
                      .setParameters(
                          PreparedParameterSerde.serialize(
                              encodedValues(parameters), jdbcTypes(parameters), parameters.size()))
                      .setStatementId(statementId)
                      .setFetchSize(fetchSize);
              long serverTimeout = serverQueryTimeoutMillis();
              if (serverTimeout > 0) {
                request.setTimeout(serverTimeout);
              }
              return call(client -> client.executePreparedStatement(request));
            })
        .doOnNext(response -> ExceptionMapper.verifySuccess(response.getStatus(), sql))
        .onErrorResume(
            error ->
                releasePreparedStatement(statementName)
                    .onErrorResume(ignored -> Mono.empty())
                    .then(Mono.error(error)));
  }

  private Mono<Void> releasePreparedStatement(String statementName) {
    TSDeallocatePreparedReq request = new TSDeallocatePreparedReq(sessionId, statementName);
    return call(client -> client.deallocatePreparedStatement(request))
        .doOnNext(status -> ExceptionMapper.verifySuccess(status, null))
        .then();
  }

  private IoTDBResult buildResult(
      String sql, TSExecuteStatementResp response, int fetchSize, String preparedStatementName) {
    if (!IoTDBQueryCursor.hasResultSet(response)) {
      statementActive.set(false);
      return IoTDBResult.empty();
    }
    IoTDBQueryCursor cursor =
        IoTDBQueryCursor.from(
            this,
            sql,
            response,
            effectiveFetchSize(fetchSize),
            preparedStatementName,
            scheduler,
            timeFactor,
            configuration.zoneId());
    activeCursor = cursor;
    return IoTDBResult.of(cursor);
  }

  /**
   * Releases the query handle and, for prepared statements, the server side prepared statement.
   *
   * <p>The requests are queued behind any request that is still in flight. A failed cleanup
   * destroys the channel, because the protocol state can no longer be trusted.
   */
  Mono<Void> releaseQuery(long queryId, String preparedStatementName) {
    return Mono.defer(
        () -> {
          activeCursor = null;
          CompletableFuture<Void> released = new CompletableFuture<>();
          cleanupTail = released;
          Mono<Void> cleanup = Mono.empty();
          if (open.get()) {
            if (queryId > 0) {
              TSCloseOperationReq request =
                  new TSCloseOperationReq(sessionId)
                      .setStatementId(statementId)
                      .setQueryId(queryId);
              cleanup =
                  cleanup.then(
                      call(client -> client.closeOperation(request))
                          .doOnNext(status -> ExceptionMapper.verifySuccess(status, null))
                          .then());
            }
            if (preparedStatementName != null) {
              cleanup = cleanup.then(releasePreparedStatement(preparedStatementName));
            }
          }
          return cleanup
              .timeout(configuration.cleanupTimeout())
              .doFinally(
                  signal -> {
                    statementActive.set(false);
                    released.complete(null);
                  })
              .onErrorResume(
                  error -> {
                    LOGGER.warn(
                        R2dbcMessages.LOG_CLEANING_UP_THE_QUERY_HANDLE_FAILED_DE915B4D, error);
                    abort();
                    return Mono.empty();
                  });
        });
  }

  /** Queues an RPC behind the previous one; the transport allows a single request in flight. */
  private <T> Mono<T> call(Function<IoTDBAsyncClient, CompletableFuture<T>> rpc) {
    CompletableFuture<T> future;
    synchronized (rpcLock) {
      CompletableFuture<Void> previous = rpcTail;
      future =
          previous.handle((value, error) -> (Void) null).thenCompose(ignored -> rpc.apply(client));
      rpcTail = future.handle((value, error) -> (Void) null);
    }
    return Mono.fromCompletionStage(future);
  }

  private static Object[] encodedValues(List<ParameterBinding> parameters) {
    Object[] values = new Object[parameters.size()];
    for (int i = 0; i < parameters.size(); i++) {
      values[i] = parameters.get(i).encodedValue();
    }
    return values;
  }

  private static int[] jdbcTypes(List<ParameterBinding> parameters) {
    int[] types = new int[parameters.size()];
    for (int i = 0; i < parameters.size(); i++) {
      types[i] = parameters.get(i).jdbcType();
    }
    return types;
  }

  private static boolean isQueryStatement(String sql) {
    String normalized = sql.stripLeading().toLowerCase(Locale.ROOT);
    return normalized.startsWith("select") || normalized.startsWith("with");
  }

  private boolean canUsePreparedStatement(String sql, List<ParameterBinding> parameters) {
    if (configuration.sqlDialect() != IoTDBConnectionConfiguration.SqlDialect.TABLE
        || !isQueryStatement(sql)) {
      return false;
    }
    for (ParameterBinding parameter : parameters) {
      if (!parameter.supportsTypedEncoding()) {
        return false;
      }
    }
    return true;
  }

  private String preparedName() {
    // Every execution gets its own name so a statement that failed after prepare cannot collide
    // with the next one.
    return "r2dbc_" + statementId + "_" + preparedStatementCounter.incrementAndGet();
  }

  /** Converts temporal parameters into the epoch ticks verified for the session precision. */
  private List<ParameterBinding> toWireParameters(List<ParameterBinding> parameters) {
    if (parameters.isEmpty()) {
      return parameters;
    }
    List<ParameterBinding> wireParameters = new ArrayList<>(parameters.size());
    for (ParameterBinding parameter : parameters) {
      wireParameters.add(
          TimeValueConverter.toWireParameter(parameter, timeFactor, configuration.zoneId()));
    }
    return wireParameters;
  }

  private int effectiveFetchSize(int requested) {
    return requested > 0 ? requested : configuration.fetchSize();
  }

  /** Server query budget in milliseconds; zero follows the server default. */
  private long serverQueryTimeoutMillis() {
    Duration timeout = configuration.serverQueryTimeout();
    if (timeout == null || timeout.isZero()) {
      return 0;
    }
    long millis = timeout.toMillis();
    return timeout.getNano() % 1_000_000 == 0 ? millis : millis + 1;
  }

  private static boolean isTimeout(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof TimeoutException) {
        return true;
      }
      if (current instanceof TTransportException
          && ((TTransportException) current).getType() == TTransportException.TIMED_OUT) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static IoTDBR2dbcException stillHasActiveResult() {
    return new IoTDBR2dbcException(
        R2dbcMessages
            .EXCEPTION_THE_CONNECTION_STILL_HAS_AN_ACTIVE_RESULT_CLOSE_IT_BEFORE_STARTING_ANOTHER_OPERATION_4D9EA1CA);
  }

  private void abort() {
    if (open.compareAndSet(true, false)) {
      client.close();
    }
  }

  @Override
  public Statement createStatement(String sql) {
    requireOpen();
    return new IoTDBStatement(this, sql);
  }

  @Override
  public Batch createBatch() {
    requireOpen();
    return new IoTDBBatch(this);
  }

  @Override
  public boolean isAutoCommit() {
    return true;
  }

  @Override
  public Publisher<Void> setAutoCommit(boolean autoCommit) {
    if (!autoCommit) {
      return Mono.error(
          new IoTDBR2dbcException(
              R2dbcMessages
                  .EXCEPTION_SETAUTOCOMMIT_FALSE_IS_NOT_SUPPORTED_BECAUSE_IOTDB_HAS_NO_CROSS_STATEMENT_TRANSACTIONS_962579D8));
    }
    return Mono.empty();
  }

  @Override
  public Publisher<Void> beginTransaction() {
    return Mono.error(
        new IoTDBR2dbcException(
            R2dbcMessages.EXCEPTION_TRANSACTIONS_ARE_NOT_SUPPORTED_BY_IOTDB_C248F946));
  }

  @Override
  public Publisher<Void> beginTransaction(TransactionDefinition definition) {
    return beginTransaction();
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return Mono.error(
        new IoTDBR2dbcException(
            R2dbcMessages.EXCEPTION_TRANSACTIONS_ARE_NOT_SUPPORTED_BY_IOTDB_C248F946));
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return Mono.error(
        new IoTDBR2dbcException(
            R2dbcMessages.EXCEPTION_TRANSACTIONS_ARE_NOT_SUPPORTED_BY_IOTDB_C248F946));
  }

  @Override
  public Publisher<Void> createSavepoint(String name) {
    return Mono.error(
        new IoTDBR2dbcException(
            R2dbcMessages.EXCEPTION_SAVEPOINTS_ARE_NOT_SUPPORTED_BY_IOTDB_9BA2D2AD));
  }

  @Override
  public Publisher<Void> releaseSavepoint(String name) {
    return createSavepoint(name);
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String name) {
    return createSavepoint(name);
  }

  @Override
  public IsolationLevel getTransactionIsolationLevel() {
    return null;
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    return Mono.error(
        new IoTDBR2dbcException(
            R2dbcMessages
                .EXCEPTION_TRANSACTION_ISOLATION_LEVELS_CANNOT_BE_CHANGED_BECAUSE_IOTDB_HAS_NO_CROSS_STATEMENT_TRANSACTIONS_54D87733));
  }

  @Override
  public Publisher<Void> setLockWaitTimeout(Duration timeout) {
    return Mono.error(
        new IoTDBR2dbcException(
            R2dbcMessages.EXCEPTION_LOCK_WAIT_TIMEOUTS_ARE_NOT_SUPPORTED_BY_IOTDB_B93F52F8));
  }

  @Override
  public Publisher<Void> setStatementTimeout(Duration timeout) {
    if (timeout == null || timeout.isNegative()) {
      return Mono.error(
          new IllegalArgumentException(
              String.format(
                  R2dbcMessages
                      .EXCEPTION_THE_STATEMENT_TIMEOUT_MUST_NOT_BE_NEGATIVE_BUT_WAS_ARG_74BD2508,
                  timeout)));
    }
    this.statementTimeout = timeout;
    return Mono.empty();
  }

  @Override
  public ConnectionMetadata getMetadata() {
    return new IoTDBConnectionMetadata(serverVersion);
  }

  @Override
  public Publisher<Boolean> validate(ValidationDepth depth) {
    // The state is read when the returned publisher is subscribed, not when it is created, so a
    // validation created before close() correctly reports a closed connection.
    return Mono.defer(
        () -> {
          if (!open.get()) {
            return Mono.just(false);
          }
          if (depth == ValidationDepth.LOCAL) {
            return Mono.just(true);
          }
          return call(client -> client.getProperties())
              .map(properties -> true)
              .onErrorResume(error -> Mono.just(false))
              .defaultIfEmpty(false);
        });
  }

  @Override
  public Publisher<Void> close() {
    return Mono.defer(
        () -> {
          if (!open.compareAndSet(true, false)) {
            return Mono.empty();
          }
          IoTDBQueryCursor cursor = activeCursor;
          if (cursor != null) {
            cursor.close();
          }
          return call(client -> client.closeSession(new TSCloseSessionReq(sessionId)))
              .doOnNext(status -> ExceptionMapper.verifySuccess(status, null))
              .timeout(configuration.cleanupTimeout())
              .onErrorResume(
                  error -> {
                    LOGGER.warn(
                        R2dbcMessages.LOG_CLOSING_THE_R2DBC_CONNECTION_FAILED_D2D86252, error);
                    return Mono.empty();
                  })
              .then()
              .doFinally(signal -> client.close())
              .doOnSuccess(
                  ignored ->
                      LOGGER.debug(R2dbcMessages.LOG_IOTDB_R2DBC_CONNECTION_CLOSED_FAA1CE20));
        });
  }

  @Override
  public Publisher<Void> postAllocate() {
    return open.get()
        ? Mono.empty()
        : Mono.error(
            new IoTDBR2dbcException(R2dbcMessages.EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7));
  }

  @Override
  public Publisher<Void> preRelease() {
    if (!open.get()) {
      return Mono.error(
          new IoTDBR2dbcException(R2dbcMessages.EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7));
    }
    IoTDBQueryCursor cursor = activeCursor;
    if (cursor != null) {
      cursor.close();
    }
    // Wait for both the queued requests and the cleanup bookkeeping: the cleanup flag is cleared
    // after the close requests completed, so a connection is only reusable once this settles.
    CompletableFuture<Void> requests;
    CompletableFuture<Void> cleanup;
    synchronized (rpcLock) {
      requests = rpcTail;
      cleanup = cleanupTail;
    }
    return Mono.fromCompletionStage(requests)
        .then(Mono.fromCompletionStage(cleanup))
        .then(
            Mono.fromRunnable(
                () -> {
                  if (statementActive.get()) {
                    throw stillHasActiveResult();
                  }
                }));
  }

  private void requireOpen() {
    if (!open.get()) {
      throw new IllegalStateException(R2dbcMessages.EXCEPTION_THE_CONNECTION_IS_CLOSED_AB73B8A7);
    }
  }

  /** Values collected during the session handshake. */
  private static final class Handshake {

    private final long sessionId;
    private final int timeFactor;
    private long statementId;
    private String serverVersion;

    private Handshake(TSOpenSessionResp response) {
      this.sessionId = response.getSessionId();
      this.timeFactor = timeFactor(response);
    }
  }

  private static final class IoTDBConnectionMetadata implements ConnectionMetadata {

    private final String version;

    private IoTDBConnectionMetadata(String version) {
      this.version = version;
    }

    @Override
    public String getDatabaseProductName() {
      return DATABASE_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseVersion() {
      return version == null ? "" : version;
    }
  }
}
