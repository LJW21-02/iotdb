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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Timeout classification and status mapping, which decide the exception type a caller catches. */
public class ExceptionMapperTest {

  @Test
  public void thriftTimeoutIsReportedAsATimeout() {
    TTransportException thriftTimeout = new TTransportException(TTransportException.TIMED_OUT);

    assertTrue(ExceptionMapper.isTimeout(thriftTimeout));
    assertTrue(
        ExceptionMapper.mapTransportFailure("rpc", thriftTimeout) instanceof R2dbcTimeoutException);
  }

  @Test
  public void nestedTimeoutIsFoundAndReportedAsATimeout() {
    Throwable nested = new IOException("read failed", new TimeoutException("statement budget"));

    assertTrue(ExceptionMapper.isTimeout(nested));
    assertTrue(
        ExceptionMapper.mapTransportFailure("read", nested) instanceof R2dbcTimeoutException);
  }

  @Test
  public void otherTransportFailuresStayGenericAndAreNotTimeouts() {
    Throwable failure = new IOException("connection reset");

    assertFalse(ExceptionMapper.isTimeout(failure));
    assertTrue(ExceptionMapper.mapTransportFailure("read", failure) instanceof IoTDBR2dbcException);
  }

  @Test
  public void unknownStatusCodesKeepTheServerMessage() {
    TSStatus status = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    status.setMessage("boom");

    R2dbcException mapped = ExceptionMapper.mapStatus(status, "select 1");
    assertEquals(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), mapped.getErrorCode());
    assertTrue(mapped.getMessage().contains("boom"));
  }

  @Test
  public void aRedirectionRecommendationIsACompletedExecutionStatus() {
    TSStatus recommendation =
        new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
            .setSubStatus(
                java.util.List.of(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                        .setRedirectNode(new TEndPoint("127.0.0.1", 6668))));

    ExceptionMapper.verifyExecutionSuccess(recommendation, "insert into t values (1)");
  }

  @Test
  public void aRedirectionRecommendationWithSeveralTargetsIsStillSuccessful() {
    TSStatus recommendation =
        new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
            .setSubStatus(
                java.util.List.of(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                        .setRedirectNode(new TEndPoint("127.0.0.1", 6668)),
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                        .setRedirectNode(new TEndPoint("127.0.0.1", 6669))));

    ExceptionMapper.verifyExecutionSuccess(recommendation, "insert into t values (1)");
  }

  @Test
  public void aBareRecommendationIsNotAcceptedAsACompletedExecution() {
    TSStatus recommendation =
        new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
            .setRedirectNode(new TEndPoint("127.0.0.1", 6668));

    R2dbcException failure =
        assertThrows(
            R2dbcException.class,
            () ->
                ExceptionMapper.verifyExecutionSuccess(recommendation, "insert into t values (1)"));
    assertEquals(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode(), failure.getErrorCode());
  }

  @Test
  public void aRecommendationWithAFailingChildKeepsTheActualFailure() {
    TSStatus recommendation =
        new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
            .setSubStatus(
                java.util.List.of(
                    new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
                        .setRedirectNode(new TEndPoint("127.0.0.1", 6668)),
                    new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode())));

    assertThrows(
        R2dbcBadGrammarException.class,
        () -> ExceptionMapper.verifyExecutionSuccess(recommendation, "insert into t values (1)"));
  }

  @Test
  public void aRecommendationRequiresSuccessfulChildrenAndARoutingTarget() {
    TSStatus recommendation =
        new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode())
            .setSubStatus(
                java.util.List.of(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));

    assertThrows(
        IoTDBR2dbcException.class,
        () -> ExceptionMapper.verifyExecutionSuccess(recommendation, "insert into t values (1)"));

    recommendation.setSubStatus(
        java.util.List.of(
            new TSStatus(TSStatusCode.MULTIPLE_ERROR.getStatusCode())
                .setRedirectNode(new TEndPoint("127.0.0.1", 6668))));
    assertThrows(
        IoTDBR2dbcException.class,
        () -> ExceptionMapper.verifyExecutionSuccess(recommendation, "insert into t values (1)"));
  }

  @Test
  public void aRecommendationWithoutTargetStillFailsForOtherRpcStatuses() {
    TSStatus recommendation = new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());

    R2dbcException failure =
        assertThrows(
            R2dbcException.class, () -> ExceptionMapper.verifySuccess(recommendation, null));

    assertTrue(failure.getMessage().contains("without naming a target node"));
  }

  @Test
  public void aTlsConnectFailureIsReportedAsATlsFailure() {
    Throwable handshake =
        new IOException("handshake", new javax.net.ssl.SSLHandshakeException("peer closed"));

    R2dbcException mapped =
        ExceptionMapper.mapConnectFailure(
            "127.0.0.1", 6667, true, Duration.ofSeconds(10), handshake);

    assertTrue(mapped instanceof IoTDBTlsException);
    assertTrue(
        "a caller that knows only the driver exception must keep working",
        mapped instanceof IoTDBR2dbcException);
    assertSame(handshake, mapped.getCause());
    assertTrue(mapped.getMessage().contains("127.0.0.1:6667"));
    assertTrue(mapped.getMessage().contains("over TLS"));
  }

  @Test
  public void aPlaintextConnectFailureKeepsTheCoordinatesAndStaysGeneric() {
    Throwable refused = new ConnectException("Connection refused");

    R2dbcException mapped =
        ExceptionMapper.mapConnectFailure(
            "127.0.0.1", 6667, false, Duration.ofSeconds(10), refused);

    assertTrue(mapped instanceof IoTDBR2dbcException);
    assertFalse(mapped instanceof IoTDBTlsException);
    assertSame(refused, mapped.getCause());
    assertTrue(mapped.getMessage().contains("127.0.0.1:6667"));
  }

  @Test
  public void aConnectTimeoutIsReportedAsATimeoutAndKeepsTheCause() {
    TimeoutException budget = new TimeoutException("connect budget");

    R2dbcException mapped =
        ExceptionMapper.mapConnectFailure("127.0.0.1", 6667, true, Duration.ofSeconds(10), budget);

    assertTrue(mapped instanceof R2dbcTimeoutException);
    assertFalse(mapped instanceof IoTDBTlsException);
    assertSame(budget, mapped.getCause());
    assertTrue(mapped.getMessage().contains("PT10S"));
  }

  @Test
  public void aServerStatusOnAPageKeepsItsOwnTypeCodeAndSql() {
    TSStatus timeout = new TSStatus(TSStatusCode.QUERY_TIMEOUT.getStatusCode());
    timeout.setMessage("Current query is time out");
    R2dbcException fromExecute = ExceptionMapper.mapStatus(timeout, "select * from t");

    R2dbcException fromPage = ExceptionMapper.mapPageFailure(fromExecute);

    assertSame(
        "the page phase must not re classify a status the server already reported",
        fromExecute,
        fromPage);
    assertTrue(fromPage instanceof R2dbcTimeoutException);
    assertTrue(fromPage instanceof R2dbcTransientException);
    assertEquals(TSStatusCode.QUERY_TIMEOUT.getStatusCode(), fromPage.getErrorCode());
    assertEquals("select * from t", fromPage.getSql());
  }

  @Test
  public void aNonTimeoutServerStatusOnAPageKeepsItsSpecificType() {
    TSStatus semanticError = new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode());
    semanticError.setMessage("unknown column");

    R2dbcException fromPage =
        ExceptionMapper.mapPageFailure(ExceptionMapper.mapStatus(semanticError, "select x from t"));

    assertTrue(fromPage instanceof R2dbcBadGrammarException);
    assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), fromPage.getErrorCode());
  }

  @Test
  public void aTransportTimeoutOnAPageIsReportedAsATimeoutWithThePageContext() {
    TTransportException thriftTimeout = new TTransportException(TTransportException.TIMED_OUT);

    R2dbcException mapped = ExceptionMapper.mapPageFailure(thriftTimeout);

    assertTrue(mapped instanceof R2dbcTimeoutException);
    assertTrue(mapped.getMessage().contains("Reading the next page failed"));
  }

  @Test
  public void aPlainTransportFailureOnAPageStaysGenericAndKeepsThePageContext() {
    R2dbcException mapped = ExceptionMapper.mapPageFailure(new IOException("connection reset"));

    assertTrue(mapped instanceof IoTDBR2dbcException);
    assertFalse(mapped instanceof R2dbcTransientException);
    assertTrue(mapped.getMessage().contains("Reading the next page failed"));
  }
}
