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
import org.apache.iotdb.rpc.TSStatusCode;

import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcTimeoutException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
}
