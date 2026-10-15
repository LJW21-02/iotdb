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

/**
 * A TLS failure: the key material could not be loaded, or the endpoint rejected the handshake.
 *
 * <p>The reason is the driver's own text, because the failure the operating system or the TLS stack
 * reports differs per platform; it stays available as the cause. Callers can therefore treat every
 * TLS problem the same way, for example to avoid retrying a rejected certificate.
 */
public class IoTDBTlsException extends IoTDBR2dbcException {

  public IoTDBTlsException(String reason, Throwable cause) {
    super(reason, cause);
  }
}
