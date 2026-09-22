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

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.ConnectionFactoryProvider;

/** ServiceLoader entry point, registered in {@code META-INF/services}. */
public final class IoTDBR2dbcConnectionFactoryProvider implements ConnectionFactoryProvider {

  @Override
  public ConnectionFactory create(ConnectionFactoryOptions options) {
    return new IoTDBR2dbcConnectionFactory(IoTDBConnectionConfiguration.from(options));
  }

  @Override
  public boolean supports(ConnectionFactoryOptions options) {
    return IoTDBConnectionConfiguration.DRIVER.equals(
        options.getValue(ConnectionFactoryOptions.DRIVER));
  }

  @Override
  public String getDriver() {
    return IoTDBConnectionConfiguration.DRIVER;
  }
}
