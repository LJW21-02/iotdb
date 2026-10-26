/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.iotdb.commons.conf.CommonDescriptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/** Starts the unchanged server with fresh, private fixture credentials. */
public final class ServerLauncher {
  private ServerLauncher() {}

  public static void main(String[] arguments) throws Exception {
    Properties properties = new Properties();
    try (var input = Files.newInputStream(Path.of(System.getProperty("fixture.credentials")))) {
      properties.load(input);
    }
    var common = CommonDescriptor.getInstance().getConfig();
    common.setAdminName(properties.getProperty("username"));
    common.setAdminPassword(properties.getProperty("password"));
    Class.forName(arguments[0])
        .getMethod("main", String[].class)
        .invoke(null, (Object) new String[] {"-s"});
  }
}
