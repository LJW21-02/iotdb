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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/** Stops only a fixture JVM whose PID and exact IOTDB_HOME both match. */
public final class FixtureNodeControl {
  private FixtureNodeControl() {}

  public static void main(String[] arguments) throws Exception {
    long pid = Long.parseLong(arguments[0]);
    Path expectedHome = Path.of(arguments[1]).toRealPath();
    var handle = ProcessHandle.of(pid);
    if (handle.isEmpty() || !handle.get().isAlive()) {
      return;
    }
    var process = handle.get();
    String[] command = process.info().arguments().orElseThrow();
    boolean launcher = Arrays.asList(command).contains("ServerLauncher");
    boolean home = false;
    for (String argument : command) {
      if (argument.startsWith("-DIOTDB_HOME=")) {
        home = Path.of(argument.substring("-DIOTDB_HOME=".length())).toRealPath().equals(expectedHome);
      }
    }
    if (!launcher || !home) {
      throw new IllegalArgumentException("Fixture process identity does not match");
    }
    process.destroyForcibly();
    process.onExit().get(20, TimeUnit.SECONDS);
  }
}
