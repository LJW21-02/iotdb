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
package org.apache.iotdb.r2dbc.spring.sample;

import org.apache.iotdb.r2dbc.spring.IoTDBRepository;

import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/** Spring Data generates the read SQL; writes use explicit IoTDB SQL and completion semantics. */
public interface ReadingRepository extends IoTDBRepository<Reading, LocalDateTime> {
  Flux<Reading> findByValueGreaterThanOrderByTimeAsc(Integer value, Pageable pageable);

  Mono<Reading> findByTime(LocalDateTime time);

  Mono<Long> countByValueGreaterThan(Integer value);

  @Query("SELECT time, v FROM readings WHERE v = :value OR v = :value ORDER BY time")
  Flux<Reading> findWithRepeatedParameter(@Param("value") Integer value);

  @Modifying
  @Query("INSERT INTO readings(time, v) VALUES (:time, :value)")
  Mono<Void> insert(@Param("time") LocalDateTime time, @Param("value") Integer value);

  @Modifying
  @Query("DELETE FROM readings WHERE time <= :time")
  Mono<Void> deleteThrough(@Param("time") LocalDateTime time);
}
