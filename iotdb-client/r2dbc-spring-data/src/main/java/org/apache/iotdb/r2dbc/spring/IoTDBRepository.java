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
package org.apache.iotdb.r2dbc.spring;

import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.Repository;

/**
 * Marker for IoTDB Table query repositories created by Spring Data R2DBC.
 *
 * <p>Declare reactive derived query methods and explicit {@code @Query} methods. Writes must use
 * {@code @Modifying} with {@code Mono<Void>}; completion confirms execution, not an affected row
 * count. This interface deliberately exposes no CRUD save/update operations: IoTDB has no
 * cross-statement transaction, generated keys, or affected row count. Do not combine it with
 * ReactiveCrudRepository.
 *
 * <p>A single Spring id must identify a row uniquely. A time-only id is appropriate for a table
 * without TAG columns. Tables with TAG columns require explicit queries over all TAGs and time;
 * treating time alone as their primary key is incorrect.
 */
@NoRepositoryBean
public interface IoTDBRepository<T, ID> extends Repository<T, ID> {}
