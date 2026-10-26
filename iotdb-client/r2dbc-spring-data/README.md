<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->
# Spring Data R2DBC for IoTDB Table SQL

`iotdb-r2dbc-spring-data` adds Spring Data 3.3.8 integration to the non-blocking driver. It is a
separate artifact: the base `iotdb-r2dbc` driver has no Spring runtime dependency. The integration
uses Spring Framework 6.1.13 and its live test uses Spring Boot 3.3.8's
`spring-boot-starter-data-r2dbc`.

## Application dependencies and automatic wiring

Add this artifact alongside the Boot starter, with the same IoTDB version as the server/client
artifacts:

```xml
<dependency>
  <groupId>org.apache.iotdb</groupId>
  <artifactId>iotdb-r2dbc-spring-data</artifactId>
  <version>2.0.11-SNAPSHOT</version>
</dependency>
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-data-r2dbc</artifactId>
</dependency>
```

```properties
spring.r2dbc.url=r2dbc:iotdb://localhost:6667/measurements?iotdb.sqlDialect=table&iotdb.timeZone=UTC
spring.r2dbc.username=${IOTDB_USER}
spring.r2dbc.password=${IOTDB_PASSWORD}
```

Create the database and table before starting the application:

```sql
CREATE DATABASE IF NOT EXISTS measurements;
CREATE TABLE IF NOT EXISTS measurements.readings(v INT32 FIELD);
```

The artifact's `META-INF/spring.factories` registers both Spring's bind-marker resolver and Spring
Data's dialect resolver. Boot can create its default `DatabaseClient`, `R2dbcEntityTemplate` and
repository proxies; an explicit DatabaseClient bean or `namedParameters(false)` is unnecessary.
Spring expands `:name` parameters to zero-based anonymous `?` markers before the driver receives
SQL. Repeated named parameters receive separate positional bindings. `IoTDBDialect` renders Table
`LIMIT`/`OFFSET`, double-quoted identifiers and lower-case identifier normalization. It rejects row
locks and does not advertise arrays or single-query aggregate loading.

Use Table SQL connections only. The factory metadata names the database product and does not
expose its configured Tree/Table mode to Spring's resolver; the resolver cannot detect a factory
that was explicitly configured with the Tree dialect. This integration does not translate
relational queries into Tree paths.

## Query repositories and explicit writes

An entity can map an application-assigned time id when its table has no TAG columns. The default
driver TIMESTAMP value is `LocalDateTime`, so use that type for the mapped property:

```java
@Table("readings")
public record Reading(@Id @Column("time") LocalDateTime time, @Column("v") Integer value) {}

public interface ReadingRepository extends IoTDBRepository<Reading, LocalDateTime> {
  Flux<Reading> findByValueGreaterThanOrderByTimeAsc(Integer value, Pageable pageable);
  Mono<Reading> findByTime(LocalDateTime time);
  Mono<Long> countByValueGreaterThan(Integer value);

  @Modifying
  @Query("INSERT INTO readings(time, v) VALUES (:time, :value)")
  Mono<Void> insert(@Param("time") LocalDateTime time, @Param("value") Integer value);

  @Modifying
  @Query("DELETE FROM readings WHERE time <= :time")
  Mono<Void> deleteThrough(@Param("time") LocalDateTime time);
}
```

Put the entity and repository under the application's component/auto-configuration package, or use
`@EnableR2dbcRepositories` with the repository package. The runnable sample entity and interface are
in `src/test/java/org/apache/iotdb/r2dbc/spring/sample/`; the Boot test supplies no DatabaseClient,
entity-template or repository-factory beans.

```java
LocalDateTime timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(1000), ZoneOffset.UTC);
repository.insert(timestamp, 42)
    .then(repository.findByTime(timestamp))
    .subscribe(reading -> System.out.println(reading.value()));

Flux<Reading> secondPage = repository.findByValueGreaterThanOrderByTimeAsc(0, PageRequest.of(1, 20));
```

Choose timestamps representable at the server's configured precision and use the connection's time
zone consistently. UTC avoids local-clock ambiguity. Do not map a time property to raw `Long` and
assume Spring will convert the default `LocalDateTime` into epoch ticks. A custom mapping or an
explicit result projection is needed for a raw-tick domain model. When TAG columns exist, the real
row key comprises all TAG values and time: a single time id does not uniquely identify those rows.
Use explicit queries over the complete key for such tables.

The `IoTDBRepository` marker intentionally extends only Spring's `Repository`; it exposes no
standard CRUD `save`. Declare read methods with `Mono`/`Flux` and write methods with explicit
`@Query`, `@Modifying`, and `Mono<Void>`. A successful write publisher completes after execution,
without claiming that exactly one row changed. Spring Framework 6.1 `DatabaseClient.rowsUpdated()`
aggregates the driver's absent counts to zero; that zero is a framework aggregation identity, not
proof that the database changed no rows. Do not expose it as an affected-row result. The repository
write contract consumes completion only. Existing timestamps use IoTDB's native write
semantics, including any overwrite behavior; they are not transactional relational inserts.

Do not combine the marker with `ReactiveCrudRepository`/`R2dbcRepository`, and do not use
`R2dbcEntityTemplate.insert/update` as a substitute for these explicit writes: standard save/insert
paths request generated values, and update paths assume affected row counts. Transactions,
`@Transactional`, optimistic `@Version` writes, row locks, generated keys, affected row counts,
streaming LOB input, and Tree repositories remain unsupported database/driver capabilities. This
artifact does not emulate them. Derived read queries, Flux pagination and explicit completion-only
writes are the supported repository contract; it does not claim full reactive CRUD compatibility.

## Verification

From the repository root, build the new module and reactor dependencies:

```bash
mvn test -pl iotdb-client/r2dbc-spring-data -am -Dtest=IoTDBDialectTest -Dsurefire.failIfNoSpecifiedTests=false
mvn test -pl iotdb-client/r2dbc-spring-data -am -Dtest=IoTDBSpringDataRepositoryIT -Dsurefire.failIfNoSpecifiedTests=false \
  -Diotdb.it.host=127.0.0.1 -Diotdb.it.port=6667 \
  -Diotdb.it.user=USER -Diotdb.it.password=PASSWORD
```

`IoTDBDialectTest` verifies both resolver registrations without connecting, zero-based/repeated
anonymous marker bindings, pagination, identifier processing, unsupported arrays and rejected row
locks. `IoTDBSpringDataRepositoryIT` is skipped unless `iotdb.it.host` is supplied. It starts the Boot
starter's automatic configuration against the live server, then checks explicit inserts, entity
mapping, derived parameter binding, counts, two pages, repeated named parameters, default
DatabaseClient named binding, empty raw driver DML count streams (before Spring count aggregation), explicit deletion and error
propagation.
It disables pooling for this deterministic lifecycle fixture and closes the driver factory after
connections and context; it does not assert that a pool owns the delegate driver's shared runtime.
