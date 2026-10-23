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

import org.apache.iotdb.r2dbc.exception.IoTDBR2dbcException;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import io.r2dbc.spi.Result;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IoTDBResultTest {

  @Test
  public void emptyResultCompletesWithoutFabricatingAnUpdateCount() {
    StepVerifier.create(IoTDBResult.empty().getRowsUpdated()).verifyComplete();
  }

  @Test
  public void aResultCanBeConsumedOnlyOnce() {
    IoTDBResult result = IoTDBResult.empty();
    result.getRowsUpdated();
    assertThrows(IoTDBR2dbcException.class, () -> result.map((row, metadata) -> row));
  }

  @Test
  public void filterReturnsANewUsableView() {
    IoTDBResult result = IoTDBResult.empty();
    Result filtered = result.filter(segment -> true);
    StepVerifier.create(filtered.map((row, metadata) -> row)).verifyComplete();
    assertThrows(IoTDBR2dbcException.class, () -> result.map((row, metadata) -> row));
  }

  @Test
  public void consumingAFilteredViewAlsoConsumesTheParent() {
    IoTDBResult result = IoTDBResult.empty();
    Result filtered = result.filter(segment -> true);
    filtered.getRowsUpdated();
    assertThrows(IoTDBR2dbcException.class, () -> result.map((row, metadata) -> row));
    assertThrows(IoTDBR2dbcException.class, () -> filtered.map((row, metadata) -> row));
  }

  @Test
  public void followingResultWaitsForPreviousCursorRelease() {
    Sinks.Empty<Void> release = Sinks.empty();
    AtomicBoolean nextStarted = new AtomicBoolean();
    IoTDBResult first = IoTDBResult.of(Flux.empty(), release.asMono(), () -> {});
    IoTDBResult second = IoTDBResult.empty();

    StepVerifier.create(
            first.followedBy(
                Mono.defer(
                    () -> {
                      nextStarted.set(true);
                      return Mono.just(second);
                    })))
        .expectNext(first)
        .then(() -> assertFalse(nextStarted.get()))
        .then(() -> release.tryEmitEmpty())
        .expectNext(second)
        .verifyComplete();
  }

  @Test
  public void cancellingAWaitingSequenceDiscardsTheActiveResult() {
    AtomicBoolean discarded = new AtomicBoolean();
    IoTDBResult first = IoTDBResult.of(Flux.empty(), Mono.never(), () -> discarded.set(true));

    Disposable subscription = first.followedBy(Mono.just(IoTDBResult.empty())).subscribe();
    subscription.dispose();

    assertTrue(discarded.get());
  }

  @Test
  public void consumingRowsUpdatedDiscardsTheQueryCursorAndAllowsTheSequenceToContinue() {
    Sinks.Empty<Void> release = Sinks.empty();
    AtomicBoolean discarded = new AtomicBoolean();
    AtomicBoolean nextStarted = new AtomicBoolean();
    IoTDBResult first =
        IoTDBResult.of(
            Flux.empty(),
            release.asMono(),
            () -> {
              discarded.set(true);
              release.tryEmitEmpty();
            });
    IoTDBResult second = IoTDBResult.empty();

    StepVerifier.create(
            first
                .followedBy(
                    Mono.defer(
                        () -> {
                          nextStarted.set(true);
                          return Mono.just(second);
                        }))
                .flatMap(result -> Flux.from(result.getRowsUpdated()).then(Mono.just(result))))
        .expectNext(first)
        .expectNext(second)
        .verifyComplete();

    assertTrue(discarded.get());
    assertTrue(nextStarted.get());
  }

  @Test
  public void cancellingTheWaitDoesNotCancelTheCursorReleaseSignal() throws Exception {
    IoTDBQueryCursor cursor =
        IoTDBQueryCursor.from(
            null,
            "SELECT 1",
            new TSExecuteStatementResp(),
            1,
            null,
            Schedulers.immediate(),
            1_000,
            ZoneId.of("UTC"));
    Field field = IoTDBQueryCursor.class.getDeclaredField("releaseCompletion");
    field.setAccessible(true);
    CompletableFuture<?> release = (CompletableFuture<?>) field.get(cursor);

    Disposable subscription = cursor.releaseCompletion().subscribe();
    subscription.dispose();

    assertFalse(release.isCancelled());
  }
}
