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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.Test;
import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.BindMarkersFactoryResolver;
import org.springframework.r2dbc.core.binding.BindTarget;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class IoTDBDialectTest {
  @Test
  public void discoversBothSpringExtensionsWithoutOpeningAConnection() {
    ConnectionFactory factory = factory("Apache IoTDB");
    assertSame(IoTDBDialect.INSTANCE, DialectResolver.getDialect(factory));
    assertNotNull(DatabaseClient.create(factory));
    assertFalse(BindMarkersFactoryResolver.resolve(factory).identifiablePlaceholders());
    assertNull(new IoTDBBindMarkersProvider().getBindMarkers(factory("Other")));
    assertFalse(new IoTDBDialectProvider().getDialect(factory("Other")).isPresent());
  }

  @Test
  public void bindsRepeatedAnonymousMarkersAtZeroBasedIndexes() {
    BindMarkers markers = IoTDBDialect.INSTANCE.getBindMarkersFactory().create();
    RecordingTarget target = new RecordingTarget();
    assertEquals("?", markers.next("first").getPlaceholder());
    markers = IoTDBDialect.INSTANCE.getBindMarkersFactory().create();
    markers.next("first").bind(target, 7);
    markers.next("first").bind(target, 11);
    markers.next("nullable").bindNull(target, Integer.class);
    assertEquals(7, target.values.get(0));
    assertEquals(11, target.values.get(1));
    assertEquals(Integer.class, target.values.get(2));
  }

  @Test
  public void rendersTablePaginationAndIdentifiersAndRejectsLocks() {
    IoTDBDialect dialect = IoTDBDialect.INSTANCE;
    assertEquals("LIMIT 2", dialect.limit().getLimit(2));
    assertEquals("OFFSET 4", dialect.limit().getOffset(4));
    assertEquals("LIMIT 2 OFFSET 4", dialect.limit().getLimitOffset(2, 4));
    assertEquals("reading", dialect.getIdentifierProcessing().standardizeLetterCase("Reading"));
    assertEquals(
        "Reading", SqlIdentifier.unquoted("Reading").toSql(dialect.getIdentifierProcessing()));
    assertEquals(
        "\"MixedCase\"",
        SqlIdentifier.quoted("MixedCase").toSql(dialect.getIdentifierProcessing()));
    assertFalse(dialect.getArraySupport().isSupported());
    assertFalse(dialect.supportsSingleQueryLoading());
    UnsupportedOperationException failure =
        assertThrows(UnsupportedOperationException.class, () -> dialect.lock().getLock(null));
    assertEquals(
        SpringDataMessages.EXCEPTION_IOTDB_DOES_NOT_SUPPORT_ROW_LOCKS_8824EBC3,
        failure.getMessage());
  }

  private static ConnectionFactory factory(String name) {
    return new ConnectionFactory() {
      @Override
      public Publisher<? extends Connection> create() {
        throw new AssertionError("must not connect");
      }

      @Override
      public ConnectionFactoryMetadata getMetadata() {
        return () -> name;
      }
    };
  }

  private static final class RecordingTarget implements BindTarget {
    private final Map<Integer, Object> values = new LinkedHashMap<>();

    @Override
    public void bind(String identifier, Object value) {
      throw new AssertionError("must bind by index");
    }

    @Override
    public void bind(int index, Object value) {
      values.put(index, value);
    }

    @Override
    public void bindNull(String identifier, Class<?> type) {
      throw new AssertionError("must bind by index");
    }

    @Override
    public void bindNull(int index, Class<?> type) {
      values.put(index, type);
    }
  }
}
