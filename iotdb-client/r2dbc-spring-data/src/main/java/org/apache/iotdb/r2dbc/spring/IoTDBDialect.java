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

import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.relational.core.dialect.AbstractDialect;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.LockOptions;
import org.springframework.r2dbc.core.binding.BindMarkersFactory;

/** Spring Data SQL rendering for the IoTDB Table model. */
public final class IoTDBDialect extends AbstractDialect implements R2dbcDialect {

  public static final IoTDBDialect INSTANCE = new IoTDBDialect();

  private static final BindMarkersFactory BIND_MARKERS = BindMarkersFactory.anonymous("?");
  private static final IdentifierProcessing IDENTIFIERS =
      IdentifierProcessing.create(
          IdentifierProcessing.Quoting.ANSI, IdentifierProcessing.LetterCasing.LOWER_CASE);

  private static final LimitClause LIMIT =
      new LimitClause() {
        @Override
        public String getLimit(long limit) {
          return "LIMIT " + limit;
        }

        @Override
        public String getOffset(long offset) {
          return "OFFSET " + offset;
        }

        @Override
        public String getLimitOffset(long limit, long offset) {
          return "LIMIT " + limit + " OFFSET " + offset;
        }

        @Override
        public Position getClausePosition() {
          return Position.AFTER_ORDER_BY;
        }
      };

  private static final LockClause LOCK =
      new LockClause() {
        @Override
        public String getLock(LockOptions options) {
          throw new UnsupportedOperationException(
              SpringDataMessages.EXCEPTION_IOTDB_DOES_NOT_SUPPORT_ROW_LOCKS_8824EBC3);
        }

        @Override
        public Position getClausePosition() {
          return Position.AFTER_ORDER_BY;
        }
      };

  private IoTDBDialect() {}

  @Override
  public BindMarkersFactory getBindMarkersFactory() {
    return BIND_MARKERS;
  }

  @Override
  public LimitClause limit() {
    return LIMIT;
  }

  @Override
  public LockClause lock() {
    return LOCK;
  }

  @Override
  public IdentifierProcessing getIdentifierProcessing() {
    return IDENTIFIERS;
  }

  @Override
  public boolean supportsSingleQueryLoading() {
    return false;
  }
}
