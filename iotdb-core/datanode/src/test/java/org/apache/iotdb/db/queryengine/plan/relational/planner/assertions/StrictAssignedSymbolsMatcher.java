/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;

import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class StrictAssignedSymbolsMatcher extends BaseStrictSymbolsMatcher {
  private final Collection<? extends RvalueMatcher> getExpected;

  public StrictAssignedSymbolsMatcher(
      Function<PlanNode, Set<Symbol>> getActual, Collection<? extends RvalueMatcher> getExpected) {
    super(getActual);
    this.getExpected = requireNonNull(getExpected, "getExpected is null");
  }

  @Override
  protected Set<Symbol> getExpectedSymbols(
      PlanNode node, SessionInfo session, Metadata metadata, SymbolAliases symbolAliases) {
    ImmutableSet.Builder<Symbol> expected = ImmutableSet.builder();
    for (RvalueMatcher matcher : getExpected) {
      Optional<Symbol> assigned = matcher.getAssignedSymbol(node, session, metadata, symbolAliases);
      if (!assigned.isPresent()) {
        return null;
      }

      expected.add(assigned.get());
    }

    return expected.build();
  }

  public static Function<PlanNode, Set<Symbol>> actualAssignments() {
    return node -> ((ProjectNode) node).getAssignments().getSymbols();
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("exact assignments", getExpected).toString();
  }
}
