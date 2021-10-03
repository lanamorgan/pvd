/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.logical;

import org.apache.calcite.linq4j.Ord;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateFieldCall;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.ArrayList;

/**
 * <code>LogicalAggregate</code> is a relational operator which eliminates
 * duplicates and computes totals.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}.
 * </ul>
 */
public class LogicalFieldAggregate extends Aggregate {
  //~ Constructors -----------------------------------------------------------

  private List <RexNode> groupList;
  private List <AggregateFieldCall> aggFieldCalls;
  /**
   * Creates a LogicalAggregate.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster    Cluster that this relational expression belongs to
   * @param traitSet   Traits
   * @param hints      Hints for this relational expression
   * @param input      Input relational expression
   * @param groupSet Bit set of grouping fields
   * @param groupSets Grouping sets, or null to use just {@code groupSet}
   * @param aggCalls Array of aggregates to compute, not null
   */
  public LogicalFieldAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      List<AggregateCall> aggCalls,
      List <RexNode> groupList) {
    super(cluster, traitSet, hints, input, ImmutableBitSet.of(), null, aggCalls);
    aggFieldCalls = new ArrayList<AggregateFieldCall>();
    for(AggregateCall call: aggCalls){
      assert (call instanceof AggregateFieldCall);
      aggFieldCalls.add((AggregateFieldCall) call);
    }
    this.groupList = groupList;
  }

  /**
   * Creates a LogicalAggregate by parsing serialized output.
   */
  public LogicalFieldAggregate(RelInput input) {
    super(input);
  }

  /** Creates a LogicalAggregate. */
  public static LogicalFieldAggregate create(final RelNode input,
      List<AggregateCall> aggCalls,
      List<RexNode> groupList) {
    return create_(input, ImmutableList.of(), aggCalls, groupList);
  }


  private static LogicalFieldAggregate create_(final RelNode input,
      List<RelHint> hints,
      List<AggregateCall> aggCalls,
      List<RexNode> groupList) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalFieldAggregate(cluster, traitSet, hints, input,  aggCalls, groupList);
  }

  //~ Methods ----------------------------------------------------------------
  public List<RexNode> getGroupList(){
    return ImmutableList.copyOf(groupList);
  }

  public List<AggregateFieldCall> getAggFieldCalls(){
    return ImmutableList.copyOf(aggFieldCalls);
  }

  @Override public LogicalFieldAggregate copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    return LogicalFieldAggregate.create_(input, hints, aggCalls, groupList);
  }


  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new LogicalFieldAggregate(getCluster(), traitSet, hintList, input, aggCalls, groupList);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    // We skip the "groups" element if it is a singleton of "group".
    super.explainTerms(pw)
        .itemIf("group", groupList, !groupList.isEmpty());
    return pw;
  }
}
