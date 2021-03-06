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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Any;

import java.util.List;


public final class LogicalAny extends Any {
  //~ Constructors -----------------------------------------------------------
  private static int ID = 0;
  private int nodeID;

  /**
   * Creates a LogicalAny.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalAny(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelNode> inputs) {
    super(cluster, traitSet, inputs);
    nodeID = ++ID;
    if (ID < 0){
      throw new RuntimeException("Maximum diffnodes created.");
    }
  }


  /**
   * Creates a LogicalAny by parsing serialized output.
   */
  public LogicalAny(RelInput input) {
    super(input);
  }

  /** Creates a LogicalAny. */
  public static LogicalAny create(List<RelNode> inputs) {
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalAny(cluster, traitSet, inputs);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalAny copy(
      RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalAny(getCluster(), traitSet, inputs);
  }

  public int getNodeID(){
    return nodeID;
  }

  public int nodeID(){
    return nodeID;
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return this;
  }
}
