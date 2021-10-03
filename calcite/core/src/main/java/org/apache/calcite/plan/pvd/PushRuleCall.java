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
package org.apache.calcite.plan.pvd;

import org.apache.calcite.plan.RelHintsPropagator;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.plan.PvdPatternRule;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PushRuleCall extends PvdRuleCall {
  //~ Instance fields --------------------------------------------------------
  int nodeID;

  //~ Constructors -----------------------------------------------------------

  PushRuleCall(
      RelOptPlanner planner, RelOptRuleOperand operand, RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeChildren, Step input, int nodeID) {
    super(planner, operand, rels, nodeChildren, input.copy());
    this.nodeID = nodeID;
  }

  public static PushRuleCall create(PvdRuleCall call, int nodeID){
    return new PushRuleCall(call.getPlanner(), call.getOperand0(), call.getRels(),
        call.getNodeInputs(), call.getInput().copy(), nodeID);
  }

  //~ Methods ----------------------------------------------------------------


  public int nodeID(){
    return nodeID;
  }

}
