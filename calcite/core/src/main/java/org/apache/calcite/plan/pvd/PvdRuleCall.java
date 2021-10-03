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

public class PvdRuleCall extends RelOptRuleCall {
  //~ Instance fields --------------------------------------------------------

  protected Step nextStep;
  protected Step input;


  //~ Constructors -----------------------------------------------------------

  PvdRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeChildren,
     @Nullable List<RelNode> parents,
      Step input) {
    super(planner, operand, rels, nodeChildren, parents);
    this.input = input;
    nextStep = input.copy();
  }

  PvdRuleCall(
      RelOptPlanner planner,
      RelOptRuleOperand operand,
      RelNode[] rels,
      Map<RelNode, List<RelNode>> nodeChildren,
      Step input) {
    this(planner, operand, rels, nodeChildren, null, input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv,
      RelHintsPropagator handler) {
    //create a copy of of tree and replace with equiv
    nextStep.addStep(this);
    nextStep.replacePtr(rel);
    //add new ds to ds and create a new step;
    if (rule instanceof PvdPatternRule){
      PvdPatternRule pvd_rule = (PvdPatternRule) (rule);
      PvdPattern ds = pvd_rule.build();
      nextStep.addOpt(ds);
    }
  }

  public Step getInput(){
    return input;
  }


  public boolean equals(Object obj){
    if (!(obj instanceof PvdRuleCall))
      return false;
    PvdRuleCall other = (PvdRuleCall) obj;
    return this.rule.equals(other.getRule()) &&
        this.operand0.equals(other.getOperand0());
  }

  public String toString(){
    return this.rule.toString();
  }

  public Step step(){
    return nextStep;
  }

}
