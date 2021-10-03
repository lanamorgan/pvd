package org.apache.calcite.plan.pvd;


import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.ExpressionRule;
import org.apache.calcite.util.Pair;


import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.Nullable;

public class DfsDriver extends PvdRuleDriver{

  private ArrayDeque<Step> stack;
  private Set<Step> plans;

  public DfsDriver(PvdPlanner planner) {
    super(planner);
    stack = new ArrayDeque<Step>();
    plans = new HashSet<Step>();
  }


  public void drive() {
    Set<PvdPattern> opts = new LinkedHashSet<PvdPattern>();
    stack.push(new Step(opts, planner.getRoot(), planner.getRoot()));
    List<RelOptRule> rules = planner.getRules();
    int count = 0;
    Set<Integer> seenPlans = new HashSet<Integer>();
    while (stack.peek() != null) {
      Step currStep = stack.pop();
      //add children
      List<RelNode> inputs = currStep.getPtr().getInputs();
      for (int i = 0; i < inputs.size(); i++) {
        Step stepChild = currStep.copy();
        stepChild.addPtrPath(i);
        stepChild.setPtr(inputs.get(i));
        stack.push(stepChild);
      }

      for (RelOptRule rule : rules) {

        final List<RelNode> rels = new ArrayList<>();
        final Map<RelNode, List<RelNode>> nodeChildren = new HashMap<>();
        boolean match =
            matchOperands(rule.getOperand(), currStep.getPtr(), rels, nodeChildren);
        if (match && rule instanceof ExpressionRule){
          PvdRuleCall tempCall = new PvdRuleCall(planner,
              rule.getOperand(),
              rels.toArray(new RelNode[rels.size()]),
              nodeChildren,
              currStep);
          ExpressionRule expRule = (ExpressionRule) rule;
          List<PvdRuleCall> ruleCalls = expRule.findAllMatches(tempCall);
          for (PvdRuleCall ruleCall : ruleCalls) {
            rule.onMatch(ruleCall);
            Step s = ruleCall.step();
            int hash = RelOptUtil.toString(s.getRoot()).hashCode();
            if(!seenPlans.contains(hash)){
              seenPlans.add(hash);
              stack.push(s);
              plans.add(s);
              count++;
            }
          }
        }
        else if (match) {
          PvdRuleCall ruleCall = new PvdRuleCall(planner,
              rule.getOperand(),
              rels.toArray(new RelNode[rels.size()]),
              nodeChildren,
              currStep);
          if (rule.matches(ruleCall)) {
            rule.onMatch(ruleCall);
            Step s = ruleCall.step();
            int hash = RelOptUtil.toString(s.getRoot()).hashCode();
            if(!seenPlans.contains(hash)){
              seenPlans.add(hash);
              stack.push(s);
              plans.add(s);
              count++;
            }
          }//end rule application
        } // end check match
      } //end rules
    }
    planner.setPlansFound(count);
  }
}
