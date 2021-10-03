package org.apache.calcite.plan.pvd;


import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.ExpressionRule;
import org.apache.calcite.rel.rules.PvdRules;
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

public class HeuristicDriver extends PvdRuleDriver{

  private ArrayDeque<Step> stack;
  private Set<Step> plans;
  private List<RelOptRule> partitionRules;
  private List<RelOptRule> pushRules;
  private List<RelOptRule> dsRules;
  private int count;

  public HeuristicDriver(PvdPlanner planner) {
    super(planner);
    stack = new ArrayDeque<Step>();
    plans = new HashSet<Step>();
    count = 0;
    addDefaultRules();
  }

  private void addDefaultRules(){
    partitionRules = PvdRules.PartitionRules;
    pushRules = PvdRules.PushRules;
    dsRules = PvdRules.DsRules;
  }

  private void driveSegment(List<RelOptRule> rules){
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
        if (match && rule instanceof ExpressionRule) {
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
            if (!seenPlans.contains(hash)) {
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
            if (!seenPlans.contains(hash)) {
              seenPlans.add(hash);
              stack.push(s);
              plans.add(s);
              count++;
            }
          }//end rule application
        } // end check match
      } //end rules
    } //end loop
  }

  public void drive() {
    Set<PvdPattern> opts = new LinkedHashSet<PvdPattern>();
    stack.push(new Step(opts, planner.getRoot(), planner.getRoot()));
    //preprocess
    driveSegment(partitionRules);
    driveSegment(pushRules);
    driveSegment(dsRules);
    planner.setPlansFound(count);
  }

  protected boolean candidateMatch(RelOptRuleOperand operand, RelNode rel,
      Map<Integer, List<Integer>> pushIDs) {
    List<RelNode> childRels = (List) rel.getInputs();
    if(rel instanceof LogicalAny){
      LogicalAny anyRel = (LogicalAny) rel;
      List<Integer> matchingIDs = new ArrayList<Integer>();
      for (int i = 0; i < childRels.size(); i ++) {
        RelNode childRel = childRels.get(i);
        boolean match = candidateMatch(operand, childRel, pushIDs);
        if(match)
          matchingIDs.add(i);
        pushIDs.put(anyRel.nodeID(), matchingIDs);
      }
    }
    else if (!(operand.matches(rel))) {
      return false;
    }

    switch (operand.childPolicy) {
    case ANY:
      return true;
    case UNORDERED:
      // For each operand, at least one child must match. If
      // matchAnyChildren, usually there's just one operand.
      for (RelOptRuleOperand childOperand : operand.getChildOperands()) {
        boolean match = false;
        for (RelNode childRel : childRels) {
          match =
              candidateMatch(
                  childOperand,
                  childRel,
                  pushIDs);
          if (match) {
            break;
          }
        }
        if (!match) {
          return false;
        }
      }
      return true;
    default:
      int n = operand.getChildOperands().size();
      if (childRels.size() < n) {
        return false;
      }
      for (Pair<RelOptRuleOperand, RelNode> pair
          : Pair.zip(operand.getChildOperands(), childRels)) {
        boolean match =
            candidateMatch(
                pair.left,
                pair.right,
                pushIDs);
        if (!match) {
          return false;
        }
      }
      return true;
    }
  }
}
