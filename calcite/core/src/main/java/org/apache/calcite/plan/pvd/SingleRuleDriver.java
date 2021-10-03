package org.apache.calcite.plan.pvd;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.rules.PvdRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.Nullable;

/*
 * This driver is intended for *simple* tests of planning rules.
 * It runs a BFS on the rule and outputs the --last explored plan--.
 * More complicated tests should use the DFS driver which outputs
 * --all explored plans--.
 */
public class SingleRuleDriver extends PvdRuleDriver{

  private Queue<Step> q;

  public SingleRuleDriver(PvdPlanner planner){
    super(planner);
    q = new LinkedList<Step>();
  }


  public void drive() {
    q.add(new Step(new LinkedHashSet<PvdPattern>(), planner.getRoot(), planner.getRoot()));
    List<RelOptRule> rules = planner.getRules();
    if (rules.size() == 0) {
      System.out.println("WARNING: No rules found; planner exited.");
      return;
    }
    //take first rule
    RelOptRule rule = rules.get(0);
    boolean isPartition = PvdRules.isPartitionRule(rule);
    boolean isMerge = PvdRules.isMergeRule(rule);

    Step currStep;
     do {
      currStep = q.poll();
       //add children
       List<RelNode> inputs = currStep.getPtr().getInputs();
       for (int i = 0; i < inputs.size(); i++) {
         Step stepChild = currStep.copy();
         stepChild.addPtrPath(i);
         stepChild.setPtr(inputs.get(i));
         q.add(stepChild);
       }
      final List<RelNode> rels = new ArrayList<>();
      final Map<RelNode, List<RelNode>> nodeChildren = new HashMap<>();
      boolean match =
          matchOperands(rule.getOperand(), currStep.getPtr(), rels, nodeChildren);
      if (match) {
        PvdRuleCall ruleCall;
        if (isPartition) {
          // this is for barebones partition testing; use dfs driver test for better tests
          ruleCall = new PartitionRuleCall(planner, rule.getOperand(),
              rels.toArray(new RelNode[rels.size()]), nodeChildren, currStep,
              0, new int[][]{{0}, {1}});
          planner.banMerge(0, 0);
          planner.banMerge(0, 1);
        }
        else if (isMerge){
          ruleCall = new MergeRuleCall(planner, rule.getOperand(),
              rels.toArray(new RelNode[rels.size()]), nodeChildren, currStep, 0, 0);
        }
        else {
          System.out.println("trying " + rule);
          ruleCall = new PvdRuleCall(planner, rule.getOperand(),
              rels.toArray(new RelNode[rels.size()]), nodeChildren, currStep);
        }
        System.out.println(rule.matches(ruleCall));
        if (rule.matches(ruleCall)) {
          System.out.println(rule + " success");
          rule.onMatch(ruleCall);
          Step m = ruleCall.step();
          q.add(m);
        }
      }

      } while (q.peek() != null);
     planner.setRoot(currStep.getRoot());
  }

}
