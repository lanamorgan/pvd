package org.apache.calcite.plan.pvd;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
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

public abstract class PvdRuleDriver{

  protected final PvdPlanner planner;

  public PvdRuleDriver(PvdPlanner planner) {
    this.planner = planner;
  }


  public abstract void drive();


  protected boolean matchOperands(RelOptRuleOperand operand,
      RelNode rel, List<RelNode> bindings,
      Map<RelNode, List<RelNode>> nodeChildren) {
    if (!(operand.matches(rel))) {
      return false;
    }
    bindings.add(rel);
    List<RelNode> childRels = (List) rel.getInputs();
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
              matchOperands(
                  childOperand,
                  childRel,
                  bindings,
                  nodeChildren);
          if (match) {
            break;
          }
        }
        if (!match) {
          return false;
        }
      }
      nodeChildren.put(rel, childRels);
      return true;
    default:
      int n = operand.getChildOperands().size();
      if (childRels.size() < n) {
        return false;
      }
      for (Pair<RelOptRuleOperand, RelNode> pair
          : Pair.zip(operand.getChildOperands(), childRels)) {
        boolean match =
            matchOperands(
                pair.left,
                pair.right,
                bindings,
                nodeChildren);
        if (!match) {
          return false;
        }
      }
      return true;
    }
  }
}
