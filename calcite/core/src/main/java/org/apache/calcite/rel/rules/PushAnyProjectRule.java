package org.apache.calcite.rel.rules;


import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PushRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexAny;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class PushAnyProjectRule
    extends RelRule<PushAnyProjectRule.Config>
    implements TransformationRule, ExpressionRule {

  protected PushAnyProjectRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
    return false;
  }

  @Override public List<PvdRuleCall> findAllMatches(PvdRuleCall call){
    LogicalProject proj = call.rel(0);
    return DiffUtil.generateAllPushList(proj.getProjects(), call);
  }

  @Override public void onMatch(RelOptRuleCall _call) {
    PushRuleCall call = (PushRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    LogicalProject proj = call.rel(0);
    List<RexNode> projectList = proj.getProjects();
    int diffNodeIndex = DiffUtil.getIndexOfID(call.nodeID(), projectList);
    RexAny any = (RexAny) projectList.get(diffNodeIndex);
    List<RexNode> anyOps = any.getOperands();
    List<RelNode> newDiffChildren = new ArrayList<RelNode>();
    for (RexNode op: anyOps){
      List<RexNode> newProjectList = new ArrayList<>(projectList);
      newProjectList.set(diffNodeIndex, op);
      //fix types at some point
      LogicalProject newChild = LogicalProject.create(proj.getInput(),
          ImmutableList.of(), newProjectList, proj.getRowType());
      newDiffChildren.add(newChild);
    }
    LogicalAny newAny = LogicalAny.create(newDiffChildren);
    call.transformTo(newAny);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalProject.class).anyInputs())
        .as(Config.class);

    @Override default PushAnyProjectRule toRule() {
      return new PushAnyProjectRule(this);
    }
  }
}
