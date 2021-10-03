package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.pvd.PartitionRuleCall;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.ArrayList;

import com.google.common.collect.ImmutableList;


public class PartitionProjectExprRule
    extends RelRule<PartitionProjectExprRule.Config>
    implements TransformationRule, ExpressionRule {

  protected PartitionProjectExprRule(Config config) {
    super(config);
  }

  @Override public List<PvdRuleCall> findAllMatches(PvdRuleCall call) {
    LogicalProject parent = call.rel(0);
    List<RexNode> projs = parent.getProjects();
    List<PvdRuleCall> combinedList = new ArrayList<>();
    for (RexNode projRoot : projs)
      combinedList.addAll(DiffUtil.generatePartitions(projRoot, call));
    return combinedList;
  }


  @Override public boolean matches(RelOptRuleCall _call){
    // do not use this class unless specifically using PRC
    // requires specific information about *how to partition* node
    assert (_call instanceof PartitionRuleCall);
    PartitionRuleCall call = (PartitionRuleCall) _call;
    LogicalProject parent = call.rel(0);
    List<RexNode> projects = parent.getProjects();
    return !projects.stream().allMatch(p -> !DiffUtil.canPartition(p, call.nodeID()));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall _call) {
    PartitionRuleCall call = (PartitionRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    LogicalProject parent = call.rel(0);
    List<RexNode> projects = parent.getProjects();
    List<RexNode> newProjects = new ArrayList<RexNode>();
    for(RexNode p : projects) {
      RexNode newRex = DiffUtil.partition(p, call.nodeID(), call.partitionMap(), planner);
      newProjects.add(newRex);
    }
    //TODO: fix typee
    call.transformTo(LogicalProject.create(parent.getInput(), ImmutableList.of(),
        newProjects, parent.getRowType()));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalProject.class).anyInputs())
        .as(Config.class);

    @Override default PartitionProjectExprRule toRule() {
      return new PartitionProjectExprRule(this);
    }
  }
}
