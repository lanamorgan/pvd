package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.pvd.PartitionRuleCall;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.ArrayList;

public class PartitionFilterExprRule
    extends RelRule<PartitionFilterExprRule.Config>
    implements TransformationRule, ExpressionRule {

  protected PartitionFilterExprRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall _call){
    PartitionRuleCall call = (PartitionRuleCall) _call;
    LogicalFilter parent = call.rel(0);
    RexNode cond = parent.getCondition();
    return DiffUtil.canPartition(cond, call.nodeID());
  }

  @Override public List<PvdRuleCall> findAllMatches(PvdRuleCall call){
    LogicalFilter parent = call.rel(0);
    RexNode cond = parent.getCondition();
    return DiffUtil.generatePartitions(cond, call);
  }

  @Override public void onMatch(RelOptRuleCall _call) {
    PartitionRuleCall call = (PartitionRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    LogicalFilter parent = call.rel(0);
    RexNode cond = parent.getCondition();
    RexNode newCond = DiffUtil.partition(cond, call.nodeID(), call.partitionMap(), planner);
    LogicalFilter newFilter = LogicalFilter.create(DiffUtil.copyRelNode(parent.getInput()), newCond);
    //System.out.println(newFilter);
    call.transformTo(newFilter);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
        .as(Config.class);

    @Override default PartitionFilterExprRule toRule() {
      return new PartitionFilterExprRule(this);
    }
  }
}
