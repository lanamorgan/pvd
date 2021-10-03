package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.pvd.MergeRuleCall;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.ArrayList;

public class MergeFilterExprRule
    extends RelRule<MergeFilterExprRule.Config>
    implements TransformationRule {

  protected MergeFilterExprRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall _call){
    MergeRuleCall call = (MergeRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    if (planner.isBannedMerge(call.parentID(), call.childID()))
      return false;
    LogicalFilter parent = call.rel(0);
    RexNode cond = parent.getCondition();
    return DiffUtil.canMerge(cond, call.parentID(), call.childID());
  }

  @Override public void onMatch(RelOptRuleCall _call) {
    MergeRuleCall call = (MergeRuleCall) _call;
    LogicalFilter parent = call.rel(0);
    RexNode cond = parent.getCondition();
    RexNode newCond = DiffUtil.merge(cond, call.parentID(), call.childID());
    call.transformTo(LogicalFilter.create(parent.getInput(), newCond));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
        .as(Config.class);

    @Override default MergeFilterExprRule toRule() {
      return new MergeFilterExprRule(this);
    }
  }
}
