package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexAny;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class PushAnyFilterRule
    extends RelRule<PushAnyFilterRule.Config>
    implements TransformationRule {

  protected PushAnyFilterRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
    LogicalFilter filter = call.rel(0);
    return (filter.getCondition() instanceof RexAny);
  }

  @Override public void onMatch(RelOptRuleCall _call) {
    PvdRuleCall call = (PvdRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    LogicalFilter filter = call.rel(0);
    RexAny cond = (RexAny) filter.getCondition();
    List<RexNode> conditions = cond.getOperands();
    List <RelNode> newChildren = new ArrayList<RelNode>();
    for(RexNode c : conditions){
      LogicalFilter newF = LogicalFilter.create(DiffUtil.copyRelNode(filter.getInput()), c);
      newChildren.add(newF);
    }
    LogicalAny newAny = LogicalAny.create(newChildren);
    if(planner.isBannedPartition(cond.getNodeID()))
      planner.banPartition(newAny.getNodeID());
    call.transformTo(newAny);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
        .as(Config.class);

    @Override default PushAnyFilterRule toRule() {
      return new PushAnyFilterRule(this);
    }
  }
}
