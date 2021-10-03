package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PushRuleCall;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexAny;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class PushFilterExprRule
    extends RelRule<PushFilterExprRule.Config>
    implements TransformationRule,ExpressionRule {

  protected PushFilterExprRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
   return false;
  }

  @Override public List<PvdRuleCall> findAllMatches(PvdRuleCall call){
    LogicalFilter filter = call.rel(0);
    return DiffUtil.generateAllPush(filter.getCondition(), call);
  }

  @Override public void onMatch(RelOptRuleCall _call) {
    PushRuleCall call = (PushRuleCall) _call;
    LogicalFilter filter = call.rel(0);
    RexNode c = DiffUtil.pushAnyExpr(filter.getCondition(), call.nodeID());
    call.transformTo(LogicalFilter.create(filter.getInput(), c));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
        .as(Config.class);

    @Override default PushFilterExprRule toRule() {
      return new PushFilterExprRule(this);
    }
  }
}
