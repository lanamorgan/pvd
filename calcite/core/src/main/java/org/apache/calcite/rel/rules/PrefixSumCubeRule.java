package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.HashIndexTableScan;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalFieldAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

public class PrefixSumCubeRule
    extends RelRule<PrefixSumCubeRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToDistinctRule. */
  protected PrefixSumCubeRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
    LogicalFilter filter = call.rel(1);
    LogicalFieldAggregate agg = call.rel(0);
    RexNode cond = filter.getCondition();

    return false;
  }


  @Override public void onMatch(RelOptRuleCall call) {

  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFieldAggregate.class)
            .inputs(b1 -> b1.operand(LogicalFilter.class).noInputs()))
        .as(Config.class);


    @Override default PrefixSumCubeRule toRule() {
      return new PrefixSumCubeRule(this);
    }
  }
}
