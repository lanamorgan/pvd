package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalBitFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexMulti;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.ArrayList;


public class BitFilterRule
    extends RelRule<BitFilterRule.Config>
    implements TransformationRule {


  protected BitFilterRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    LogicalFilter fil = call.rel(0);
    RexNode cond = fil.getCondition();
    if (cond.getKind() == SqlKind.MULTI) {
      RexMulti multi = (RexMulti) cond;
      List<RexNode> multiChildren = multi.getChild().getOperands();
      for (RexNode child : multiChildren) {
        // must match tree "column = True"
        if (!(child instanceof RexCall && child.isA(SqlKind.EQUALS))) {
          return false;
        }
        RexCall rex = (RexCall) child;
        RexNode left = rex.getOperands().get(0);
        RexNode right = rex.getOperands().get(1);
        boolean leftIsCol = left.isA(SqlKind.FIELD_ACCESS) || left.isA(SqlKind.INPUT_REF);
        boolean rightIsBool = right.isA(SqlKind.LITERAL);
        if (!leftIsCol || !rightIsBool) {
          return false;
        }
      }
      return true;
    }

    return false;
  }


  @Override public void onMatch(RelOptRuleCall call){
    LogicalFilter fil = call.rel(0);
    RexMulti cond = (RexMulti) fil.getCondition();
    List<RexNode> multiChildren = cond.getChild().getOperands();
    List<RexNode> colList = new ArrayList<>();
    for (RexNode child : multiChildren){
      RexCall rex = (RexCall) child;
      colList.add(rex.getOperands().get(0));
    }
    LogicalBitFilter bf = LogicalBitFilter.create(fil.getInput(), cond, colList);
    call.transformTo(bf);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFilter.class)
            .anyInputs()).as(Config.class);

    @Override default BitFilterRule toRule() {
      return new BitFilterRule(this);
    }
  }
}
