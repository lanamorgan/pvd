package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.HashIndexTableScan;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

public class HashIndexScanRule
    extends RelRule<HashIndexScanRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToDistinctRule. */
  protected HashIndexScanRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
    LogicalFilter filter = call.rel(0);
    RexNode condition = filter.getCondition();
    //is condition of form attr = val?
    if (!(condition.isA(SqlKind.EQUALS)))
      return false;
    RexCall rex = (RexCall) condition;
    RexNode left = rex.getOperands().get(0);
    RexNode right = rex.getOperands().get(1);
    boolean leftIsCol = left.isA(SqlKind.FIELD_ACCESS) || left.isA(SqlKind.INPUT_REF);
    boolean rightIsParam = right.isA(SqlKind.PARAM);
    return leftIsCol && rightIsParam;
  }


  @Override public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    LogicalTableScan scan = call.rel(1);
    RexNode condition = filter.getCondition();
    RelNode indexScan = HashIndexTableScan.create(filter.getCluster(), scan.getTable(), condition);
    call.transformTo(indexScan);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFilter.class)
            .inputs(b1 -> b1.operand(LogicalTableScan.class).noInputs()))
        .as(Config.class);


    @Override default HashIndexScanRule toRule() {
      return new HashIndexScanRule(this);
    }
  }
}
