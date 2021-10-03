package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.GenericCube;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AggregateFieldCall;
import org.apache.calcite.rel.logical.LogicalFieldAggregate;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.ArrayList;

public class GenericCubeAggregateRule
    extends RelRule<GenericCubeAggregateRule.Config>
    implements TransformationRule {

  protected GenericCubeAggregateRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
    LogicalFieldAggregate agg = call.rel(0);
    List <AggregateFieldCall> aggCalls = agg.getAggFieldCalls();
    List <RexNode> groupList = agg.getGroupList();
    //is there an ANY or MULTI in the groupList or aggList?
    boolean hasDiffAggs = false;
    for (AggregateFieldCall fc: aggCalls){
      hasDiffAggs = hasDiffAggs || fc.hasDiffNode();
    }
    boolean hasDiffGroup = false;
    for(RexNode group: groupList){
      hasDiffGroup = hasDiffGroup ||
          (group.getKind() == SqlKind.ANY || group.getKind() == SqlKind.MULTI);
    }
    return hasDiffAggs || hasDiffGroup;
  }


  @Override public void onMatch(RelOptRuleCall call) {
    LogicalFieldAggregate agg = call.rel(0);
    List<RexNode> emptyProjects = new ArrayList<RexNode>(0);
    List<RelNode> emptyFilters = new ArrayList<RelNode>(0);
    RelNode cube = GenericCube.create(DiffUtil.copyRelNode(agg.getInput()), agg.getAggCallList(),
        agg.getGroupList(), emptyProjects, emptyFilters);
    call.transformTo(cube);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFieldAggregate.class)
            .anyInputs()).as(Config.class);

    @Override default GenericCubeAggregateRule toRule() {
      return new GenericCubeAggregateRule(this);
    }
  }
}
