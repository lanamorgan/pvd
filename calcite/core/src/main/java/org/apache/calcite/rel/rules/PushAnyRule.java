package org.apache.calcite.rel.rules;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class PushAnyRule
    extends RelRule<PushAnyRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToDistinctRule. */
  protected PushAnyRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
   return true;
  }


  @Override public void onMatch(RelOptRuleCall _call) {
    PvdRuleCall call = (PvdRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    SingleRel parent = call.rel(0);
    LogicalAny child = call.rel(1);
    List<RelNode> anyChildren = child.getInputs();
    List <RelNode> newChildren = new ArrayList<RelNode>();
    for(RelNode oldChild: anyChildren){
      RelNode copyChild = DiffUtil.copyRelNode(oldChild);
      newChildren.add(parent.copy(parent.getTraitSet(), Arrays.asList(copyChild)));
    }
    LogicalAny newAny = LogicalAny.create(newChildren);
    if(planner.isBannedPartition(child.getNodeID()))
      planner.banPartition(newAny.getNodeID());
    call.transformTo(newAny);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(SingleRel.class)
            .inputs(b1 -> b1.operand(LogicalAny.class).anyInputs()))
            .as(Config.class);

    @Override default PushAnyRule toRule() {
      return new PushAnyRule(this);
    }
  }
}
