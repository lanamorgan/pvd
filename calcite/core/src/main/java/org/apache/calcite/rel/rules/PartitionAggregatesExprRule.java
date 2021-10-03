package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.pvd.PartitionRuleCall;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.rel.logical.LogicalFieldAggregate;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.ArrayList;

import com.google.common.collect.ImmutableList;


public class PartitionAggregatesExprRule
    extends RelRule<PartitionAggregatesExprRule.Config>
    implements TransformationRule, ExpressionRule {

  protected PartitionAggregatesExprRule(Config config) {
    super(config);
  }

  @Override public List<PvdRuleCall> findAllMatches(PvdRuleCall call){
    LogicalFieldAggregate parent = call.rel(0);
    List<RexNode> groupList = parent.getGroupList();
    // TODO also aggs
    // List<RexNode> aggs = parent.getGroupList();

    List<PvdRuleCall> combinedList = new ArrayList<>();
    for (RexNode group: groupList)
      combinedList.addAll(DiffUtil.generatePartitions(group, call));
    return combinedList;
  }


  @Override public boolean matches(RelOptRuleCall _call){
    // do not use this class unless specifically using PRC
    // requires specific information about *how to partition* node
    assert (_call instanceof PartitionRuleCall);
    PartitionRuleCall call = (PartitionRuleCall) _call;
    LogicalFieldAggregate parent = call.rel(0);
    List<RexNode> groups = parent.getGroupList();
    return !groups.stream().allMatch(p -> !DiffUtil.canPartition(p, call.nodeID()));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall _call) {
    PartitionRuleCall call = (PartitionRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    LogicalFieldAggregate parent = call.rel(0);
    List<RexNode> groups = parent.getGroupList();
    List<RexNode> newGroups = new ArrayList<RexNode>();
    for(RexNode g : groups) {
      RexNode newRex = DiffUtil.partition(g, call.nodeID(), call.partitionMap(), planner);
      newGroups.add(newRex);
    }
    //TODO: fix typee
    call.transformTo(LogicalFieldAggregate.create(parent.getInput(),
        parent.getAggCallList(), newGroups));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalFieldAggregate.class).anyInputs())
        .as(Config.class);

    @Override default PartitionAggregatesExprRule toRule() {
      return new PartitionAggregatesExprRule(this);
    }
  }
}
