package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.pvd.PartitionRuleCall;
import org.apache.calcite.plan.pvd.PvdRuleCall;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Util;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class PartitionAnyRule
    extends RelRule<PartitionAnyRule.Config>
    implements TransformationRule, ExpressionRule {

  /** Creates an IntersectToDistinctRule. */
  protected PartitionAnyRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call){
    LogicalAny any = call.rel(0);
    List<RelNode> inputs = any.getInputs();
    return inputs.size() > 1 &&
        !inputs.stream().allMatch(n -> n instanceof LogicalAny);
  }

  @Override public List<PvdRuleCall> findAllMatches(PvdRuleCall call){
    LogicalAny any = call.rel(0);
    return DiffUtil.generatePartitions(any, call);
  }


  @Override public void onMatch(RelOptRuleCall _call) {
    PartitionRuleCall call = (PartitionRuleCall) _call;
    PvdPlanner planner = (PvdPlanner) call.getPlanner();
    LogicalAny parent = call.rel(0);
    int [][] partitionMap = call.partitionMap();
    assert (partitionMap != null);
    int numPartitions = partitionMap.length;
    List<RelNode> partitions = new ArrayList<RelNode>(numPartitions);
    for(int i = 0; i < numPartitions; i++){
      List<RelNode> partitionChildren = new ArrayList<RelNode>();
      int[] indexes = partitionMap[i];
      for(int index: indexes){
        RelNode child  = copyChild(parent.getInputs().get(index));
        partitionChildren.add(child);
      }
      LogicalAny newAny = LogicalAny.create(partitionChildren);
      partitions.add(newAny);
      planner.banPartition(newAny.getNodeID());
    }
    call.transformTo(LogicalAny.create(partitions));
  }

  private RelNode copyChild(RelNode child){
    List<RelNode> childInputs = child.getInputs();
    List<RelNode> copiedInputs = new ArrayList<>();
    for (RelNode ci : childInputs){
      copiedInputs.add(copyChild(ci));
    }
    return child.copy(child.getTraitSet(), copiedInputs);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandSupplier(b0 -> b0.operand(LogicalAny.class).anyInputs())
        .as(Config.class);

    @Override default PartitionAnyRule toRule() {
      return new PartitionAnyRule(this);
    }
  }
}
