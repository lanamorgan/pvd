package org.apache.calcite.pvd;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.LinkedList;

public class PrefixSumCube extends GenericCube {
  //~ Constructors -----------------------------------------------------------


  public PrefixSumCube(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelNode input, List<AggregateCall> aggCalls,
      List <RexNode> groupList, List<RexNode> projects, List<RelNode> filters){
    super(cluster, traitSet, hints, input, aggCalls, groupList, projects, filters);
  }
  //~ Methods ----------------------------------------------------------------
  @Override public PrefixSumCube copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls){
    //TODO
    return this;
  }


  public static PrefixSumCube create(RelNode input,
      List<AggregateCall> aggCalls, List <RexNode> groupList,
      List<RexNode> projects, List<RelNode> filters) {

    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new PrefixSumCube(cluster, traitSet, ImmutableList.of(), input,
        aggCalls, groupList, projects, filters);
  }


  @Override public RelNode withHints(List<RelHint> hintList) {
    return new PrefixSumCube(getCluster(), traitSet, hintList, input,
        aggCalls, getGroupList(), getProjects(), getFilters());
  }
}
