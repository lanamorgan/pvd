package org.apache.calcite.pvd;


import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AggregateFieldCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.ArrayList;

public class GenericCube extends Aggregate {

  private List<RexNode> projects;
  private List<RelNode> filters;
  private List<RexNode> groupList;
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a GenericCube
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  private List <AggregateFieldCall> aggFieldCalls;

  public GenericCube(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelNode input, List<AggregateCall> aggCalls,
      List <RexNode> groupList, List<RexNode> projects, List<RelNode> filters){
    super(cluster, traitSet, hints, input, ImmutableBitSet.of(), null, aggCalls);
    aggFieldCalls = new ArrayList<AggregateFieldCall>();
    for(AggregateCall call: aggCalls){
      assert (call instanceof AggregateFieldCall);
      aggFieldCalls.add((AggregateFieldCall) call);
    }
    this.groupList = groupList;
    this.projects = projects;
    this.filters = filters;
  }
  //~ Methods ----------------------------------------------------------------
  public List<RexNode> getGroupList(){
    return ImmutableList.copyOf(groupList);
  }

  @Override public GenericCube copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls){
    //TODO
    return this;
  }

  public List<RelNode> getFilters(){
    return filters;
  }

  public List<RexNode> getProjects(){
    return projects;
  }

  public List<AggregateFieldCall> getAggFieldCalls(){
    return ImmutableList.copyOf(aggFieldCalls);
  }

  public static GenericCube create(RelNode input,
      List<AggregateCall> aggCalls, List <RexNode> groupList,
      List<RexNode> projects, List<RelNode> filters) {

    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new GenericCube(cluster, traitSet, ImmutableList.of(), input,
        aggCalls, groupList, projects, filters);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("filters", filters)
        .item("projects", projects);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new GenericCube(getCluster(), traitSet, hintList, input,
        aggCalls, getGroupList(), projects, filters);
  }
}
