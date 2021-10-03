package org.apache.calcite.pvd;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class HashIndexTableScan extends TableScan {

  private RexNode keyCondition;
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a HashIndexTableScan.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public HashIndexTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelOptTable table, RexNode keyCondition) {
    super(cluster, traitSet, hints, table);
    this.keyCondition = keyCondition;
  }


  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return this;
  }

  /** Creates a LogicalTableScan.
   *
   * @param cluster     Cluster
   * @param relOptTable Table
   * @param hints       The hints
   */
  public static HashIndexTableScan create(RelOptCluster cluster,
      final RelOptTable relOptTable, RexNode keyCondition) {
    final Table table = relOptTable.unwrap(Table.class);
    final RelTraitSet traitSet =
        cluster.traitSetOf(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
              if (table != null) {
                return table.getStatistic().getCollations();
              }
              return ImmutableList.of();
            });
    return new HashIndexTableScan(cluster, traitSet, ImmutableList.of(), relOptTable, keyCondition);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("condition", keyCondition);
  }
  @Override public RelNode withHints(List<RelHint> hintList) {
    return new HashIndexTableScan(getCluster(), traitSet, hintList, table, keyCondition);
  }
}
