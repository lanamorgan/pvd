package org.apache.calcite.rel.rules;

import org.apache.calcite.pvd.DiffUtil;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;

import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalTableScan} to the result
 * of calling {@link RelOptTable#toRel}.
 *
 * {@code org.apache.calcite.rel.core.RelFactories.TableScanFactoryImpl}
 * has called {@link RelOptTable#toRel(RelOptTable.ToRelContext)}.
 */

public class ProjectMergeExprRule <C extends ProjectMergeExprRule.Config>
    extends RelRule<C>
    implements TransformationRule {
  //~ Static fields/initializers ---------------------------------------------

  //~ Constructors -----------------------------------------------------------

  protected ProjectMergeExprRule(C config) { super(config); }


  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    Project p = call.rel(0);
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call){
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).predicate(LogicalProject::hasDiffNode)
                .anyInputs())
        .as(ProjectMergeExprRule.Config.class);

    @Override
    default ProjectMergeExprRule toRule() {
      return new ProjectMergeExprRule(this);
    }
  }
}
