package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.pvd.PvdRuleCall;

import java.util.List;

public interface ExpressionRule{
  public List<PvdRuleCall> findAllMatches(PvdRuleCall call);
}
