package org.apache.calcite.plan;

import  org.apache.calcite.plan.pvd.PvdPattern;

public interface PvdPatternRule{
  public PvdPattern build();
}

