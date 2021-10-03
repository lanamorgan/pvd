package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

public class RexParam extends RexNode{
  private RelDataType type;

  public RexParam(String str, RelDataType type){
    this.type = type;
    this.digest = str;
  }

  @Override public RelDataType getType(){
    return type;
  }

  public <R> R accept(RexVisitor<R> visitor){
    return null;
  }

  @Override public SqlKind getKind() {
    return SqlKind.PARAM;
  }

  /**
   * Accepts a visitor with a payload, dispatching to the right overloaded
   * {@link RexBiVisitor#visitInputRef(RexInputRef, Object)} visitXxx} method.
   */
  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg){
    return null;
  }

  /** {@inheritDoc}
   *
   * <p>Every node must implement {@link #equals} based on its content
   */
  @Override public boolean equals(@Nullable Object obj){
    return false;
  }

  /** {@inheritDoc}
   *
   * <p>Every node must implement {@link #hashCode} consistent with
   * {@link #equals}
   */
  @Override public int hashCode(){
    return digest.hashCode();
  }
}
