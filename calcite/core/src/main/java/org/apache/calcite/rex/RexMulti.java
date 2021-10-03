package org.apache.calcite.rex;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMultiOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.ArrayList;

public class RexMulti extends RexCall{

  protected RexAny child;
  protected int hash = 0;

  public RexMulti(RelDataType type, SqlOperator op, List<RexNode> children){
    super(type, op, children);
    RexNode any = children.get(0);
    assert (any instanceof RexAny);
    this.child = (RexAny) any;
  }

  public static RexMulti create(RexAny child){
    RelDataType type = child.getType();
    List<RexNode> children = new ArrayList<RexNode>();
    children.add(child);
    return new RexMulti(type, SqlMultiOperator.INSTANCE, children);
  }
  @Override public RelDataType getType(){
    return type;
  }

  public RexAny getChild(){
    return child;
  }

  public <R> R accept(RexVisitor<R> visitor){
    return null;
  }

  @Override public SqlKind getKind() {
    return SqlKind.MULTI;
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
