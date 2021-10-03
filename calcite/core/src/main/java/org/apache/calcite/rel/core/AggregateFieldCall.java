/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.core;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Call to an aggregate function within an
 * {@link org.apache.calcite.rel.core.Aggregate}.
 */
public class AggregateFieldCall extends AggregateCall{
  //~ Instance fields --------------------------------------------------------

  private final ImmutableList<RexNode> fieldList;
  private boolean hasDiffNode;

  private AggregateFieldCall(SqlAggFunction aggFunction, RelCollation collation, RelDataType type,
      @Nullable String name, List<RexNode> fieldList) {
    super(aggFunction, false, false, true, ImmutableList.of(), 0, collation, type, name);
    this.fieldList = ImmutableList.copyOf(fieldList);
    hasDiffNode = false;
    for (RexNode node: this.fieldList){
      hasDiffNode = (hasDiffNode || node.getKind() == SqlKind.ANY
          || node.getKind() == SqlKind.MULTI);
    }
  }

  public static AggregateFieldCall create(SqlAggFunction aggFunction, RelCollation collation,
     RelDataType type, @Nullable String name, List<RexNode> fieldList) {

    return new AggregateFieldCall(aggFunction, collation, type, name, fieldList);
  }

  public final List<RexNode> getFieldList(){ return fieldList;}

  public boolean hasDiffNode(){
    return hasDiffNode;
  }

  @Override public String toString() {
    StringBuilder buf = new StringBuilder(getAggregation().toString());
    buf.append("(");
    if (isApproximate()) {
      buf.append("APPROXIMATE ");
    }
    if (isDistinct()) {
      buf.append((getArgList().size() == 0) ? "DISTINCT" : "DISTINCT ");
    }
    int i = -1;
    for (RexNode arg : fieldList) {
      if (++i > 0) {
        buf.append(", ");
      }
      buf.append("$");
      buf.append(arg.toString());
    }
    buf.append(")");
    if (!getCollation().equals(RelCollations.EMPTY)) {
      buf.append(" WITHIN GROUP (");
      buf.append(getCollation());
      buf.append(")");
    }
    return buf.toString();
  }

}
