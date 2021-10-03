package org.apache.calcite.pvd;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AggregateFieldCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAny;
import org.apache.calcite.rel.logical.LogicalFieldAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelAnyType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.rex.RexAny;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexMulti;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexNodeList;
import org.apache.calcite.rex.RexParam;
import org.apache.calcite.rex.RexSimpleField;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.fun.SqlBetweenOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlAny;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMulti;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlParam;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.util.Pair;


import com.google.common.collect.ImmutableList;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class SimpleSqlToRel{

  protected final RelOptCluster cluster;
  private final RexBuilder rexBuilder;
  private final RelOptPlanner planner;
  private final Map<String, SimpleTable> catalog;
  private RelDataTypeFactory typeFactory;
  private List <AggregateCall> aggs;
  private final SqlNodeToRexConverter exprConverter;


  public SimpleSqlToRel(RexBuilder rexBuilder,
      RelOptPlanner planner,
      Map<String, SimpleTable> catalog)
  {
    this.planner = planner;
    this.rexBuilder= rexBuilder;
    this.cluster = RelOptCluster.create(planner, rexBuilder);
    this.catalog = catalog;
    this.typeFactory = typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.aggs = new ArrayList<AggregateCall>();
    this.exprConverter = new SqlNodeToRexConverterImpl(StandardConvertletTable.INSTANCE);
  }

  /*
   * This makes a really stupid relational plan where joins/scans are at the bottom,
   * then filters, then projections, then grouping/aggs/group filters, then sort.
   * Using SqlToRelConverter requires typechecking and ordinal fields that are
   * both incompatible with and unnecessary for the coarse planning PVD does.
   */

  public RelNode convertQuery(SqlNode root){
    if (root.getKind() == SqlKind.ANY){
      SqlAny anyRoot = (SqlAny) root;
      SqlNodeList children = anyRoot.getChildren();
      List <RelNode> relChildren = new ArrayList<RelNode>();
      for (SqlNode child : children){
        relChildren.add(convertQuery(child));
      }
      return LogicalAny.create(relChildren);
    }
    SqlSelect select = (SqlSelect) root;
    RelNode relroot = convertFrom(select.getFrom());
    RelNode where = convertWhere(select.getWhere(), relroot);
    relroot = where == null ? relroot : where;
    // convert unresolved calls to appropriate type, expand star
    resolveSelectList(select, relroot);
    // if has aggregates, create aggregate/groupby
    if (select.getGroup() != null || !aggs.isEmpty()){
      // need an aggregate if there is a groupby or aggs in projectList
      relroot = convertAgg(select, relroot);
    }
    else {
      relroot = convertSelectList(select.getSelectList(), relroot);
    }
    // TO DO order by
    return relroot;
  }

  private void resolveSelectList(SqlSelect select, RelNode input){
    SqlNodeList projectList = (SqlNodeList) select.getSelectList();
    List <SqlNode> newSelectList = new ArrayList<SqlNode>();
    for (SqlNode item: projectList){
      if (item instanceof SqlBasicCall){
        SqlBasicCall call = (SqlBasicCall) item;
        AggregateFieldCall attempt = resolve(call, input);
        if (attempt != null)
          aggs.add(attempt);
        else
          newSelectList.add(item);
      }
      else
        newSelectList.add(item);
    SqlNodeList newProjects = SqlNodeList.of(SqlParserPos.ZERO, newSelectList);
    select.setSelectList(newProjects);
   }
  }

  private AggregateFieldCall resolve(SqlBasicCall call, RelNode input){
    SqlOperator op = call.getOperator();
    String name = op.getName();
    SqlAggFunction aggFunction;
    RelDataType type;
    switch (name){
    case "SUM":
      aggFunction = SqlStdOperatorTable.SUM;
      type = typeFactory.createSqlType(SqlTypeName.BIGINT);
      break;
    case "COUNT":
      aggFunction = SqlStdOperatorTable.COUNT;
      type = typeFactory.createSqlType(SqlTypeName.BIGINT);
      break;
    case "MIN":
      aggFunction = SqlStdOperatorTable.MIN;
      type = typeFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    case "MAX":
      aggFunction = SqlStdOperatorTable.MAX;
      type = typeFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    case"AVG":
      aggFunction = SqlStdOperatorTable.AVG;
      type = typeFactory.createSqlType(SqlTypeName.FLOAT);
      break;
    default:
      return null;
    }
    RelCollation collation = RelCollations.EMPTY;
    List<RexNode> fieldList = convertOperands(call.getOperands(), input);
    return AggregateFieldCall.create(aggFunction, collation, type, name, fieldList);
  }

  private List<RexNode> convertOperands(SqlNode[] operands, RelNode input){
    List<RexNode> operandList = new ArrayList<RexNode>();
    for (SqlNode operand: operands){
      convertItem(operand, input.getRowType(),
          operandList, new ArrayList<RelDataTypeField>());
    }
    return operandList;
  }

  public RelNode convertAgg(SqlSelect select, RelNode input) {
    SqlNodeList projectList = (SqlNodeList) select.getSelectList();
    SqlNodeList groups = (SqlNodeList) select.getGroup();
    SqlNode having = select.getHaving();
    List<RexNode> groupList = new ArrayList<RexNode>();
    List<RelDataTypeField> groupType = new ArrayList<RelDataTypeField>();
    if (groups != null){
    //fill with groups
      for (SqlNode group : groups) {
        convertItem(group, input.getRowType(), groupList, groupType);
      }
    }
    if (!projectList.isEmpty()){
      RelNode proj = convertSelectList(projectList, input);
      input = proj;
    }
    //translate having condition with input
    return LogicalFieldAggregate.create(input, aggs, groupList);
  }



  public RelNode convertSelectList(SqlNode selectList, RelNode input){
    SqlNodeList select = (SqlNodeList) selectList;
    List <RexNode> projectList = new ArrayList<RexNode>();
    List <RelDataTypeField> projectTypes = new ArrayList<RelDataTypeField>();
    RelDataType inputType = input.getRowType();
    for (SqlNode item:select){
        convertItem(item, inputType, projectList, projectTypes);
    }
    RelDataType outputType =
        new RelRecordType(StructKind.PEEK_FIELDS, projectTypes, true);
    return LogicalProject.create(input,
        ImmutableList.of(), projectList, outputType);
  }

  public void convertItem(SqlNode item, RelDataType inputType,
      List <RexNode> projectList,  List <RelDataTypeField> projectTypes){
    SqlKind kind = item.getKind();
    switch(kind) {
    case MULTI:
      SqlMulti multi = (SqlMulti) item;
      if (multi.getChild().getKind() != SqlKind.ANY)
        throw new RuntimeException("MULTI can only have an ANY child.");
      SqlAny child = multi.getChild();
      RexAny rexAny = makeRexAny(child, inputType);
      projectList.add(RexMulti.create(rexAny));
      projectTypes.add(new RelDataTypeFieldImpl("MULTI", projectTypes.size(), rexAny.getType()));
      break;
    case ANY:
      SqlAny any = (SqlAny) item;
      rexAny = makeRexAny(any, inputType);
      projectList.add(rexAny);
      projectTypes.add(new RelDataTypeFieldImpl("ANY", projectTypes.size(), rexAny.getType()));
      break;
    case IDENTIFIER:
      SqlIdentifier id = (SqlIdentifier) item;
      if (id.isStar()){
        //TODO: expand star to entire input row
        throw new RuntimeException("* identifier found");
      }
      String name = id.toString().toLowerCase();
      RelDataTypeField itemType;
      if (isFullyQualified(name))
        itemType = inputType.getField(name, false, false);
      else
        itemType = getSemiQualifiedType(name, inputType);
      if (itemType == null){
        throw new RuntimeException(name + " not found in catalog.");
      }
      RexNode rexItem = new RexSimpleField(name, 0, itemType.getType());
      projectList.add(rexItem);
      projectTypes.add(itemType);
      break;
    case OTHER_FUNCTION:
      break;
    default:
      throw new RuntimeException(kind + " found in project list: " + item.getClass());
    }
  }

  public RexAny makeRexAny(SqlAny any, RelDataType inputType){
    SqlNodeList children = any.getChildren();
    List <RexNode> innerExps = new ArrayList<RexNode>();
    List <RelDataTypeField> innerTypes = new ArrayList<RelDataTypeField>();
    List <RelDataType> anyChildTypes = new ArrayList<RelDataType>();
    int count = 0;
    for (SqlNode child: children){
      SqlNodeList childList = (SqlNodeList) child;
      List <RexNode> childExps = new ArrayList<RexNode>();
      List <RelDataTypeField> childTypes = new ArrayList<RelDataTypeField>();
      for(SqlNode childItem: childList) {
        convertItem(childItem, inputType, childExps, childTypes);
      }
      RelDataType childType =
          new RelRecordType(StructKind.PEEK_FIELDS, childTypes, true);
      innerExps.add(RexNodeList.of(childType, childExps));
      innerTypes.add(new RelDataTypeFieldImpl("", count, childType));
      count +=1;
    }
    RelDataType outputType = new RelAnyType(innerTypes, anyChildTypes);
    return new RexAny(outputType, innerExps);
  }

  public RelNode convertFrom(SqlNode from){
    SqlKind kind = from.getKind();
    switch (kind) {
    case IDENTIFIER:
      SqlIdentifier id = (SqlIdentifier) from;
      SimpleTable table = catalog.get(from.toString().toLowerCase());
      return LogicalTableScan.create(cluster, table, ImmutableList.of());
    case JOIN:
      SqlJoin join = (SqlJoin) from;
      RelNode left = convertFrom(join.getLeft());
      RelNode right = convertFrom(join.getRight());
      List<RelHint> hints = new ArrayList<RelHint>();
      SqlNode expr = join.getCondition();
      RelDataType[] joinTypes = new RelDataType[]{left.getRowType(), right.getRowType()};
      RelDataType joinRowType = typeFactory.createJoinType(joinTypes);
      RexNode condition = convertExpression(join.getCondition(), joinRowType);
      Set<CorrelationId> variables = new HashSet<CorrelationId>();
      JoinRelType joinType = joinTypeAsRel(join.getJoinType());
      return LogicalJoin.create(left, right, hints, condition, variables, joinType);
      default:
        throw new RuntimeException(kind + " table found:" + from.getClass());
    }
  }

  private JoinRelType joinTypeAsRel(JoinType jt){
    switch(jt) {
      case INNER:
        return JoinRelType.INNER;
      case FULL:
        return JoinRelType.FULL;
      case LEFT:
        return JoinRelType.LEFT;
      case RIGHT:
        return JoinRelType.RIGHT;
      default:
        return JoinRelType.INNER;
    }
  }

  public RelNode convertWhere(SqlNode where, RelNode input){
    if (where == null){
      return null;
    }
    RexNode condition = convertExpression(where, input.getRowType());
    return LogicalFilter.create(input, condition);
  }

  public RexNode convertExpression(SqlNode expr, RelDataType input){
    SqlKind kind = expr.getKind();
    switch(kind){
      case ANY:
        SqlAny any = (SqlAny) expr;
        SqlNodeList childExpr = any.getChildren();
        List<RexNode> rexChildren = new ArrayList<RexNode>();
        for(SqlNode child: childExpr){
          rexChildren.add(convertExpression(child, input));
        }
        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.ANY);
        return new RexAny(boolType, rexChildren);
    case MULTI:
      SqlMulti multi = (SqlMulti) expr;
      any = multi.getChild();
      childExpr = any.getChildren();
      rexChildren = new ArrayList<RexNode>();
      for(SqlNode child: childExpr){
        rexChildren.add(convertExpression(child, input));
      }
      RelDataType anyType = typeFactory.createSqlType(SqlTypeName.ANY);
      RexAny rexAny = new RexAny(anyType, rexChildren);
      return RexMulti.create(rexAny);
    case BETWEEN:
      SqlBasicCall btwn = (SqlBasicCall) expr;
      SqlNode[] bops = btwn.getOperands();
      RexNode valOp = convertExpression(bops[SqlBetweenOperator.VALUE_OPERAND], input);
      RexNode lower = convertExpression(bops[SqlBetweenOperator.LOWER_OPERAND], input);
      RexNode upper = convertExpression(bops[SqlBetweenOperator.UPPER_OPERAND], input);
      RexNode ge = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, valOp, lower);
      RexNode le = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, valOp, upper);
      return rexBuilder.makeCall(SqlStdOperatorTable.AND, ge, le);
    case AND:
      return makeBinaryCall(SqlStdOperatorTable.AND, expr, input);
    case OR:
      return makeBinaryCall(SqlStdOperatorTable.OR, expr, input);
    case NOT_EQUALS:
      return makeBinaryCall(SqlStdOperatorTable.NOT_EQUALS, expr, input);
    case LESS_THAN:
      return makeBinaryCall(SqlStdOperatorTable.LESS_THAN, expr, input);
    case GREATER_THAN:
      return makeBinaryCall(SqlStdOperatorTable.GREATER_THAN, expr, input);
    case GREATER_THAN_OR_EQUAL:
      return makeBinaryCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
          expr, input);
    case LESS_THAN_OR_EQUAL:
      return makeBinaryCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, expr, input);
    case EQUALS:
      return makeBinaryCall(SqlStdOperatorTable.EQUALS, expr, input);
    case IDENTIFIER:
      SqlIdentifier id = (SqlIdentifier) expr;
      String name = id.toString().toLowerCase();
      RelDataTypeField itemType;
      if (isFullyQualified(name)) {
        itemType = input.getField(name, false, false);
      }
      else
        itemType = getSemiQualifiedType(name, input);
      RexNode rexItem = new RexSimpleField(name, 0, itemType.getType());
      return rexItem;
    case PARAM:
      SqlParam param = (SqlParam) expr;
      SqlTypeName typeName = param.getType();
      String pretty = "param: " + typeName.getName();
      return new RexParam(pretty, typeFactory.createSqlType(typeName));
    case LITERAL:
      SqlLiteral lit = (SqlLiteral) expr;
      RelDataType litType = lit.createSqlType(typeFactory);
      return rexBuilder.makeLiteral(lit.getValue(), litType, false);
      default:
        throw new RuntimeException(kind + " found: " + expr.getClass());
    }
  }
  private boolean isFullyQualified(String fieldName){
    return fieldName.contains(".");
  }

  private RelDataTypeField getSemiQualifiedType(String fieldName, RelDataType input){
    List<String> inputNames = input.getFieldNames();
    boolean match = false;
    int fq_index = -1;

    for (int i = 0; i < inputNames.size(); i++){
      String inputName = inputNames.get(i).split("\\.", 2)[1];
      if (inputName.equals(fieldName)){
        if (match){
          throw new RuntimeException(fieldName +
              " is ambiguous and could not be resolved; use fully qualified name.");
        }
        else {
          match = true;
          fq_index = i;
        }
      }
    }
    if(fq_index == -1)
      throw new RuntimeException(fieldName + " could not be resolved.");
    return input.getFieldList().get(fq_index);
  }

  private RexNode makeBinaryCall(SqlOperator op, SqlNode expr, RelDataType input){
    SqlBasicCall call = (SqlBasicCall) expr;
    SqlNode[] operands = call.getOperands();
    RexNode op1 = convertExpression(operands[0], input);
    RexNode op2 = convertExpression(operands[1], input);
    return rexBuilder.makeCall(op, op1, op2);
  }
}
