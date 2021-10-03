package org.apache.calcite.test.pvd;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.pvd.SimpleTable;
import org.apache.calcite.pvd.SimpleSqlToRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.BitFilterRule;
import org.apache.calcite.rel.rules.HashIndexScanRule;
import org.apache.calcite.rel.rules.HashCubeAggregateRule;
import org.apache.calcite.rel.rules.GenericCubeAggregateRule;
import org.apache.calcite.rel.rules.PartitionAnyRule;
import org.apache.calcite.rel.rules.PartitionFilterExprRule;
import org.apache.calcite.rel.rules.PartitionProjectExprRule;
//import org.apache.calcite.rel.rules.PrefixSumRule;
import org.apache.calcite.rel.rules.PushAnyRule;
import org.apache.calcite.rel.rules.PushAnyFilterRule;
import org.apache.calcite.rel.rules.MergeFilterExprRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.HashMap;
import java.util.Map;



import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleRulePlanningTest {

  public Sql sql(String sql) {
    return new Sql(sql);
  }

  /*
   * Test rules individually
   */

  @Test
  void testHashIndexScanRule(){
    String sqlStr = "select emp.name from emp where emp.name = param:varchar";
    Sql sql = sql(sqlStr)
        .withRule(HashIndexScanRule.Config.DEFAULT.toRule());
    sql.checkPlanning("LogicalProject(emp.name=[name])\n" +
    "  HashIndexTableScan(table=[[emp]], condition=[=(name, param: VARCHAR)])\n");
  }

//  @Test void testHashIndexScanFail(){
//    String sqlStr = "select name from emp where empid <  param:integer";
//    Sql sql = sql(sqlStr)
//        .withRule(HashIndexScanRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(emp.name=[name])\n"
//        + "  LogicalFilter(condition=[<(empid, param: INTEGER)])\n"
//        + "    LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testGenericCubeAggRule() {
//    //test replacing aggregate only
//    String sqlStr = "select count(empid) from emp group by ANY{[eptno], [empid]}";
//    Sql sql = sql(sqlStr).withRule(GenericCubeAggregateRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("GenericCube(COUNT=[COUNT($empid)], filters=[[]], projects=[[]])\n"
//        + "  LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testGenericCubeFail() {
//    String sqlStr = "select count(empid) from emp group by eptno";
//    Sql sql = sql(sqlStr).withRule(GenericCubeAggregateRule.Config.DEFAULT.toRule());
//    sql.checkPlanning( "LogicalFieldAggregate(COUNT=[COUNT($empid)], group=[[eptno]])\n"
//        + "  LogicalTableScan(table=[[emp]])\n" );
//  }
//
//  @Test void testHashCubeAggregateRule() {
//    //test replaces count query
//    String sqlStr = "select count(empid) from emp group by ANY{[eptno], [empid]} ";
//    Sql sql = sql(sqlStr)
//        .withRule(HashCubeAggregateRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("HashCube(COUNT=[COUNT($empid)], filters=[[]], projects=[[]], filters=[null], projects=[null])\n"
//        + "  LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testHashCubeAggregateFailRule(){
//  //test does not replace non-counting aggregates
//    String sqlStr = "select sum(empid), empid from emp group by ANY{[eptno], [empid]} ";
//    Sql sql = sql(sqlStr)
//        .withRule(HashCubeAggregateRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalFieldAggregate(SUM=[SUM($empid)], group=[[ANY([eptno], [empid])]])\n"
//        + "  LogicalProject(emp.empid=[empid])\n"
//        + "    LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testPushAnyFilter() {
//    String sqlStr = "select name from emp where ANY {eptno > 0, role = param:varchar}";
//    Sql sql = sql(sqlStr)
//        .withRule(PushAnyFilterRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(emp.name=[name])\n"
//        + "  LogicalAny(all=[false])\n"
//        + "    LogicalFilter(condition=[>(eptno, 0)])\n"
//        + "      LogicalTableScan(table=[[emp]])\n"
//        + "    LogicalFilter(condition=[=(role, param: VARCHAR)])\n"
//        + "      LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testMultiStepPushFilter() {
//    String sqlStr = "select name from emp where ANY {eptno > 0, role = param:varchar}";
//    Sql sql = sql(sqlStr)
//        .withRule(PushAnyFilterRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(emp.name=[name])\n"
//        + "  LogicalAny(all=[false])\n"
//        + "    LogicalFilter(condition=[>(eptno, 0)])\n"
//        + "      LogicalTableScan(table=[[emp]])\n"
//        + "    LogicalFilter(condition=[=(role, param: VARCHAR)])\n"
//        + "      LogicalTableScan(table=[[emp]])\n");
//
//    sql = sql.withRule(PushAnyRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalAny(all=[false])\n"
//        + "  LogicalProject(emp.name=[name])\n"
//        + "    LogicalFilter(condition=[>(eptno, 0)])\n"
//        + "      LogicalTableScan(table=[[emp]])\n"
//        + "  LogicalProject(emp.name=[name])\n"
//        + "    LogicalFilter(condition=[=(role, param: VARCHAR)])\n"
//        + "      LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testAnyPartition() {
//    String sqlStr = "ANY select name from emp; select eptno from emp";
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionAnyRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalAny(all=[false])\n"
//        + "  LogicalAny(all=[false])\n"
//        + "    LogicalProject(emp.name=[name])\n"
//        + "      LogicalTableScan(table=[[emp]])\n"
//        + "  LogicalAny(all=[false])\n"
//        + "    LogicalProject(emp.eptno=[eptno])\n"
//        + "      LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testFilterExprPartition() {
//    String sqlStr = "select name from emp where ANY {eptno > 0, role = param:varchar}";
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(emp.name=[name])\n"
//        + "  LogicalFilter(condition=[ANY(ANY(>(eptno, 0)), ANY(=(role, param: VARCHAR)))])\n"
//        + "    LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testProjectExprPartition() {
//    String sqlStr = "select ANY{[name], [eptno]} from emp";
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionProjectExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(ANY=[ANY(ANY([name]), ANY([eptno]))])\n"
//        + "  LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testFilterExprMerge() {
//    String sqlStr = "select name from emp where ANY {eptno > 0, role = param:varchar}";
//    // create a nested merge
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(emp.name=[name])\n"
//        + "  LogicalFilter(condition=[ANY(ANY(>(eptno, 0)), ANY(=(role, param: VARCHAR)))])\n"
//        + "    LogicalTableScan(table=[[emp]])\n");
//    //clear banned partition map
//    sql.clearHistory();
//    // merge 0th child
//    sql.withRule(MergeFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning( "LogicalProject(emp.name=[name])\n"
//        + "  LogicalFilter(condition=[ANY(>(eptno, 0), ANY(=(role, param: VARCHAR)))])\n"
//        + "    LogicalTableScan(table=[[emp]])\n");
//  }
//
//  @Test void testMergePartitionLoop() {
//    String sqlStr = "select name from emp where ANY {eptno > 0, role = param:varchar}";
//    // create a nested merge
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(emp.name=[name])\n"
//        + "  LogicalFilter(condition=[ANY(ANY(>(eptno, 0)), ANY(=(role, param: VARCHAR)))])\n"
//        + "    LogicalTableScan(table=[[emp]])\n");
//
//    // attempt to merge node created by a partition
//    sql.withRule(MergeFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("LogicalProject(emp.name=[name])\n"
//        + "  LogicalFilter(condition=[ANY(ANY(>(eptno, 0)), ANY(=(role, param: VARCHAR)))])\n"
//        + "    LogicalTableScan(table=[[emp]])\n");
//  }

//  @Test void testAggregatePartition() {
//    String sqlStr = "select any {[(count(name)], [count(empid)]} from emp " +
//        "group by any{[name], [empid]}";
//    // create a nested merge
//    Sql sql = sql(sqlStr)
//        .withRule(AggregatePartitionRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("");
//  }
//
//  @Test void testGenericCubeFilter() {
//    String sqlStr = "select name from emp where ANY {eptno > 0, role = param:varchar}";
//    // create a nested merge
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("");
//  }
//
//  @Test void testGenericCubeProject() {
//    String sqlStr = "select name, count(empid) from emp group by name";
//    // create a nested merge
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("");
//  }
//
//
//  @Test void testGenericCubeProjectFilter() {
//    String sqlStr = "select name, any{count(name), count(empid)} from emp " +
//        "where ANY {eptno > 0, role = param:varchar} group by any {name, empid}";
//    // create a nested merge
//    Sql sql = sql(sqlStr)
//        .withRule(PartitionFilterExprRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("");
//  }

//  @Test void testBitFilter() {
//    String sqlStr = "select name from emp where MULTI ANY " +
//        "{empid = TRUE, role = TRUE}";
//    // create a nested merge
//    Sql sql = sql(sqlStr)
//        .withRule(BitFilterRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("");
//  }

//  @Test void testPrefixSum() {
//    String sqlStr = "select name from emp where eptno BETWEEN param:int AND param: int group by name";
//    // create a nested merge
//    Sql sql = sql(sqlStr);
//        //.withRule(PrefixSumRule.Config.DEFAULT.toRule());
//    sql.checkPlanning("");
//
//  }


  private static final ThreadLocal<boolean[]> LINUXIFY =
      ThreadLocal.withInitial(() -> new boolean[] {true});

  private static final SqlWriterConfig SQL_WRITER_CONFIG =
      SqlPrettyWriter.config()
          .withAlwaysUseParentheses(true)
          .withUpdateSetListNewline(false)
          .withFromFolding(SqlWriterConfig.LineFolding.TALL)
          .withIndentation(0);

  /**
   * Allows fluent testing.
   */
  public class Sql {
    private final String sql;
    private SqlParser parser;
    private SimpleSqlToRel converter;
    private SqlNode parseTree;
    private RelNode relRoot;
    private PvdPlanner planner;

    Sql(String sql) {
      this.sql = sql;
      this.planner = new PvdPlanner();
      createParser();
      createConverter();
      parseTree();
      convertToRel();
      planner.setRoot(relRoot);
    }

    public Sql withRule(RelOptRule rule) {
      planner.addRule(rule);
      return this;
    }

    public void clearHistory(){
      planner.clearHistory();
    }

    private void createParser(){
      Quoting quoting = Quoting.DOUBLE_QUOTE;
      Casing unquotedCasing = Casing.TO_UPPER;
      Casing quotedCasing = Casing.UNCHANGED;
      final SqlParser.Config configBuilder =
          SqlParser.config().withParserFactory(SqlParserImpl.FACTORY)
              .withQuoting(quoting)
              .withUnquotedCasing(unquotedCasing)
              .withQuotedCasing(quotedCasing)
              .withConformance(SqlConformanceEnum.DEFAULT);
      UnaryOperator<SqlParser.Config> transform = UnaryOperator.identity();
      final SqlParser.Config config = transform.apply(configBuilder);
      parser = SqlParser.create(sql, config);
    }


    private void createConverter(){
      final RelDataTypeFactory typeFactory =
          new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
      Map<String, SimpleTable> catalog = TestUtil.createCatalog();
      final RexBuilder rexBuilder = new RexBuilder(typeFactory);
      this.converter = new SimpleSqlToRel(rexBuilder, planner, catalog);
    }

    public void checkPlanning(String expected) {
      RelNode r = planner.findBestExp();
      final String planAfter = RelOptUtil.toString(r);
      String jsonPlan = RelOptUtil.dumpPlan("json", r,
          SqlExplainFormat.JSON, SqlExplainLevel.NON_COST_ATTRIBUTES);
      System.out.println(jsonPlan);
      TestUtil.assertEqualsVerbose(expected, linux(planAfter));
    }

    private void parseTree() {
      if (parseTree != null){
        return;
      }
      final SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("Error while parsing SQL: " + sql, e);
      }
      parseTree = sqlNode;
    }

    private void convertToRel() {
      if (relRoot != null){
        return;
      }
      parseTree();
      relRoot = converter.convertQuery(parseTree);
    }

    private String linux(String s) {
      if (LINUXIFY.get()[0]) {
        s = Util.toLinux(s);
      }
      return s;
    }
  }
}
