package org.apache.calcite.test.pvd;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.pvd.SimpleTable;
import org.apache.calcite.pvd.SimpleSqlToRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.SqlDialect;
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
import org.apache.calcite.test.MockRelOptPlanner;
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

class SimpleSqlToRelTest {

  public final Sql sql(String sql) {
    return new Sql(sql);
  }

  /*
   * Test the ability to parse a string of diffSQL into an (unvalidated)
   * SQL parse tree.
   */
  @Test
  void testSimpleAny() {
    final String sql = "select ANY{[empid, rand], [empid]} from emp";
    String expected = "SELECT ANY { (`EMPID`, `RAND`), (`EMPID`) }\n"
        + "FROM `EMP`";
    sql(sql).ok(expected);
  }

  @Test
  void testNestedAny(){
    final String sql = "select any{[empid], [any{[eptno, name]}]} from emp";
    String expected = "SELECT ANY { (`EMPID`), (ANY { (`EPTNO`, `NAME`) }) }\n" +
        "FROM `EMP`";
    sql(sql).ok(expected);
  }

  @Test
  void testSimpleMulti() {
    final String sql = "select MULTI ANY{[empid, rand]} from emp";
    String expected = "SELECT MULTI ANY { (`EMPID`, `RAND`) }\n"
        + "FROM `EMP`";
    sql(sql).ok(expected);
  }

  @Test
  void testGroupMulti() {
    final String sql = "select empid from emp group by MULTI ANY{[deptno]}";
    String expected = "SELECT `EMPID`\n" +
        "FROM `EMP`\n" +
        "GROUP BY MULTI ANY { (`DEPTNO`) }";
    sql(sql).ok(expected);
  }

  @Test
  void testAnyMulti() {
    final String sql = "select empid from emp group by ANY{[deptno], [empid]}";
    String expected = "SELECT `EMPID`\n" +
        "FROM `EMP`\n" +
        "GROUP BY ANY { (`DEPTNO`), (`EMPID`) }";
    sql(sql).ok(expected);
  }

  @Test
  void testSimpleAnyFilter() {
    final String sql = "select a, b, c from t where ANY {a = 0, b= 0, c=0}";
    String expected = "SELECT `A`, `B`, `C`\n" +
        "FROM `T`\n" +
        "WHERE ANY { (`A` = 0), (`B` = 0), (`C` = 0) }";
    sql(sql).ok(expected);
  }

  @Test
  void testSimpleParam() {
    final String sql = "select a, b, c from t where a = param:int";
    String expected = "SELECT `A`, `B`, `C`\n" +
        "FROM `T`\n" +
        "WHERE (`A` = PARAM: INTEGER)";
    sql(sql).ok(expected);
  }

  @Test
  void testParamInAny() {
    final String sql = "select a, b, c from t where ANY {a = param:int, b= param:int}";
    String expected = "SELECT `A`, `B`, `C`\n" +
        "FROM `T`\n" +
        "WHERE ANY { (`A` = PARAM: INTEGER), (`B` = PARAM: INTEGER) }";
    sql(sql).ok(expected);
  }


  @Test
  void testAnyRoot() {
    final String sql = "ANY select a, b, c from t; select a from t where t = 4; select b, d from a";
    String expected = "ANY { (SELECT `A`, `B`, `C`\n"
        + "FROM `T`), (SELECT `A`\n"
        + "FROM `T`\n"
        + "WHERE (`T` = 4)), (SELECT `B`, `D`\n"
        + "FROM `A`) }"
        ;
    sql(sql).ok(expected);
  }


  /*
   * Test the ability to convert a parse tree with diff nodes into a relational
   * algebra expression tree (a diff tree).
   */
  @Test
  void testSimpleProjectAndScan(){
    String sqlStr = "select count(name) from emp";
    Sql sql = sql(sqlStr);
    sql.convertsTo("LogicalFieldAggregate(COUNT=[COUNT($name)])\n"
        + "  LogicalTableScan(table=[[emp]])\n");
  }

  @Test
  void testSimpleProjectAny(){
    String sqlStr = "select any{[name, any{[eptno]}]} from emp";
    Sql sql = sql(sqlStr);
    sql.convertsTo("LogicalProject(ANY=[ANY([name, ANY([eptno])])])\n"
        + "  LogicalTableScan(table=[[emp]])\n");
  }

  @Test
  void testSimpleFilter(){
    String sqlStr = "select count(name) from emp where eptno > 0 and role = param: varchar";
    Sql sql = sql(sqlStr);
    sql.convertsTo("LogicalFieldAggregate(COUNT=[COUNT($name)])\n"
        + "  LogicalFilter(condition=[AND(>(eptno, 0), =(role, param: VARCHAR))])\n"
        + "    LogicalTableScan(table=[[emp]])\n");
  }

  @Test
  void testAnyFilter() {
    String sqlStr = "select name from emp where ANY {eptno > 0, role = param:varchar}";
    Sql sql = sql(sqlStr);
    sql.convertsTo("LogicalProject(emp.name=[name])\n"
        + "  LogicalFilter(condition=[ANY(>(eptno, 0), =(role, param: VARCHAR))])\n"
        + "    LogicalTableScan(table=[[emp]])\n");
  }

  @Test
  void testSimpleGroup() {
    String sqlStr = "select sum(empid), count(empid), eptno from emp group by eptno";
    Sql sql = sql(sqlStr);
    sql.convertsTo("LogicalFieldAggregate(SUM=[SUM($empid)], COUNT=[COUNT($empid)], group=[[eptno]])\n"
        + "  LogicalProject(emp.eptno=[eptno])\n"
        + "    LogicalTableScan(table=[[emp]])\n");
  }

  @Test
  void testSimpleJoin() {
    String sqlStr = "select empid, emp.eptno from emp inner join depts on emp.eptno = depts.eptno";
    Sql sql = sql(sqlStr);
    sql.convertsTo("LogicalProject(emp.empid=[empid], emp.eptno=[emp.eptno])\n"
        + "  LogicalJoin(condition=[=(emp.eptno, depts.eptno)], joinType=[inner])\n"
        + "    LogicalTableScan(table=[[emp]])\n"
        + "    LogicalTableScan(table=[[depts]])\n");
  }

  @Test
  void testMultiLevelAny() {
    String sql = "ANY select any{[name, any{[eptno]}]} from emp;" +
        "select count(name) from emp where role = param: varchar;" +
        "select name from emp group by MULTI ANY{[role], [eptno]} ";
    String expected = "LogicalAny(all=[false])\n"
        + "  LogicalProject(ANY=[ANY([name, ANY([eptno])])])\n"
        + "    LogicalTableScan(table=[[emp]])\n"
        + "  LogicalFieldAggregate(COUNT=[COUNT($name)])\n"
        + "    LogicalFilter(condition=[=(role, param: VARCHAR)])\n"
        + "      LogicalTableScan(table=[[emp]])\n"
        + "  LogicalFieldAggregate(COUNT=[COUNT($name)], group=[[MULTI(ANY([role], [eptno]))]])\n"
        + "    LogicalProject(emp.name=[name])\n"
        + "      LogicalTableScan(table=[[emp]])\n";
    sql(sql).convertsTo(expected);
  }

  @Test void testBetween() {
    String sqlStr = "select name from emp where eptno BETWEEN param:int AND param: int group by name";
    String expected =  "LogicalFieldAggregate(group=[[name]])\n"
        + "  LogicalProject(emp.name=[name])\n"
        + "    LogicalFilter(condition=[AND(>=(eptno, param: INTEGER), <=(eptno, param: INTEGER))])\n"
        + "      LogicalTableScan(table=[[emp]])\n";
    sql(sqlStr).convertsTo(expected);
  }




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
    private SqlNode parseTree = null;
    private RelNode relRoot = null;

    Sql(String sql) {
      this.sql = sql;
      createParser();
      createConverter();
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
      RelOptPlanner planner = new MockRelOptPlanner(Contexts.EMPTY_CONTEXT);
      final RexBuilder rexBuilder = new RexBuilder(typeFactory);
      this.converter = new SimpleSqlToRel(rexBuilder, planner, catalog);
    }

    private SqlNode parseTree() {
      if (parseTree != null){
        return parseTree;
      }
      final SqlNode sqlNode;
      try {
        sqlNode = parser.parseQuery();
      } catch (SqlParseException e) {
        throw new RuntimeException("Error while parsing SQL: " + sql, e);
      }
      parseTree = sqlNode;
      return sqlNode;
    }

    private RelNode convertToRel() {
      if (relRoot != null){
        return relRoot;
      }
      parseTree();
      relRoot = converter.convertQuery(parseTree);
      return relRoot;
    }

    public void checkParseTree(SqlNode sqlNode, String expected){
      final SqlDialect dialect = AnsiSqlDialect.DEFAULT;
      final SqlWriterConfig c2 = SQL_WRITER_CONFIG.withDialect(dialect);
      final String actual = sqlNode.toSqlString(c -> c2).getSql();
      TestUtil.assertEqualsVerbose(expected, linux(actual));
    }

    public void ok(String expected){
      SqlNode sqlNode = parseTree();
      checkParseTree(sqlNode, expected);
    }

    public void convertsTo(String expected) {
      parseTree();
      convertToRel();
      String relString = RelOptUtil.toString(relRoot);
      TestUtil.assertEqualsVerbose(expected, linux(relString));
    }

    private String linux(String s) {
      if (LINUXIFY.get()[0]) {
        s = Util.toLinux(s);
      }
      return s;
    }
  }
}
