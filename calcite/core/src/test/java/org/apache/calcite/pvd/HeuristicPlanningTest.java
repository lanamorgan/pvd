package org.apache.calcite.test.pvd;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.pvd.PvdPlanner;
import org.apache.calcite.plan.pvd.PvdRuleDriverType;
import org.apache.calcite.pvd.SimpleTable;
import org.apache.calcite.pvd.SimpleSqlToRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
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

class HeuristicPlanningTest {

  public Sql sql(String sql) {
    return new Sql(sql);
  }
//
//  @Test
//  void testOne(){
//    String sqlStr = "select name from emp where name = param:varchar";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(1);
//    System.out.println("Test #1");
//  }
//
//  @Test void testTwo() {
//    //test replaces count query
//    String sqlStr = "select count(empid) from emp where name = param:varchar group by ANY{[eptno], [empid]} ";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(7);
//    System.out.println("Test #2");
//  }
//
//  @Test void testThree(){
//    //test replaces count query
//    String sqlStr = "select empid from emp where ANY{name = param:varchar, empid = 3, empid = 4}";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(36);
//    System.out.println("Test #3");
//  }
//
//
//  @Test void testFour(){
//    //test replaces count query
//    String sqlStr = "select empid from emp where ANY{name = param:varchar, empid=3}";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(9);
//    System.out.println("Test #4");
//  }
//
//  @Test void testFive(){
//    //test replaces count query
//    String sqlStr = "select empid from emp group by ANY{[eptno], [empid], [empid, eptno]," +
//        "[name], [eptno, name], [empid,name], [eptno, empid], [role, eptno], [role]}";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(21148);
//    System.out.println("Test #5");
//  }
//
//  @Test void testSix(){
//    //test replaces count query
//    String sqlStr = "select empid from emp where ANY{name = param:varchar, empid = 3}"
//        + " group by ANY{[eptno], [empid]}";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(32);
//    System.out.println("Test #6");
//  }
//
//  @Test void testSeven(){
//    //test replaces count query
//    String sqlStr = "select ANY{[eptno], [empid], [eptno, empid]} from emp"
//        + " where ANY{name = param:varchar, empid = 3, empid =4}"
//        + " group by ANY{[eptno], [empid], [eptno, empid]}";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(60);
//    System.out.println("Test #7");
//  }
//
//  @Test void testEight(){
//    //test replaces count query
//    String sqlStr = "select ANY{[eptno], [empid], [eptno, empid], [role]} from emp"
//        + " where ANY{name = param:varchar, empid = 3, empid =4, role = param:varchar}"
//        + " group by ANY{[eptno], [empid], [eptno, empid], [role]}";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(308);
//    System.out.println("Test #8");
//  }

//  @Test void testNine(){
//    //test replaces count query
//    String sqlStr = "select ANY{[eptno], [empid], [empid, eptno],"
//        + "[name], [eptno, name], [empid,name], [eptno, empid], [role, eptno], [role]} from emp"
//        + " where ANY{name = param:varchar, empid = 3, empid =4, role = param:varchar}"
//        + " group by ANY{[eptno], [empid], [empid, eptno],[name], [eptno, name], [empid,name],"
//        + " [eptno, empid], [role, eptno], [role]}";
//    Sql sql = sql(sqlStr);
//    sql.checkPlanning(26);
//    System.out.println("Test #9");
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
      this.planner = new PvdPlanner(PvdRuleDriverType.DFS);
      planner.addAllPvdRules();
      createParser();
      createConverter();
      parseTree();
      convertToRel();
      planner.setRoot(relRoot);
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

    public void checkPlanning(int numExpected) {
      long startTime = System.nanoTime();
      RelNode r = planner.findBestExp();
      long endTime = System.nanoTime();
      long timeElapsed = endTime - startTime;
      System.out.println("==========================================");
//      System.out.println("Number of nodes:");
//      System.out.println("Number of diffnodes: 3 (non-parameter)");
//      System.out.println("% diffnode: " + (3.0/11));
      System.out.println("Time: " + timeElapsed/1000000);
      System.out.println("Number of steps taken: " + planner.numPlansFound());
      TestUtil.assertEqualsVerbose(numExpected, planner.numPlansFound());
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
  }
}
