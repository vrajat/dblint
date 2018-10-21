package io.inviscid.sqlplanner.planner;

import static org.junit.jupiter.api.Assertions.*;

import io.inviscid.sqlplanner.QanException;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PlannerTest {
  static Planner planner;

  @BeforeAll
  static void setPlanner() throws QanException {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Tpcds tpcds = new Tpcds("tpcds");
    SchemaPlus tpcdsSchemaPlus = rootSchema.add("tpcds", tpcds);
    tpcds.setSchemaPlus(tpcdsSchemaPlus);
    tpcds.addTables();

    planner = new Planner(tpcdsSchemaPlus);
  }

  @Test
  void planSelectScan() throws SqlParseException,
      ValidationException, RelConversionException {
    RelNode relNode = planner.plan("select d_date_id from date_dim");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
         + "LogicalProject(D_DATE_ID=[$1])\n"
         + "  LogicalTableScan(table=[[tpcds, DATE_DIM]])\n", explainPlan);

  }

  @Test
  void planSelectFilterScan() throws SqlParseException,
      ValidationException, RelConversionException {
   RelNode relNode = planner.plan("select d_date_id from date_dim where d_year=2018");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
         + "LogicalProject(D_DATE_ID=[$1])\n"
         + "  LogicalFilter(condition=[=(CAST($6):INTEGER, 2018)])\n"
         + "    LogicalTableScan(table=[[tpcds, DATE_DIM]])\n", explainPlan);
  }

  @Test
  void optimizeSelectFilterScan() throws SqlParseException,
      ValidationException, RelConversionException {
   RelNode relNode = planner.optimize("select d_date_id from date_dim where d_year=2018");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
         + "LogicalProject(D_DATE_ID=[$1])\n"
         + "  LogicalFilter(condition=[=(CAST($6):INTEGER, 2018)])\n"
         + "    LogicalTableScan(table=[[tpcds, DATE_DIM]])\n", explainPlan);
  }

  @Test
  void planSelectFilterIndex() throws SqlParseException,
      ValidationException, RelConversionException {
    RelNode relNode = planner.plan(
        "select i_color from item where i_item_id='abc'");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
            + "LogicalProject(I_COLOR=[$17])\n"
            + "  LogicalFilter(condition=[=(CAST($1):VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE "
            + "\"ISO-8859-1$en_US$primary\", 'abc')])\n"
            + "    LogicalTableScan(table=[[tpcds, ITEM]])\n", explainPlan);
  }

  @Test
  void optimizeSelectFilterIndex() throws SqlParseException,
      ValidationException, RelConversionException {
    RelNode relNode = planner.optimize(
        "select i_color from item where i_item_id='abc'");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
            + "LogicalProject(I_COLOR=[$17])\n"
            + "  LogicalIndexTableScan(table=[[tpcds, ITEM]], conditions=[[=(CAST($1):"
            + "VARCHAR CHARACTER SET \"ISO-8859-1\" COLLATE "
            + "\"ISO-8859-1$en_US$primary\", 'abc')]])\n", explainPlan);
  }

  @Test
  void digestTest() throws SqlParseException, ValidationException, RelConversionException {
    String digest = planner.digest("select i_color from item where i_color = 'abc'",
        SqlDialect.DatabaseProduct.MYSQL.getDialect());
    assertEquals("SELECT `I_COLOR`\n"
        + "FROM `tpcds`.`ITEM`\n"
        + "WHERE `I_COLOR` = ?", digest);
  }
}