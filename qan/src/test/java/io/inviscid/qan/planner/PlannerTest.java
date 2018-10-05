package io.inviscid.qan.planner;

import static org.junit.jupiter.api.Assertions.*;

import io.inviscid.qan.QanException;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Test;

class PlannerTest {
  @Test
  void planTpcds() throws QanException, SqlParseException,
      ValidationException, RelConversionException {
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    Tpcds tpcds = new Tpcds("tpcds");
    SchemaPlus tpcdsSchemaPlus = rootSchema.add("tpcds", tpcds);
    tpcds.setSchemaPlus(tpcdsSchemaPlus);
    tpcds.addTables();

    Planner planner = new Planner(tpcdsSchemaPlus);
    RelNode relNode = planner.plan("select d_date_id from date_dim");
    String explainPlan = RelOptUtil.dumpPlan("--Logical Plan", relNode,
        SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES);
    assertNotNull(relNode);
    assertEquals(
        "--Logical Plan\n"
         + "LogicalProject(D_DATE_ID=[$1])\n"
         + "  EnumerableTableScan(table=[[tpcds, DATE_DIM]])\n", explainPlan);

  }
}