package io.dblint.mart.sqlplanner.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;

public class InsertVisitor extends ClassifyingVisitor {
  public InsertVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlInsert) {
      this.passed = true;
    }
    return super.visit(sqlCall);
  }
}
