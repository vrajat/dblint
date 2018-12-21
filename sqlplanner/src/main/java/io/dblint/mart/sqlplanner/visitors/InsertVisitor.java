package io.dblint.mart.sqlplanner.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;

public class InsertVisitor extends ClassifyingVisitor {
  SqlNode targetTable;
  SqlNode source;

  public InsertVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlInsert) {
      this.passed = true;
      SqlInsert insert = (SqlInsert) sqlCall;
      targetTable = insert.getTargetTable();
      source = insert.getSource();
    }
    return super.visit(sqlCall);
  }

  public SqlNode getTargetTable() {
    return targetTable;
  }

  public SqlNode getSource() {
    return source;
  }
}
