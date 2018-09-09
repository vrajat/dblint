package io.inviscid.qan.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlSelect;

/**
 * Created by rvenkatesh on 9/9/18.
 */
public class LookupVisitor extends ClassifyingVisitor {
  public LookupVisitor() {
    super(true);
  }

  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlSelect) {
      SqlSelect sqlSelect = (SqlSelect) sqlCall;
      if (sqlSelect.isDistinct()
          || sqlSelect.hasOrderBy()) {
        this.passed = false;
      }
    }
    return super.visit(sqlCall);
  }
}
