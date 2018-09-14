package io.inviscid.qan.visitors;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;

public class RedshiftTooManyJoinsVisitor extends ClassifyingVisitor {
  int numJoins = 0;
  public final int limit;

  public RedshiftTooManyJoinsVisitor() {
    this(10);
  }

  public RedshiftTooManyJoinsVisitor(int limit) {
    super(true);
    this.limit = limit;
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlJoin) {
      numJoins++;
    }
    return super.visit(sqlCall);
  }

  @Override
  public boolean isPassed() {
    return numJoins > limit;
  }
}
