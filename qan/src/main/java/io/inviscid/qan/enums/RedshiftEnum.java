package io.inviscid.qan.enums;

import io.inviscid.qan.visitors.RedshiftTooManyJoinsVisitor;
import org.apache.calcite.sql.SqlNode;

public enum RedshiftEnum implements QueryType {
  BAD_TOOMANYJOINS {
    @Override
    public boolean isPassed(SqlNode sqlNode) {
      RedshiftTooManyJoinsVisitor redshiftTooManyJoinsVisitor = new RedshiftTooManyJoinsVisitor();
      sqlNode.accept(redshiftTooManyJoinsVisitor);
      return redshiftTooManyJoinsVisitor.isPassed();
    }
  }
}
