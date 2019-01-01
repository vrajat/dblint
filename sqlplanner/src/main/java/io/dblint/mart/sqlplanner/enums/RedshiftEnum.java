package io.dblint.mart.sqlplanner.enums;

import io.dblint.mart.sqlplanner.visitors.TooManyJoinsVisitor;
import org.apache.calcite.sql.SqlNode;

public enum RedshiftEnum implements QueryType {
  BAD_TOOMANYJOINS {
    @Override
    public boolean isPassed(SqlNode sqlNode) {
      TooManyJoinsVisitor tooManyJoinsVisitor = new TooManyJoinsVisitor();
      sqlNode.accept(tooManyJoinsVisitor);
      return tooManyJoinsVisitor.isPassed();
    }
  }
}
