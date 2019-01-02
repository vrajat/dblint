package io.dblint.mart.sqlplanner.enums;

import io.dblint.mart.sqlplanner.visitors.LookupVisitor;
import io.dblint.mart.sqlplanner.visitors.TooManyJoinsVisitor;
import org.apache.calcite.sql.SqlNode;

/**
 * Created by rvenkatesh on 9/9/18.
 */
public enum AnalyticsEnum implements QueryType {
  LOOKUP {
    @Override
    public boolean isPassed(SqlNode sqlNode) {
      LookupVisitor lookupVisitor = new LookupVisitor();
      sqlNode.accept(lookupVisitor);
      return lookupVisitor.isPassed();
    }
  },
  BAD_TOOMANYJOINS {
    @Override
    public boolean isPassed(SqlNode sqlNode) {
      TooManyJoinsVisitor tooManyJoinsVisitor = new TooManyJoinsVisitor();
      sqlNode.accept(tooManyJoinsVisitor);
      return tooManyJoinsVisitor.isPassed();
    }
  }
}
