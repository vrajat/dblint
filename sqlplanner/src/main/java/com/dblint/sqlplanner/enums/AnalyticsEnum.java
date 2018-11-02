package com.dblint.sqlplanner.enums;

import com.dblint.sqlplanner.visitors.LookupVisitor;
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
  }
}
