package io.inviscid.sqlplanner.enums;

import io.inviscid.sqlplanner.visitors.MySqlIndexVisitor;
import org.apache.calcite.rel.RelNode;

public enum MySqlEnum implements QueryType {
  BAD_NOINDEX {
    @Override
    public boolean isPassed(RelNode relNode, EnumContext context) {
      MySqlIndexVisitor visitor = new MySqlIndexVisitor();
      MySqlEnumContext enumContext = (MySqlEnumContext) context;
      visitor.go(relNode);
      enumContext.setIndices(visitor.getIndices());
      return visitor.hasLessIndexScans();
    }
  }
}
