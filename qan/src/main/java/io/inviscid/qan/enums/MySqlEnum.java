package io.inviscid.qan.enums;

import io.inviscid.qan.visitors.MySqlIndexVisitor;
import org.apache.calcite.rel.RelNode;

public enum MySqlEnum implements QueryType {
  BAD_NOINDEX {
    @Override
    public boolean isPassed(RelNode relNode) {
      MySqlIndexVisitor visitor = new MySqlIndexVisitor();
      visitor.go(relNode);
      return visitor.hasNoIndexScans();
    }
  }
}
