package io.inviscid.qan.visitors;

import io.inviscid.qan.planner.LogicalIndexTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

public class MySqlIndexVisitor extends RelVisitor {
  int numIndexTableScans = 0;
  int numScans = 0;

  @Override
  public void visit(RelNode relNode, int ordinal, RelNode parent) {
    if (relNode instanceof TableScan) {
      numScans++;
      if (relNode instanceof LogicalIndexTableScan) {
        numIndexTableScans++;
      }
    }
    super.visit(relNode, ordinal, parent);
  }

  public boolean hasNoIndexScans() {
    return numIndexTableScans == 0;
  }
}
