package io.inviscid.sqlplanner.visitors;

import io.inviscid.sqlplanner.planner.LogicalIndexTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

public class MySqlIndexVisitor extends RelVisitor {
  int numIndexTableScans = 0;
  int numScans = 0;
  double numFullScanRows = 0.0;
  double numIndexScanRows = 0.0;

  @Override
  public void visit(RelNode relNode, int ordinal, RelNode parent) {
    if (relNode instanceof TableScan) {
      TableScan scan = ((TableScan) relNode);
      double rowCount = scan.getRows();
      numScans++;
      if (relNode instanceof LogicalIndexTableScan) {
        numIndexTableScans++;
        numIndexScanRows += rowCount;
      } else {
        numFullScanRows += rowCount;
      }
    }
    super.visit(relNode, ordinal, parent);
  }

  public boolean hasNoIndexScans() {
    return numIndexTableScans == 0;
  }

  public boolean hasLessIndexScans() {
    return numIndexScanRows < numFullScanRows;
  }
}
