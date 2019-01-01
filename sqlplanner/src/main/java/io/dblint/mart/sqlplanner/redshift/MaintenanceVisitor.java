package io.dblint.mart.sqlplanner.redshift;

import io.dblint.mart.sqlplanner.visitors.ClassifyingVisitor;

public class MaintenanceVisitor extends ClassifyingVisitor {
  MaintenanceVisitor() {
    super(false);
  }

  void visit(String query) {
    if (query.startsWith("padb_fetch_sample:")
        || query.startsWith("Vacuum") ) {
      this.passed = true;
    }
  }
}
