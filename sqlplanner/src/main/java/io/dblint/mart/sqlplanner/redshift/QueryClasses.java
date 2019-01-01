package io.dblint.mart.sqlplanner.redshift;

import io.dblint.mart.sqlplanner.visitors.InsertVisitor;

public class QueryClasses {
  public final InsertVisitor insertContext;
  public final MaintenanceVisitor maintenanceContext;

  public QueryClasses(InsertVisitor insertContext,
                      MaintenanceVisitor maintenanceContext) {
    this.insertContext = insertContext;
    this.maintenanceContext = maintenanceContext;
  }
}
