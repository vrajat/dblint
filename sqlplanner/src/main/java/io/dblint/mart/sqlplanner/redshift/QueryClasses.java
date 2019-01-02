package io.dblint.mart.sqlplanner.redshift;

import io.dblint.mart.sqlplanner.visitors.CtasVisitor;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;

public class QueryClasses {
  public final InsertVisitor insertContext;
  public final MaintenanceVisitor maintenanceContext;
  public final CtasVisitor ctasContext;

  /**
   * Holds context of a query. The valid context depends on the type of query.
   * @param insertContext Context if it is an INSERT query
   * @param maintenanceContext Context if it is a maintenance query
   * @param ctasContext Context if it is a CTAS query
   */
  public QueryClasses(InsertVisitor insertContext,
                      MaintenanceVisitor maintenanceContext,
                      CtasVisitor ctasContext) {
    this.insertContext = insertContext;
    this.maintenanceContext = maintenanceContext;
    this.ctasContext = ctasContext;
  }
}
