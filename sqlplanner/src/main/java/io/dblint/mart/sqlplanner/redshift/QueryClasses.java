package io.dblint.mart.sqlplanner.redshift;

import io.dblint.mart.sqlplanner.visitors.CopyVisitor;
import io.dblint.mart.sqlplanner.visitors.CtasVisitor;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import io.dblint.mart.sqlplanner.visitors.UnloadVisitor;

public class QueryClasses {
  public final InsertVisitor insertContext;
  public final MaintenanceVisitor maintenanceContext;
  public final CtasVisitor ctasContext;
  public final UnloadVisitor unloadContext;
  public final CopyVisitor copyContext;

  /**
   * Holds context of a query. The valid context depends on the type of query.
   * @param insertContext Context if it is an INSERT query
   * @param maintenanceContext Context if it is a maintenance query
   * @param ctasContext Context if it is a CTAS query
   * @param unloadContext Context if it is an UNLOAD query
   * @param copyContext Context if it is a COPY query
   */
  public QueryClasses(InsertVisitor insertContext,
                      MaintenanceVisitor maintenanceContext,
                      CtasVisitor ctasContext,
                      UnloadVisitor unloadContext,
                      CopyVisitor copyContext) {
    this.insertContext = insertContext;
    this.maintenanceContext = maintenanceContext;
    this.ctasContext = ctasContext;
    this.unloadContext = unloadContext;
    this.copyContext = copyContext;
  }
}
