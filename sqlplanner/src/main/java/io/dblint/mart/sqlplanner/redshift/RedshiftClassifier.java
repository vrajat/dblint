package io.dblint.mart.sqlplanner.redshift;

import io.dblint.mart.sqlplanner.planner.Parser;
import io.dblint.mart.sqlplanner.planner.RedshiftParser;
import io.dblint.mart.sqlplanner.visitors.CtasVisitor;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class RedshiftClassifier {

  public final Parser parser;

  public RedshiftClassifier() {
    parser = new Parser(RedshiftParser.FACTORY);
  }

  /**
   * Classify whether a query is an insert statement.
   * @param query Query String
   * @return Returns the visitor with additional info about the insert statement
   * @throws SqlParseException Exception thrown if there is a syntax error
   */
  public QueryClasses classify(String query) throws SqlParseException {
    // Maintenance Visitor is before parse as regex is used.

    MaintenanceVisitor maintenanceVisitor = new MaintenanceVisitor();
    InsertVisitor insertVisitor = new InsertVisitor();
    CtasVisitor ctasVisitor = new CtasVisitor();
    maintenanceVisitor.visit(query);

    if (!maintenanceVisitor.isPassed()) {
      SqlNode sqlNode = parser.parse(query);
      sqlNode.accept(insertVisitor);
      sqlNode.accept(ctasVisitor);
    }

    return new QueryClasses(insertVisitor, maintenanceVisitor, ctasVisitor);
  }
}
