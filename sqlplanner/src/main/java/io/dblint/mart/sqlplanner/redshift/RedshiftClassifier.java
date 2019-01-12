package io.dblint.mart.sqlplanner.redshift;

import io.dblint.mart.redshift.SqlRedshiftParser;
import io.dblint.mart.sqlplanner.planner.Parser;
import io.dblint.mart.sqlplanner.visitors.CopyVisitor;
import io.dblint.mart.sqlplanner.visitors.CtasVisitor;
import io.dblint.mart.sqlplanner.visitors.InsertVisitor;
import io.dblint.mart.sqlplanner.visitors.UnloadVisitor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class RedshiftClassifier {

  public final Parser parser;

  public RedshiftClassifier() {
    parser = new Parser(SqlRedshiftParser.FACTORY);
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
    UnloadVisitor unloadVisitor = new UnloadVisitor();
    CopyVisitor copyVisitor = new CopyVisitor();

    maintenanceVisitor.visit(query);

    if (!maintenanceVisitor.isPassed()) {
      SqlNode sqlNode = parser.parse(query);
      sqlNode.accept(insertVisitor);
      sqlNode.accept(ctasVisitor);
      sqlNode.accept(unloadVisitor);
      sqlNode.accept(copyVisitor);
    }

    return new QueryClasses(insertVisitor, maintenanceVisitor, ctasVisitor,
        unloadVisitor, copyVisitor);
  }
}
