import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.ArrayList;
import java.util.List;

public class Classifier {
  public final Parser parser;
  public final List<ClassifyingVisitor> visitors;
  public enum QueryClass {
    LOOKUP
  }

  abstract class ClassifyingVisitor extends SqlBasicVisitor<Void> {
    protected final QueryClass queryClass;
    protected boolean passed;

    ClassifyingVisitor(QueryClass queryClass, boolean passed) {
      this.queryClass = queryClass;
      this.passed = passed;
    }

    public boolean isPassed() {
      return passed;
    }
  }

  class LookupVisitor extends ClassifyingVisitor {
    LookupVisitor() {
      super(QueryClass.LOOKUP, true);
    }

    public Void visit(SqlCall sqlCall) {
      if (sqlCall instanceof SqlSelect) {
        SqlSelect sqlSelect = (SqlSelect) sqlCall;
        if (sqlSelect.isDistinct()
          || sqlSelect.hasOrderBy()) {
          this.passed = false;
        }
      }
      return super.visit(sqlCall);
    }
  }

  Classifier() {
    this.parser = new Parser();
    this.visitors = new ArrayList<>();
    this.visitors.add(new LookupVisitor());
  }

  public List<QueryClass> classify(String sql) throws SqlParseException {
    List<QueryClass> classList = new ArrayList<>();
    SqlNode parseTree = parser.parse(sql);
    for (ClassifyingVisitor visitor: visitors) {
      parseTree.accept(visitor);
      if (visitor.isPassed()) {
        classList.add(visitor.queryClass);
      }
    }

    return classList;
  }
}
