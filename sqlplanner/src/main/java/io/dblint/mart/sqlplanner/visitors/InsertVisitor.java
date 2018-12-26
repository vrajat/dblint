package io.dblint.mart.sqlplanner.visitors;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class InsertVisitor extends ClassifyingVisitor {
  private SqlIdentifier targetTable;
  private List<SqlIdentifier> sources = new ArrayList<>();
  private boolean insertClause = false;

  public InsertVisitor() {
    super(false);
  }

  @Override
  public Void visit(SqlIdentifier identifier) {
    sources.add(identifier);
    return super.visit(identifier);
  }

  @Override
  public Void visit(SqlCall sqlCall) {
    if (sqlCall instanceof SqlInsert) {
      SqlInsert insert = (SqlInsert) sqlCall;
      if (insert.getTargetTable() instanceof SqlIdentifier) {
        targetTable = (SqlIdentifier) insert.getTargetTable();
        insertClause = true;
        insert.getSource().accept(this);
        this.passed = true;
      }
      return null;
    }

    if (insertClause) {
      if (sqlCall instanceof SqlJoin) {
        SqlJoin join = (SqlJoin) sqlCall;
        join.getLeft().accept(this);
        join.getRight().accept(this);
      } else if (sqlCall instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) sqlCall;
        if (select.getFrom() != null) {
          select.getFrom().accept(this);
        }
      } else if (sqlCall instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) sqlCall;
        if (basicCall.getOperator() instanceof SqlAsOperator) {
          basicCall.operand(0).accept(this);
        } else {
          return super.visit(sqlCall);
        }
      } else {
        return super.visit(sqlCall);
      }
    }

    return null;
  }

  public String getTargetTable() {
    return targetTable.toString();
  }

  public List<String> getSources() {
    return sources.stream().map(SqlIdentifier::toString).collect(Collectors.toList());
  }
}
