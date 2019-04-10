package io.dblint.mart.sqlplanner.visitors;

import io.dblint.mart.sqlplanner.planner.SqlMartLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;

import java.util.List;

public class LiteralShuffle extends SqlShuttle {
  @Override
  public SqlNode visit(SqlLiteral literal) {
    return SqlMartLiteral.createLiteral(literal);
  }

  @Override
  public SqlNode visit(final SqlCall call) {
    // Handler creates a new copy of 'call' only if one or more operands
    // change.
    ArgHandler<SqlNode> argHandler;
    if (call instanceof SqlInsert) {
      argHandler = new InsertArgHandler(call);
    } else if (call instanceof SqlUpdate) {
      argHandler = new UpdateArgHandler(call);
    } else if (call instanceof SqlDelete) {
      argHandler = new DeleteArgHandler(call);
    } else {
      argHandler = new CallCopyingArgHandler(call, false);
    }

    call.getOperator().acceptCall(this, call, false, argHandler);
    return argHandler.result();
  }

  protected abstract class DmlCopyingArgHandler implements ArgHandler<SqlNode> {
    boolean update;
    SqlNode[] clonedOperands;
    protected final SqlCall call;

    public DmlCopyingArgHandler(SqlCall call) {
      this.call = call;
      this.update = false;
      final List<SqlNode> operands = call.getOperandList();
      this.clonedOperands = operands.toArray(new SqlNode[0]);
    }

    public SqlNode visitChild(
        SqlVisitor<SqlNode> visitor,
        SqlNode expr,
        int index,
        SqlNode operand) {
      if (operand == null) {
        return null;
      }
      SqlNode newOperand = operand.accept(LiteralShuffle.this);
      if (newOperand != operand) {
        update = true;
      }
      clonedOperands[index] = newOperand;
      return newOperand;
    }
  }

  class InsertArgHandler extends DmlCopyingArgHandler {
    public InsertArgHandler(SqlCall call) {
      super(call);
    }

    public SqlNode result() {
      if (update) {
        return new SqlInsert(
            call.getParserPosition(),
            (SqlNodeList) clonedOperands[0],
            clonedOperands[1],
            clonedOperands[2],
            (SqlNodeList) clonedOperands[3]);
      } else {
        return call;
      }
    }
  }

  class UpdateArgHandler extends DmlCopyingArgHandler {
    public UpdateArgHandler(SqlCall call) {
      super(call);
    }

    public SqlNode result() {
      if (update) {
        return new SqlUpdate(
            call.getParserPosition(),
            clonedOperands[0],
            (SqlNodeList) clonedOperands[1],
            (SqlNodeList) clonedOperands[2],
            clonedOperands[3],
            ((SqlUpdate) call).getSourceSelect(),
            (SqlIdentifier) clonedOperands[4]);
      } else {
        return call;
      }
    }
  }

  class DeleteArgHandler extends DmlCopyingArgHandler {
    public DeleteArgHandler(SqlCall call) {
      super(call);
    }

    public SqlNode result() {
      if (update) {
        return new SqlDelete(
            call.getParserPosition(),
            clonedOperands[0],
            clonedOperands[1],
            ((SqlDelete) call).getSourceSelect(),
            (SqlIdentifier) clonedOperands[2]);
      } else {
        return call;
      }
    }
  }
}
