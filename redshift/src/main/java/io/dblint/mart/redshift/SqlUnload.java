package io.dblint.mart.redshift;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

public class SqlUnload extends SqlCall {
  private final SqlSpecialOperator operator;
  private final SqlLiteral sqlStmt;
  private final SqlLiteral s3Loc;
  private final SqlLiteral role;
  private final SqlLiteral delim;
  private final SqlLiteral nullAs;

  /**
   * Represents an UNLOAD statement.
   * @param pos Position of the statement (for logs and exceptions)
   * @param sqlStmt Literal that stores the sql statement
   * @param s3Loc S3 Location where results are stored
   * @param role IAM Role for AWS operations
   * @param delim Delimiter in the o/p
   * @param nullAs NULL is stored as literal
   */
  public SqlUnload(SqlParserPos pos, SqlNode sqlStmt, SqlNode s3Loc,
                   SqlNode role, SqlNode delim, SqlNode nullAs) {
    super(pos);

    assert sqlStmt instanceof SqlLiteral;
    assert s3Loc instanceof SqlLiteral;
    assert role instanceof SqlLiteral;
    assert delim instanceof SqlLiteral;
    assert nullAs instanceof SqlLiteral;

    this.sqlStmt = (SqlLiteral) sqlStmt;
    this.s3Loc = (SqlLiteral) s3Loc;
    this.role = (SqlLiteral) role;
    this.delim = (SqlLiteral) delim;
    this.nullAs = (SqlLiteral) nullAs;

    operator = new SqlSpecialOperator("UNLOAD", SqlKind.OTHER);
  }

  @Override
  public SqlOperator getOperator() {
    return operator;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(sqlStmt, s3Loc, role, delim, nullAs);
  }

  public SqlLiteral getSqlStmt() {
    return sqlStmt;
  }

  public SqlLiteral getS3Loc() {
    return s3Loc;
  }
}
