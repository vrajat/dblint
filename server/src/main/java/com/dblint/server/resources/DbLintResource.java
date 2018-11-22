package com.dblint.server.resources;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.dblint.server.pojo.GitState;
import com.dblint.server.pojo.QueryResponse;
import com.dblint.server.pojo.SqlQuery;
import com.dblint.sqlplanner.planner.Parser;
import org.apache.calcite.sql.SqlDialect;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/api/dblint/")
@Produces(MediaType.APPLICATION_JSON)
public class DbLintResource {
  final Parser parser;
  final GitState gitState;

  public DbLintResource(Parser parser, GitState gitState) {
    this.parser = parser;
    this.gitState = gitState;
  }

  /**
   * Return a digest of the Sql Query.
   * @param sql SqlQuery object with SQL string and other properties
   * @return SQL Digest if successful else an error message
   */
  @POST
  @Path("/digest")
  @Metered
  @ExceptionMetered
  public QueryResponse digest(SqlQuery sql) {
    try {
      String digest = parser.digest(sql.sql,
              SqlDialect.DatabaseProduct.valueOf(sql.dialect.toUpperCase()).getDialect());
      return new QueryResponse(digest, true);
    } catch (Exception exc) {
      return new QueryResponse(exc.getMessage(), false);
    }
  }

  /**
   * Return a pretty of the Sql Query.
   * @param sql SqlQuery object with SQL string and other properties
   * @return Pretty Printed if successful else an error message
   */
  @POST
  @Path("/pretty")
  @Metered
  @ExceptionMetered
  public QueryResponse pretty(SqlQuery sql) {
    try {
      String pretty = parser.pretty(sql.sql,
              SqlDialect.DatabaseProduct.valueOf(sql.dialect.toUpperCase()).getDialect());
      return new QueryResponse(pretty, true);
    } catch (Exception exc) {
      return new QueryResponse(exc.getMessage(), false);
    }
  }

  @GET
  @Path("/version")
  @Metered
  public GitState version() {
    return gitState;
  }
}
