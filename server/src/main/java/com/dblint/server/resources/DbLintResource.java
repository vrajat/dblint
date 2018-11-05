package com.dblint.server.resources;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Metered;
import com.dblint.sqlplanner.planner.Parser;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/dblint/")
@Produces(MediaType.APPLICATION_JSON)
public class DbLintResource {
  final Parser parser;

  public DbLintResource(Parser parser) {
    this.parser = parser;
  }

  @POST
  @Path("/digest")
  @Metered
  @ExceptionMetered
  public String digest(String sql) throws SqlParseException {
    return parser.digest(sql, SqlDialect.DatabaseProduct.MYSQL.getDialect());
  }
}
