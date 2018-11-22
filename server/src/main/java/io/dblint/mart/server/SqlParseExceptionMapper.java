package io.dblint.mart.server;

import org.apache.calcite.sql.parser.SqlParseException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class SqlParseExceptionMapper implements ExceptionMapper<SqlParseException> {
  @Override
  public Response toResponse(SqlParseException exception) {
    return Response.status(400)
        .entity(exception.getMessage())
        .type(MediaType.APPLICATION_JSON)
        .build();
  }
}
