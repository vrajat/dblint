package com.dblint.server.pojo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryResponseTest {
  private static QueryResponse success;
  private static QueryResponse error;

  @BeforeAll
  static void setResponse() {
    success = new QueryResponse("select", true);
    error = new QueryResponse("errMsg", false);
  }

  static Stream<Arguments> responseProvider() {
    return Stream.of(
      Arguments.arguments(success, "{\"sql\":\"select\","
            + "\"errorMessage\":null,\"success\":true}"),
        Arguments.arguments(error, "{\"sql\":null,"
            + "\"errorMessage\":\"errMsg\",\"success\":false}")
    );
  }

  @ParameterizedTest
  @MethodSource("responseProvider")
  void serialize(QueryResponse response, String expected)
      throws JsonProcessingException {
    String serialized = new ObjectMapper().writeValueAsString(response);
    assertEquals(expected, serialized);
  }

  static Stream<Arguments> stringProvider() {
    return Stream.of(
      Arguments.arguments(success, "{\"sql\":\"select\","
            + "\"errorMessage\":null,\"success\":true}"),
        Arguments.arguments(error, "{\"sql\":null,"
            + "\"errorMessage\":\"errMsg\",\"success\":false}")
    );
  }

  @ParameterizedTest
  @MethodSource("stringProvider")
  void deSerialize(QueryResponse expected, String response)
      throws IOException {
    QueryResponse deserialized = new ObjectMapper().readValue(response, QueryResponse.class);
    assertEquals(deserialized, expected);
  }
}
