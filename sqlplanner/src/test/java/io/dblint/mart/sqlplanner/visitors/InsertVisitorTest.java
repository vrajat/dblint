package io.dblint.mart.sqlplanner.visitors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.dblint.mart.sqlplanner.planner.Parser;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class InsertVisitorTest {
  static Logger logger = LoggerFactory.getLogger(InsertVisitorTest.class);

  static class TestCase {
    public final String name;
    public final String targetTable;
    public final List<String> sourceTables;
    public final String query;

    @JsonCreator
    public TestCase(
        @JsonProperty("name") String name,
        @JsonProperty("target") String targetTable,
        @JsonProperty("sources") List<String> sourceTables,
        @JsonProperty("query") String query) {
      this.name = name;
      this.targetTable = targetTable;
      this.sourceTables = sourceTables;
      this.query = query;
    }

    Arguments getArgs() {
      return Arguments.of(
          this.name,
          this.targetTable,
          this.sourceTables,
          this.query
      );
    }
  }

  static class SqlProvider implements ArgumentsProvider {
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
      Stream.Builder<Arguments> argumentsBuilder = Stream.builder();
      YAMLMapper mapper = new YAMLMapper();
      YAMLFactory factory = new YAMLFactory();
      for (String filename : extensionContext.getTags()) {
        try {
          JsonParser parser = factory.createParser(this.getClass().getResource(filename));
          List<TestCase> cases = mapper.readValue(parser, new TypeReference<List<TestCase>>(){});
          for(TestCase testCase : cases) {
            argumentsBuilder.add(testCase.getArgs());
          }
        } catch (IOException exc) {
          logger.warn("Failed to process " + filename + ":" + exc.getMessage());
        }
      }
      return argumentsBuilder.build();
    }
  }

  static Parser parser;
  @BeforeAll
  static void setParser() {
    parser = new Parser();
  }

  @ParameterizedTest(name="[{index}] {0}")
  @ArgumentsSource(SqlProvider.class)
  @Tags({@Tag("/insertSuccess.yaml")})
  void sanityTest(String name, String targetTable,
                  List<String> sourceTables, String query) throws SqlParseException {
    SqlNode node = parser.parse(query);
    InsertVisitor visitor = new InsertVisitor();
    node.accept(visitor);

    logger.debug("Expected:" + sourceTables.size());
    logger.debug("Actual: " + visitor.getSources().size());

    assertTrue(visitor.passed);
    assertEquals(targetTable, visitor.getTargetTable().toString());

    Iterator<String> expected = sourceTables.iterator();
    Iterator<String> actual = visitor.getSources().iterator();
    while (expected.hasNext() && actual.hasNext()) {
      assertEquals(expected.next(), actual.next());
    }
    assertFalse(expected.hasNext());
    assertFalse(actual.hasNext());
  }
}