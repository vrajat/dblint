import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ParserTest {
  @Test
  public void sanityTest() throws SqlParseException {
    Parser parser = new Parser();
    SqlNode sqlNode = parser.parse("select 1 from tbl");
    assertNotNull(sqlNode);
  }

  @Test
  public void testError() {
    Parser parser = new Parser();
    assertThrows(SqlParseException.class, () -> parser.parse("select from"));
  }
}