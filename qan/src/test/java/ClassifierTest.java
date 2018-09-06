import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ClassifierTest {
  @Test
  public void lookupOnlyTest() throws SqlParseException {
    Classifier classifier = new Classifier();
    List<Classifier.QueryClass> queryClassList = classifier.classify("select a from b where c = 10");

    List<Classifier.QueryClass> expected = new ArrayList<>();
    expected.add(Classifier.QueryClass.LOOKUP);
    assertIterableEquals(expected, queryClassList);
  }

}