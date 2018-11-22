package io.dblint.mart.metricsink.mysql;

import static org.junit.jupiter.api.Assertions.*;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaParserTest {
  static Logger logger = LoggerFactory.getLogger(SchemaParserTest.class);

  static String tableStructureStr =
      "<table_structure name=\"a_table\">\n"
      + "<field Field=\"updated_on\" Type=\"datetime\" Null=\"NO\" Key=\"\" "
          +  "Extra=\"\" Comment=\"\" />\n"
      + "<field Field=\"created_on\" Type=\"datetime\" Null=\"NO\" Key=\"\" "
          +  "Extra=\"\" Comment=\"\" />\n"
      + "<key Table=\"indexed_table\" Non_unique=\"0\" Key_name=\"PRIMARY\" "
          + "Seq_in_index=\"1\" Column_name=\"id\" Collation=\"A\" Cardinality=\"45\" Null=\"\" "
          + "Index_type=\"BTREE\" Comment=\"\" Index_comment=\"\" />\n"
      + "<key Table=\"indexed_table\" Non_unique=\"0\" Key_name=\"SECONDARY\" "
          + "Seq_in_index=\"1\" Column_name=\"id\" Collation=\"A\" Cardinality=\"45\" Null=\"\" "
          + "Index_type=\"BTREE\" Comment=\"\" Index_comment=\"\" />\n"
      + "<options Name=\"indexed_table\" Engine=\"InnoDB\" Version=\"10\" "
          + "Row_format=\"Compact\" Rows=\"50\" Avg_row_length=\"500\" Data_length=\"20000\" "
          + "Max_data_length=\"0\" Index_length=\"65536\" Data_free=\"0\" Auto_increment=\"202\" "
          + "Create_time=\"2013-12-19 10:40:30\" Collation=\"latin1_swedish_ci\" "
          +  "Create_options=\"\" Comment=\"\" />\n"
      +"</table_structure>\n";


  static String databaseStr = "<database name=\"a_db\">\n"
      + tableStructureStr
      + tableStructureStr
      + "</database>\n";

  static String mySqlStr = "<?xml version=\"1.0\"?>\n"
      + "<mysqldump xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
      + databaseStr
      + "</mysqldump>";


  @Test
  void parseFieldTest() throws XMLStreamException, IOException {
    String fieldStr = "<field Field=\"updated_on\" Type=\"datetime\" "
        + "Null=\"NO\" Key=\"\" Extra=\"\" Comment=\"\" />";
    logger.debug(fieldStr);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fieldStr.getBytes());
    SchemaParser.Field field = SchemaParser.parseField(byteArrayInputStream);

    assertNotNull(field);
    assertEquals("updated_on", field.field);
    assertEquals("datetime", field.type);
    assertEquals("NO", field.isNull);
    assertEquals("", field.key);
    assertEquals("", field.extra);
    assertEquals("", field.comment);
  }

  @Test
  void parseKeyTest() throws XMLStreamException, IOException {
    String keyStr = "<key Table=\"indexed_table\" Non_unique=\"0\" Key_name=\"PRIMARY\" "
        + "Seq_in_index=\"1\" Column_name=\"id\" Collation=\"A\" Cardinality=\"45\" Null=\"\" "
        + "Index_type=\"BTREE\" Comment=\"\" Index_comment=\"\" />";

    logger.debug(keyStr);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(keyStr.getBytes());
    SchemaParser.Key key = SchemaParser.parseKey(byteArrayInputStream);

    assertEquals("indexed_table", key.table);
  }

  @Test
  void parseOptionsTest() throws XMLStreamException, IOException {
    String optionsStr = "<options Name=\"indexed_table\" Engine=\"InnoDB\" Version=\"10\" "
        + "Row_format=\"Compact\" Rows=\"50\" Avg_row_length=\"500\" Data_length=\"20000\" "
        + "Max_data_length=\"0\" Index_length=\"65536\" Data_free=\"0\" Auto_increment=\"202\" "
        + "Create_time=\"2013-12-19 10:40:30\" Collation=\"latin1_swedish_ci\" Create_options=\"\""
        + " Comment=\"\" />";

    logger.debug(optionsStr);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(optionsStr.getBytes());
    SchemaParser.Options options = SchemaParser.parseOptions(byteArrayInputStream);

    assertEquals("indexed_table", options.name);
    assertEquals(50, options.rows.intValue());
  }

  @Test
  void parseTableStructureTest() throws XMLStreamException, IOException {
    logger.debug(tableStructureStr);
    ByteArrayInputStream byteStream = new ByteArrayInputStream(tableStructureStr.getBytes());
    SchemaParser.TableStructure tableStructure = SchemaParser.parseTableStructure(byteStream);

    assertEquals("a_table", tableStructure.name);
    assertEquals(2, tableStructure.fieldList.size());
    assertEquals(2, tableStructure.keys.size());
    assertEquals("indexed_table", tableStructure.options.name);

  }

  @Test
  void parseDatabaseTest() throws XMLStreamException, IOException {
    logger.debug(databaseStr);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(databaseStr.getBytes());
    SchemaParser.Database database = SchemaParser.parseDatabase(byteArrayInputStream);

    assertEquals("a_db", database.name);
    assertEquals(2, database.tables.size());
  }

  @Test
  void parseMySqlDumpTest() throws XMLStreamException, IOException {
    logger.debug(mySqlStr);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(mySqlStr.getBytes());
    SchemaParser.Database database = SchemaParser.parseMySqlDump(byteArrayInputStream);

    assertEquals("a_db", database.name);
    assertEquals(2, database.tables.size());
  }
}