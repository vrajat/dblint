package io.inviscid.metricsink.mysql;

import static org.junit.jupiter.api.Assertions.*;

import javax.xml.stream.XMLStreamException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;

class SchemaParserTest {
    static String tableStructureStr =
        "<table_structure name=\"a_table\">"
        + "<field Field=\"updated_on\" Type=\"datetime\" Null=\"NO\" Key=\"\" "
          +  "Extra=\"\" Comment=\"\" />"
        + "<field Field=\"created_on\" Type=\"datetime\" Null=\"NO\" Key=\"\" "
          +  "Extra=\"\" Comment=\"\" />"
        + "<key Table=\"indexed_table\" Non_unique=\"0\" Key_name=\"PRIMARY\" "
          + "Seq_in_index=\"1\" Column_name=\"id\" Collation=\"A\" Cardinality=\"45\" Null=\"\" "
          + "Index_type=\"BTREE\" Comment=\"\" Index_comment=\"\" />"
        + "<key Table=\"indexed_table\" Non_unique=\"0\" Key_name=\"SECONDARY\" "
          + "Seq_in_index=\"1\" Column_name=\"id\" Collation=\"A\" Cardinality=\"45\" Null=\"\" "
          + "Index_type=\"BTREE\" Comment=\"\" Index_comment=\"\" />"
        + "<options Name=\"indexed_table\" Engine=\"InnoDB\" Version=\"10\" "
          + "Row_format=\"Compact\" Rows=\"50\" Avg_row_length=\"500\" Data_length=\"20000\" "
          + "Max_data_length=\"0\" Index_length=\"65536\" Data_free=\"0\" Auto_increment=\"202\" "
          + "Create_time=\"2013-12-19 10:40:30\" Collation=\"latin1_swedish_ci\" "
          +  "Create_options=\"\" Comment=\"\" />"
        +"</table_structure>";


    static String databaseStr = "<database name=\"a_db\">"
        + tableStructureStr
        + tableStructureStr
        + "</database>";

  @Test
  void parseFieldTest() throws XMLStreamException, IOException {
    String fieldStr = "<field Field=\"updated_on\" Type=\"datetime\" "
        + "Null=\"NO\" Key=\"\" Extra=\"\" Comment=\"\" />";
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

    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(optionsStr.getBytes());
    SchemaParser.Options options = SchemaParser.parseOptions(byteArrayInputStream);

    assertEquals("indexed_table", options.name);
  }

  @Test
  void parseTableStructureTest() throws XMLStreamException, IOException {
    ByteArrayInputStream byteStream = new ByteArrayInputStream(tableStructureStr.getBytes());
    SchemaParser.TableStructure tableStructure = SchemaParser.parseTableStructure(byteStream);

    assertEquals("a_table", tableStructure.name);
    assertEquals(2, tableStructure.fieldList.size());
    assertEquals(2, tableStructure.keys.size());
    assertEquals("indexed_table", tableStructure.options.name);

  }

  @Test
  void parseDatabaseTest() throws XMLStreamException, IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(databaseStr.getBytes());
    SchemaParser.Database database = SchemaParser.parseDatabase(byteArrayInputStream);

    assertEquals("a_db", database.name);
    assertEquals(2, database.tables.size());
  }
}