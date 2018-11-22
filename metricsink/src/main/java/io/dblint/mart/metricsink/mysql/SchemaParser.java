package io.dblint.mart.metricsink.mysql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

public class SchemaParser {
  public static class Field {
    public final String field;
    public final String type;
    public final String isNull;
    public final String key;
    public final String extra;
    public final String comment;
    public final String defaultValue;

    /**
     * Create a field or a column of a table.
     * @param field Name of the field or column.
     * @param type Type of the field or column.
     * @param isNull Can be null or not
     * @param key Key name
     * @param extra Extra attributes
     * @param comment Comment for the field
     * @param defaultValue Default value of the field
     */
    @JsonCreator
    public Field(@JsonProperty("Field") String field,
                 @JsonProperty("Type") String type,
                 @JsonProperty("Null") String isNull,
                 @JsonProperty("Key") String key,
                 @JsonProperty("Extra") String extra,
                 @JsonProperty("Comment") String comment,
                 @JsonProperty("Default") String defaultValue) {
      this.field = field;
      this.type = type;
      this.isNull = isNull;
      this.key = key;
      this.extra = extra;
      this.comment = comment;
      this.defaultValue = defaultValue;
    }
  }

  public static class Key {
    public final String table;
    public final String nonUnique;
    public final String keyName;
    public final String seqInIndex;
    public final String columnName;
    public final String collation;
    public final String cardinality;
    public final String isNull;
    public final String indexType;
    public final String comment;
    public final String indexComment;
    public final String subPart;

    /**
     * Key or index on a table.
     * @param table Name of the Key. PRIMARY for a primary key
     * @param nonUnique Not unique or unique
     * @param keyName Name of the key.
     * @param seqInIndex Sequence among other keys.
     * @param columnName Name of the column on key
     * @param collation Collation of the key
     * @param cardinality Cardinality of the key
     * @param isNull Can value be null
     * @param indexType Type of index
     * @param comment Comment on the key
     * @param indexComment Comment related to index
     * @param subPart Unknown field
     */
    @JsonCreator
    public Key(@JsonProperty("Table") String table,
               @JsonProperty("Non_unique") String nonUnique,
               @JsonProperty("Key_name") String keyName,
               @JsonProperty("Seq_in_index") String seqInIndex,
               @JsonProperty("Column_name") String columnName,
               @JsonProperty("Collation") String collation,
               @JsonProperty("Cardinality") String cardinality,
               @JsonProperty("Null") String isNull,
               @JsonProperty("Index_type") String indexType,
               @JsonProperty("Comment") String comment,
               @JsonProperty("Index_comment") String indexComment,
               @JsonProperty("Sub_part") String subPart) {
      this.table = table;
      this.nonUnique = nonUnique;
      this.keyName = keyName;
      this.seqInIndex = seqInIndex;
      this.columnName = columnName;
      this.collation = collation;
      this.cardinality = cardinality;
      this.isNull = isNull;
      this.indexType = indexType;
      this.comment = comment;
      this.indexComment = indexComment;
      this.subPart = subPart;
    }
  }

  public static class Options {
    public final String name;
    public final String engine;
    public final String version;
    public final String rowFormat;
    public final Long rows;
    public final String avgRowLength;
    public final String dataLength;
    public final String maxDataLength;
    public final String indexLength;
    public final String dataFree;
    public final String autoIncrement;
    public final String createTime;
    public final String collation;
    public final String createOptions;
    public final String comment;

    /**
     * Options of a table.
     * @param name Name of the option
     * @param engine Engine type of table
     * @param version Version of the table
     * @param rowFormat Rowformat of the table
     * @param rows No. of rows in the table
     * @param avgRowLength Avg Row Length of the table
     * @param dataLength Data length of the table
     * @param maxDataLength Max data length allowed
     * @param indexLength Length of index
     * @param dataFree Unknown
     * @param autoIncrement Current auto increment column
     * @param createTime Create time of the table.
     * @param collation Collation of the table
     * @param createOptions Options during create
     * @param comment Any comments
     */
    @JsonCreator
    public Options(@JsonProperty("Name") String name,
                   @JsonProperty("Engine") String engine,
                   @JsonProperty("Version") String version,
                   @JsonProperty("Row_format") String rowFormat,
                   @JsonProperty("Rows") Long rows,
                   @JsonProperty("Avg_row_length") String avgRowLength,
                   @JsonProperty("Data_length") String dataLength,
                   @JsonProperty("Max_data_length") String maxDataLength,
                   @JsonProperty("Index_length") String indexLength,
                   @JsonProperty("Data_free") String dataFree,
                   @JsonProperty("Auto_increment") String autoIncrement,
                   @JsonProperty("Create_time") String createTime,
                   @JsonProperty("Collation") String collation,
                   @JsonProperty("Create_options") String createOptions,
                   @JsonProperty("Comment") String comment) {
      this.name = name;
      this.engine = engine;
      this.version = version;
      this.rowFormat = rowFormat;
      this.rows = rows;
      this.avgRowLength = avgRowLength;
      this.dataLength = dataLength;
      this.maxDataLength = maxDataLength;
      this.indexLength = indexLength;
      this.dataFree = dataFree;
      this.autoIncrement = autoIncrement;
      this.createTime = createTime;
      this.collation = collation;
      this.createOptions = createOptions;
      this.comment = comment;
    }
  }

  public static class TableStructure {
    public final String name;
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "field")
    public final List<Field> fieldList;
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "key")
    public final List<Key> keys;
    public final Options options;

    /**
     * A MySql Table.
     * @param name Name of the table
     * @param fieldList List of fields or columns
     * @param keys List of keys or indexes
     * @param options Options of the table
     */
    @JsonCreator
    public TableStructure(@JsonProperty("name") String name,
                          @JsonProperty("field") List<Field> fieldList,
                          @JsonProperty("key") List<Key> keys,
                          @JsonProperty("options") Options options) {
      this.name = name;
      this.fieldList = fieldList;
      this.keys = keys;
      this.options = options;
    }
  }

  public static class Database {
    public final String name;
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "table_structure")
    public final List<TableStructure> tables;

    /**
     * A MySql database.
     * @param name Name of the database
     * @param tables List of tables in the database
     */
    @JsonCreator
    public Database(@JsonProperty("name") String name,
                    @JsonProperty("table_structure") List<TableStructure> tables) {
      this.name = name;
      this.tables = tables;
    }
  }

  public static class MySqlDump {
    public final Database database;

    /**
     * Handles mysqldump tag.
     * @param database The MySql Database
     */
    @JsonCreator
    public MySqlDump(@JsonProperty("database") Database database) {
      this.database = database;
    }
  }

  /**
   * Parse an XML file from a MySql Dump command.
   * @param is InputStream of the XML file
   * @return {@link Database} for the MySql database
   * @throws XMLStreamException An exception if XML is malformed
   * @throws IOException An exception if inputstream cannot be processed successfully.
   */
  public static Database parseMySqlDump(InputStream is) throws XMLStreamException, IOException {
    XmlMapper xmlMapper = new XmlMapper();
    XMLInputFactory factory = XMLInputFactory.newInstance();
    return xmlMapper.readValue(factory.createXMLStreamReader(is), MySqlDump.class).database;
  }

  /**
   * Parses database XML from a MySql dump command.
   * @param is InputStream of the XML source
   * @return {@link Database} for the MySql database
   * @throws XMLStreamException An exception if XML is malformed
   * @throws IOException An exception if inputstream cannot be processed successfully.
   */
  public static Database parseDatabase(InputStream is) throws XMLStreamException, IOException {
    XmlMapper xmlMapper = new XmlMapper();
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    return xmlMapper.readValue(xmlInputFactory.createXMLStreamReader(is), Database.class);
  }

  static Field parseField(InputStream is) throws XMLStreamException, IOException {
    XmlMapper xmlMapper = new XmlMapper();
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    return xmlMapper.readValue(xmlInputFactory.createXMLStreamReader(is), Field.class);
  }

  static Key parseKey(InputStream is) throws XMLStreamException, IOException {
    XmlMapper xmlMapper = new XmlMapper();
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    return xmlMapper.readValue(xmlInputFactory.createXMLStreamReader(is), Key.class);
  }

  static Options parseOptions(InputStream is) throws XMLStreamException, IOException {
    XmlMapper xmlMapper = new XmlMapper();
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    return xmlMapper.readValue(xmlInputFactory.createXMLStreamReader(is), Options.class);
  }

  static TableStructure parseTableStructure(InputStream is) throws XMLStreamException, IOException {
    XmlMapper xmlMapper = new XmlMapper();
    XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();
    return xmlMapper.readValue(xmlInputFactory.createXMLStreamReader(is), TableStructure.class);
  }
}
