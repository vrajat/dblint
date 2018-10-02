package io.inviscid.metricsink.mysql;

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
  static class Field {
    public final String field;
    public final String type;
    public final String isNull;
    public final String key;
    public final String extra;
    public final String comment;

    @JsonCreator
    public Field(@JsonProperty("Field") String field,
                 @JsonProperty("Type") String type,
                 @JsonProperty("Null") String isNull,
                 @JsonProperty("Key") String key,
                 @JsonProperty("Extra") String extra,
                 @JsonProperty("Comment") String comment) {
      this.field = field;
      this.type = type;
      this.isNull = isNull;
      this.key = key;
      this.extra = extra;
      this.comment = comment;
    }
  }

  static class Key {
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
               @JsonProperty("Index_comment") String indexComment) {
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
    }
  }

  static class Options {
    public final String name;
    public final String engine;
    public final String version;
    public final String rowFormat;
    public final String rows;
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

    @JsonCreator
    public Options(@JsonProperty("Name") String name,
                   @JsonProperty("Engine") String engine,
                   @JsonProperty("Version") String version,
                   @JsonProperty("Row_format") String rowFormat,
                   @JsonProperty("Rows") String rows,
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

  static class TableStructure {
    public final String name;
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "field")
    public final List<Field> fieldList;
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "key")
    public final List<Key> keys;
    public final Options options;

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

  static class Database {
    public final String name;
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "table_structure")
    public final List<TableStructure> tables;

    @JsonCreator
    public Database(@JsonProperty("name") String name,
                    @JsonProperty("table_structure") List<TableStructure> tables) {
      this.name = name;
      this.tables = tables;
    }
  }

  Database database;

  static Database parseDatabase(InputStream is) throws XMLStreamException, IOException {
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
