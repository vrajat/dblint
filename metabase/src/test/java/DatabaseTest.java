import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {
  String jsonString = "{\n" +
      "        \"description\": null,\n" +
      "        \"features\": [\n" +
      "            \"basic-aggregations\",\n" +
      "            \"standard-deviation-aggregations\",\n" +
      "            \"expression-aggregations\",\n" +
      "            \"foreign-keys\",\n" +
      "            \"native-parameters\",\n" +
      "            \"nested-queries\",\n" +
      "            \"expressions\",\n" +
      "            \"set-timezone\",\n" +
      "            \"binning\"\n" +
      "        ],\n" +
      "        \"cache_field_values_schedule\": \"0 50 0 * * ? *\",\n" +
      "        \"timezone\": \"UTC\",\n" +
      "        \"metadata_sync_schedule\": \"0 50 * * * ? *\",\n" +
      "        \"name\": \"superdb\",\n" +
      "        \"caveats\": null,\n" +
      "        \"is_full_sync\": true,\n" +
      "        \"updated_at\": \"2018-08-22T07:00:42.833Z\",\n" +
      "        \"native_permissions\": \"write\",\n" +
      "        \"details\": {\n" +
      "            \"host\": \"10.1.0.14\",\n" +
      "            \"port\": 3306,\n" +
      "            \"dbname\": \"superdb\",\n" +
      "            \"user\": \"superuser\",\n" +
      "            \"password\": \"**MetabasePass**\",\n" +
      "            \"tunnel-port\": 22,\n" +
      "            \"ssl\": true\n" +
      "        },\n" +
      "        \"is_sample\": false,\n" +
      "        \"id\": 2,\n" +
      "        \"is_on_demand\": false,\n" +
      "        \"engine\": \"mysql\",\n" +
      "        \"created_at\": \"2018-08-22T07:00:42.426Z\",\n" +
      "        \"points_of_interest\": null\n" +
      "    }";

  @Test
  public void serializeObject() throws IOException {
    Database database = new ObjectMapper().readValue(jsonString, Database.class);
    assertEquals("superdb", database.name);
  }

  @Test
  public void serializeArray() throws IOException {
    String arrayJsonString = "[" + jsonString + "," + jsonString + "]";
    Database[] databases = new ObjectMapper().readValue(arrayJsonString, Database[].class);
    assertEquals(2, databases.length);
  }

}