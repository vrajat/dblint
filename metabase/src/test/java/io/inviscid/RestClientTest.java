package io.inviscid;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.WireMockServer;

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RestClientTest {

  String jsonString = "{\n"
      + "        \"description\": null,\n"
      + "        \"features\": [\n"
      + "            \"basic-aggregations\",\n"
      + "            \"standard-deviation-aggregations\",\n"
      + "            \"expression-aggregations\",\n"
      + "            \"foreign-keys\",\n"
      + "            \"native-parameters\",\n"
      + "            \"nested-queries\",\n"
      + "            \"expressions\",\n"
      + "            \"set-timezone\",\n"
      + "            \"binning\"\n"
      + "        ],\n"
      + "        \"cache_field_values_schedule\": \"0 50 0 * * ? *\",\n"
      + "        \"timezone\": \"UTC\",\n"
      + "        \"metadata_sync_schedule\": \"0 50 * * * ? *\",\n"
      + "        \"name\": \"superdb\",\n"
      + "        \"caveats\": null,\n"
      + "        \"is_full_sync\": true,\n"
      + "        \"updated_at\": \"2018-08-22T07:00:42.833Z\",\n"
      + "        \"native_permissions\": \"write\",\n"
      + "        \"details\": {\n"
      + "            \"host\": \"10.1.0.14\",\n"
      + "            \"port\": 3306,\n"
      + "            \"dbname\": \"superdb\",\n"
      + "            \"user\": \"superuser\",\n"
      + "            \"password\": \"**MetabasePass**\",\n"
      + "            \"tunnel-port\": 22,\n"
      + "            \"ssl\": true\n"
      + "        },\n"
      + "        \"is_sample\": false,\n"
      + "        \"id\": 2,\n"
      + "        \"is_on_demand\": false,\n"
      + "        \"engine\": \"mysql\",\n"
      + "        \"created_at\": \"2018-08-22T07:00:42.426Z\",\n"
      + "        \"points_of_interest\": null\n"
      + "    }";

  private static WireMockServer wireMockServer;

  @BeforeAll
  static void setUp() {
    wireMockServer = new WireMockServer();
    wireMockServer.start();
  }

  @AfterAll
  static void tearDown() {
    if (wireMockServer != null) {
      wireMockServer.stop();
    }
  }

  @Test
  void sanityTest() {
    stubFor(
        get("/api/database")
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("[" + jsonString + "," + jsonString + "]")));

    RestClient restClient = new RestClient("http://localhost:8080");
    List<Database> databases = restClient.getDatabases();
    assertEquals(2, databases.size());
  }
}