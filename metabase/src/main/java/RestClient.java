import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.util.Arrays;
import java.util.List;

public class RestClient {
  final String restUri;
  final Client client;

  RestClient(String restUri) {
    this.restUri = restUri;
    client = ClientBuilder.newClient();
  }

  public List<Database> getDatabases() {
    return Arrays.asList(
        client
            .target(restUri + "/api/database")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .get(Database[].class)
    );
  }
}
