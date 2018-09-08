package io.inviscid;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by rvenkatesh on 9/8/18.
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class IntegrationTest {
  public static final DropwizardAppExtension<MartConfiguration> EXTENSION =
      new DropwizardAppExtension<>(MartApplication.class,
          ResourceHelpers.resourceFilePath("test_config.yml"));

  @Test
  public void loginHandlerRedirectsAfterPost() {
    Response response = EXTENSION.client()
        .target(String.format("http://localhost:%d/", EXTENSION.getAdminPort()))
        .request()
        .get(Response.class);

    assertEquals(200, response.getStatus());
  }
}
