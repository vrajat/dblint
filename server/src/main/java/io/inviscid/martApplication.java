package io.inviscid;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class martApplication extends Application<martConfiguration> {

    public static void main(final String[] args) throws Exception {
        new martApplication().run(args);
    }

    @Override
    public String getName() {
        return "mart";
    }

    @Override
    public void initialize(final Bootstrap<martConfiguration> bootstrap) {
        // TODO: application initialization
    }

    @Override
    public void run(final martConfiguration configuration,
                    final Environment environment) {
        // TODO: implement application
    }

}
