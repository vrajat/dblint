package io.inviscid;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class MartApplication extends Application<MartConfiguration> {

    public static void main(final String[] args) throws Exception {
        new MartApplication().run(args);
    }

    @Override
    public String getName() {
        return "mart";
    }

    @Override
    public void initialize(final Bootstrap<MartConfiguration> bootstrap) {
        // TODO: application initialization
    }

    @Override
    public void run(final MartConfiguration configuration,
                    final Environment environment) {
        // TODO: implement application
    }

}
