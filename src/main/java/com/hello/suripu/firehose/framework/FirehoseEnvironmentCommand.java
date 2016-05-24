package com.hello.suripu.firehose.framework;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Created by ksg on 5/23/16
 */
public abstract class FirehoseEnvironmentCommand<T extends WorkerConfiguration> extends ConfiguredCommand<T> {


    protected FirehoseEnvironmentCommand(final String name, final String description) {
        super(name, description);
    }

    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        final Environment environment = new Environment(bootstrap.getApplication().getName(),
                bootstrap.getObjectMapper(),
                bootstrap.getValidatorFactory().getValidator(),
                bootstrap.getMetricRegistry(),
                bootstrap.getClassLoader());
        run(environment, namespace, configuration);
    }

    protected abstract void run(Environment environment, Namespace namespace, T configuration) throws Exception;
}
