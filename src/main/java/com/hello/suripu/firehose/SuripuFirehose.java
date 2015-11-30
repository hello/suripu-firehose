package com.hello.suripu.firehose;

import com.hello.suripu.firehose.framework.WorkerConfiguration;
import com.hello.suripu.firehose.workers.sense.SenseCommand;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.joda.time.DateTimeZone;

import java.util.TimeZone;

/**
 * Created by jakepiccolo on 11/30/15.
 */
public class SuripuFirehose extends Application<WorkerConfiguration> {

    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        DateTimeZone.setDefault(DateTimeZone.UTC);
        new SuripuFirehose().run(args);
    }

    @Override
    public void initialize(Bootstrap<WorkerConfiguration> bootstrap) {
        bootstrap.addCommand(new SenseCommand("sense", "save sense data to firehose"));
    }

    @Override
    public void run(WorkerConfiguration configuration, Environment environment) throws Exception {
        // Nada
    }
}
