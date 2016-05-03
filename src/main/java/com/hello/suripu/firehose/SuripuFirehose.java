package com.hello.suripu.firehose;

import com.hello.suripu.firehose.framework.WorkerConfiguration;
import com.hello.suripu.firehose.workers.messeji.MessejiCommand;
import com.hello.suripu.firehose.workers.pill.PillCommand;
import com.hello.suripu.firehose.workers.sense.SenseCommand;
import com.hello.suripu.firehose.workers.sense.TestSenseFirehoseCommand;
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
        bootstrap.addCommand(new MessejiCommand("messeji", "save messeji request logs to firehose"));
        bootstrap.addCommand(new TestSenseFirehoseCommand("test_sense_firehose", "send test sense data to firehose"));
        bootstrap.addCommand(new PillCommand("pill", "save pill data to firehose"));
    }

    @Override
    public void run(WorkerConfiguration configuration, Environment environment) throws Exception {
        // Nada
    }
}
