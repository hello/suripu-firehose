package com.hello.suripu.firehose.workers.messeji;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.hello.suripu.core.configuration.QueueName;
import com.hello.suripu.firehose.FirehoseDAO;
import com.hello.suripu.firehose.framework.ConfigurationUtil;
import com.hello.suripu.firehose.framework.WorkerConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jakepiccolo on 4/12/16.
 */
public class MessejiCommand extends ConfiguredCommand<WorkerConfiguration> {

    private final static Logger LOGGER = LoggerFactory.getLogger(MessejiCommand.class);

    public MessejiCommand(String name, String description) {
        super(name, description);
    }

    @Override
    protected void run(Bootstrap<WorkerConfiguration> bootstrap, Namespace namespace, WorkerConfiguration configuration) throws Exception {
        final Environment environment = new Environment(bootstrap.getApplication().getName(),
                bootstrap.getObjectMapper(),
                bootstrap.getValidatorFactory().getValidator(),
                bootstrap.getMetricRegistry(),
                bootstrap.getClassLoader());
        configuration.getMetricsFactory().configure(environment.lifecycle(),
                bootstrap.getMetricRegistry());
        bootstrap.run(configuration, environment);

        if(configuration.getMetricsEnabled()) {
            ConfigurationUtil.setupGraphite(environment, configuration, "firehose");
            LOGGER.info("Metrics enabled.");
        } else {
            LOGGER.warn("Metrics not enabled.");
        }

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final KinesisClientLibConfiguration kinesisClientLibConfiguration = ConfigurationUtil.getKclConfig(configuration, awsCredentialsProvider, QueueName.SENSE_SENSORS_DATA);

        final ClientConfiguration clientConfiguration = new ClientConfiguration().withConnectionTimeout(200).withMaxErrorRetry(1);
        final AmazonKinesisFirehose firehose = new AmazonKinesisFirehoseClient(awsCredentialsProvider, clientConfiguration);
        firehose.setRegion(Region.getRegion(Regions.fromName(configuration.getFirehoseRegion())));
        final FirehoseDAO firehoseDAO = new FirehoseDAO(configuration.getFirehoseStream(), firehose);

        LOGGER.info("Using firehose stream: {}", firehoseDAO.describeStream().toString());

        final IRecordProcessorFactory factory = new MessejiRequestLogProcessorFactory(firehoseDAO, environment.metrics());

        final Worker worker = new Worker(factory, kinesisClientLibConfiguration);
        worker.run();
    }
}
