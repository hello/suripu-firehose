package com.hello.suripu.firehose.workers.sense;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.google.common.collect.ImmutableMap;
import com.hello.suripu.core.ObjectGraphRoot;
import com.hello.suripu.core.configuration.DynamoDBTableName;
import com.hello.suripu.core.configuration.QueueName;
import com.hello.suripu.core.db.FeatureStore;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.coredropwizard.clients.AmazonDynamoDBClientFactory;
import com.hello.suripu.firehose.framework.ConfigurationUtil;
import com.hello.suripu.firehose.framework.WorkerConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jakepiccolo on 11/30/15.
 */
public class SenseCommand extends ConfiguredCommand<WorkerConfiguration> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SenseCommand.class);

    public SenseCommand(String name, String description) {
        super(name, description);
    }

    private void setupRolloutModule(final AmazonDynamoDBClientFactory amazonDynamoDBClientFactory, final WorkerConfiguration configuration) {
        final AmazonDynamoDB featureDynamoDB = amazonDynamoDBClientFactory.getForTable(DynamoDBTableName.FEATURES);
        final String featureNamespace = (configuration.getDebug()) ? "dev" : "prod";
        final FeatureStore featureStore = new FeatureStore(
                featureDynamoDB,
                configuration.dynamoDBConfiguration().tables().get(DynamoDBTableName.FEATURES),
                featureNamespace);
        final SenseFirehoseRolloutModule rolloutModule = new SenseFirehoseRolloutModule(featureStore, 30);
        ObjectGraphRoot.getInstance().init(rolloutModule);
    }

    @Override
    public void run(Bootstrap<WorkerConfiguration> bootstrap, Namespace namespace, WorkerConfiguration configuration) throws Exception {
        final Environment environment = new Environment(bootstrap.getApplication().getName(),
                bootstrap.getObjectMapper(),
                bootstrap.getValidatorFactory().getValidator(),
                bootstrap.getMetricRegistry(),
                bootstrap.getClassLoader());

        configuration.getMetricsFactory().configure(environment.lifecycle(), bootstrap.getMetricRegistry());
        bootstrap.run(configuration, environment);

        if(configuration.getMetricsEnabled()) {
            ConfigurationUtil.setupGraphite(environment, configuration, "firehose");
            LOGGER.info("Metrics enabled.");
        } else {
            LOGGER.warn("Metrics not enabled.");
        }

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final KinesisClientLibConfiguration kinesisClientLibConfiguration = ConfigurationUtil.getKclConfig(configuration, awsCredentialsProvider, QueueName.SENSE_SENSORS_DATA);

        final AmazonDynamoDBClientFactory amazonDynamoDBClientFactory = AmazonDynamoDBClientFactory.create(awsCredentialsProvider, configuration.dynamoDBConfiguration());

        setupRolloutModule(amazonDynamoDBClientFactory, configuration);

        // Set up merged user info DDB
        final ImmutableMap<DynamoDBTableName, String> tableNames = configuration.dynamoDBConfiguration().tables();
        final AmazonDynamoDB alarmInfoDynamoDBClient = amazonDynamoDBClientFactory.getForTable(DynamoDBTableName.ALARM_INFO);
        final MergedUserInfoDynamoDB mergedUserInfoDynamoDB = new MergedUserInfoDynamoDB(alarmInfoDynamoDBClient , tableNames.get(DynamoDBTableName.ALARM_INFO));

        final ClientConfiguration clientConfiguration = new ClientConfiguration().withConnectionTimeout(200).withMaxErrorRetry(1);
        final AmazonKinesisFirehose firehose = new AmazonKinesisFirehoseClient(awsCredentialsProvider, clientConfiguration);
        firehose.setRegion(Region.getRegion(Regions.fromName(configuration.getFirehoseRegion())));
        final DeviceDataDAOFirehose firehoseDAO = new DeviceDataDAOFirehose(configuration.getFirehoseStream(), firehose);

        LOGGER.info("Using firehose stream: {}", firehoseDAO.describeStream().toString());

        final IRecordProcessorFactory factory = new SenseDataRecordProcessorFactory(mergedUserInfoDynamoDB, firehoseDAO, configuration.getMaxRecords(), environment.metrics());

        final Worker worker = new Worker(factory, kinesisClientLibConfiguration);
        worker.run();
    }
}
