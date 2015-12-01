package com.hello.suripu.firehose.workers.sense;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.collect.ImmutableMap;
import com.hello.suripu.core.ObjectGraphRoot;
import com.hello.suripu.core.configuration.DynamoDBTableName;
import com.hello.suripu.core.configuration.QueueName;
import com.hello.suripu.core.db.FeatureStore;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.coredw8.clients.AmazonDynamoDBClientFactory;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jakepiccolo on 11/30/15.
 */
public class SenseCommand extends ConfiguredCommand<SenseConfiguration> {

    private final static Logger LOGGER = LoggerFactory.getLogger(SenseCommand.class);

    public SenseCommand(String name, String description) {
        super(name, description);
    }

    private void setupGraphite(final Environment environment, final SenseConfiguration configuration) {
        final String graphiteHostName = configuration.getGraphite().getHost();
        final String apiKey = configuration.getGraphite().getApiKey();
        final Integer interval = configuration.getGraphite().getReportingIntervalInSeconds();
        final List<String> includeMetrics = configuration.getGraphite().getIncludeMetrics();

        final String env = (configuration.getDebug()) ? "dev" : "prod";
        final String prefix = String.format("%s.%s.firehose", apiKey, env);

        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHostName, 2003));

        final GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(environment.metrics())
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(new MetricFilter() {
                    public boolean matches(String name, Metric metric) {
                        for (final String includeMetric : includeMetrics) {
                            if (name.startsWith(includeMetric)){
                                return true;
                            }
                        }
                        return false;
                    }
                })
                .build(graphite);
        graphiteReporter.start(interval, TimeUnit.SECONDS);
    }

    private KinesisClientLibConfiguration getKclConfig(final SenseConfiguration configuration, final AWSCredentialsProvider provider) throws UnknownHostException {
        final ImmutableMap<QueueName, String> queueNames = configuration.getQueues();

        LOGGER.debug("{}", queueNames);
        final String queueName = queueNames.get(QueueName.SENSE_SENSORS_DATA);

        final String workerId = InetAddress.getLocalHost().getCanonicalHostName();
        final KinesisClientLibConfiguration kinesisConfig = new KinesisClientLibConfiguration(
                configuration.getAppName(),
                queueName,
                provider,
                workerId);

        kinesisConfig.withInitialPositionInStream(InitialPositionInStream.LATEST);
        kinesisConfig.withMaxRecords(configuration.getMaxRecords());
        kinesisConfig.withKinesisEndpoint(configuration.getKinesisEndpoint());

        if(configuration.getTrimHorizon()) {
            kinesisConfig.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        } else {
            kinesisConfig.withInitialPositionInStream(InitialPositionInStream.LATEST);
        }

        return kinesisConfig;
    }

    private void setupRolloutModule(final AmazonDynamoDBClientFactory amazonDynamoDBClientFactory, final SenseConfiguration configuration) {
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
    public void run(Bootstrap<SenseConfiguration> bootstrap, Namespace namespace, SenseConfiguration configuration) throws Exception {
        final Environment environment = new Environment(bootstrap.getApplication().getName(),
                bootstrap.getObjectMapper(),
                bootstrap.getValidatorFactory().getValidator(),
                bootstrap.getMetricRegistry(),
                bootstrap.getClassLoader());
        configuration.getMetricsFactory().configure(environment.lifecycle(),
                bootstrap.getMetricRegistry());
        bootstrap.run(configuration, environment);

        if(configuration.getMetricsEnabled()) {
            setupGraphite(environment, configuration);
            LOGGER.info("Metrics enabled.");
        } else {
            LOGGER.warn("Metrics not enabled.");
        }

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final KinesisClientLibConfiguration kinesisClientLibConfiguration = getKclConfig(configuration, awsCredentialsProvider);

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

        final IRecordProcessorFactory factory = new SenseDataRecordProcessorFactory(mergedUserInfoDynamoDB, firehoseDAO, configuration.getMaxRecords(), environment.metrics());

        final Worker worker = new Worker(factory, kinesisClientLibConfiguration);
        worker.run();
    }
}
