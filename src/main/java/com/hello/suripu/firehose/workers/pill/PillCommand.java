package com.hello.suripu.firehose.workers.pill;

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
import com.hello.suripu.core.configuration.DynamoDBTableName;
import com.hello.suripu.core.configuration.QueueName;
import com.hello.suripu.core.db.DeviceDAO;
import com.hello.suripu.core.db.KeyStore;
import com.hello.suripu.core.db.KeyStoreDynamoDB;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.core.db.util.JodaArgumentFactory;
import com.hello.suripu.coredw8.clients.AmazonDynamoDBClientFactory;
import com.hello.suripu.firehose.framework.ConfigurationUtil;
import com.hello.suripu.firehose.framework.WorkerConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.jdbi.DBIFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.skife.jdbi.v2.DBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ksg via jakey on 05/02/16
 */
public class PillCommand extends ConfiguredCommand<WorkerConfiguration> {

    private final static Logger LOGGER = LoggerFactory.getLogger(PillCommand.class);

    public PillCommand(String name, String description) {
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
        final DBIFactory dbiFactory = new DBIFactory();
        final DBI commonDBI = dbiFactory.build(environment, configuration.getCommonDB(), "postgresql-common");
        commonDBI.registerArgumentFactory(new JodaArgumentFactory());
        final DeviceDAO deviceDAO = commonDBI.onDemand(DeviceDAO.class);

        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final KinesisClientLibConfiguration kinesisClientLibConfiguration = ConfigurationUtil.getKclConfig(configuration, awsCredentialsProvider, QueueName.BATCH_PILL_DATA);

        final ClientConfiguration clientConfiguration = new ClientConfiguration().withConnectionTimeout(200).withMaxErrorRetry(1);
        final AmazonKinesisFirehose firehose = new AmazonKinesisFirehoseClient(awsCredentialsProvider, clientConfiguration);
        firehose.setRegion(Region.getRegion(Regions.fromName(configuration.getFirehoseRegion())));
        final PillDataDAOFirehose firehoseDAO = new PillDataDAOFirehose(configuration.getFirehoseStream(), firehose);

        // Set up merged user info and keystore
        final AmazonDynamoDBClientFactory amazonDynamoDBClientFactory = AmazonDynamoDBClientFactory.create(awsCredentialsProvider, configuration.dynamoDBConfiguration());
        final ImmutableMap<DynamoDBTableName, String> tableNames = configuration.dynamoDBConfiguration().tables();
        final AmazonDynamoDB alarmInfoDynamoDBClient = amazonDynamoDBClientFactory.getForTable(DynamoDBTableName.ALARM_INFO);
        final MergedUserInfoDynamoDB mergedUserInfoDynamoDB = new MergedUserInfoDynamoDB(alarmInfoDynamoDBClient , tableNames.get(DynamoDBTableName.ALARM_INFO));

        final AmazonDynamoDB pillKeyStoreDynamoDB = amazonDynamoDBClientFactory.getForTable(DynamoDBTableName.PILL_KEY_STORE);
        final KeyStore pillKeyStore = new KeyStoreDynamoDB(pillKeyStoreDynamoDB,tableNames.get(DynamoDBTableName.PILL_KEY_STORE), new byte[16], 120);

        LOGGER.info("Using firehose stream: {}", firehoseDAO.describeStream().toString());

        final IRecordProcessorFactory factory = new PillProcessorFactory(mergedUserInfoDynamoDB, pillKeyStore, deviceDAO, firehoseDAO, environment.metrics());

        final Worker worker = new Worker(factory, kinesisClientLibConfiguration);
        worker.run();
    }
}
