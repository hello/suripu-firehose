package com.hello.suripu.firehose.workers.sense;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.google.common.collect.ImmutableList;
import com.hello.suripu.core.models.DeviceData;
import com.hello.suripu.firehose.framework.WorkerConfiguration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Created by jakepiccolo on 11/30/15.
 */
public class TestSenseFirehoseCommand extends ConfiguredCommand<WorkerConfiguration> {

    public TestSenseFirehoseCommand(String name, String description) {
        super(name, description);
    }

    @Override
    public void run(Bootstrap<WorkerConfiguration> bootstrap, Namespace namespace, WorkerConfiguration configuration) throws Exception {
        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final ClientConfiguration clientConfiguration = new ClientConfiguration().withConnectionTimeout(200).withMaxErrorRetry(1);
        final AmazonKinesisFirehose firehose = new AmazonKinesisFirehoseClient(awsCredentialsProvider, clientConfiguration);
        firehose.setRegion(Region.getRegion(Regions.fromName(configuration.getFirehoseRegion())));
        final DeviceDataDAOFirehose firehoseDAO = new DeviceDataDAOFirehose(configuration.getFirehoseStream(), firehose);

        final DateTime dateTime = DateTime.now().minusMinutes(5);
        final List<DeviceData> dataList = ImmutableList.of(
                new DeviceData.Builder()
                        .withAccountId(1L)
                        .withDeviceId(2L)
                        .withDateTimeUTC(dateTime)
                        .withOffsetMillis(0)
                        .build(),
                new DeviceData.Builder()
                        .withAccountId(3L)
                        .withDeviceId(4L)
                        .withDateTimeUTC(dateTime.plusMinutes(1))
                        .withOffsetMillis(0)
                        .build()
        );
        final int result = firehoseDAO.batchInsertAll(dataList);
        System.out.println(result);
    }
}
