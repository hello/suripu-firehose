package com.hello.suripu.firehose.framework;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.collect.ImmutableMap;
import com.hello.suripu.core.configuration.QueueName;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jakepiccolo on 4/12/16.
 */
public class ConfigurationUtil {

    private final static Logger LOGGER = LoggerFactory.getLogger(ConfigurationUtil.class);

    public static void setupGraphite(final Environment environment, final WorkerConfiguration configuration, final String metricName) {
        final String graphiteHostName = configuration.getGraphite().getHost();
        final String apiKey = configuration.getGraphite().getApiKey();
        final Integer interval = configuration.getGraphite().getReportingIntervalInSeconds();
        final List<String> includeMetrics = configuration.getGraphite().getIncludeMetrics();

        final String env = (configuration.getDebug()) ? "dev" : "prod";
        final String prefix = String.format("%s.%s.%s", apiKey, env, metricName);

        final Graphite graphite = new Graphite(new InetSocketAddress(graphiteHostName, 2003));

        final GraphiteReporter graphiteReporter = GraphiteReporter.forRegistry(environment.metrics())
                .prefixedWith(prefix)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter((name, metric) -> {
                    for (final String includeMetric : includeMetrics) {
                        if (name.startsWith(includeMetric)){
                            return true;
                        }
                    }
                    return false;
                })
                .build(graphite);
        graphiteReporter.start(interval, TimeUnit.SECONDS);
    }

    public static KinesisClientLibConfiguration getKclConfig(final WorkerConfiguration configuration, final AWSCredentialsProvider provider, final QueueName queueNameEnum) throws UnknownHostException {
        final ImmutableMap<QueueName, String> queueNames = configuration.getQueues();

        LOGGER.debug("{}", queueNames);

        final String queueName = queueNames.get(queueNameEnum);

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
}
