package com.hello.suripu.firehose.framework;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.hello.suripu.coredw8.configuration.KinesisConfiguration;
import com.hello.suripu.coredw8.configuration.NewDynamoDBConfiguration;
import com.hello.suripu.core.configuration.QueueName;
import com.hello.suripu.coredw8.configuration.GraphiteConfiguration;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.NotNull;

/**
 * Created by jakepiccolo on 11/30/15.
 */
public class WorkerConfiguration extends Configuration {
    @Valid
    @NotNull
    @JsonProperty("metrics_enabled")
    private Boolean metricsEnabled;
    public Boolean getMetricsEnabled() {
        return metricsEnabled;
    }

    @Valid
    @NotNull
    @JsonProperty("graphite")
    private GraphiteConfiguration graphite;
    public GraphiteConfiguration getGraphite() {
        return graphite;
    }

    @Valid
    @NotNull
    @JsonProperty("app_name")
    private String appName;
    public String getAppName() {
        return appName;
    }

    @Valid
    @NotNull
    @JsonProperty("kinesis")
    private KinesisConfiguration kinesisConfiguration;
    public String getKinesisEndpoint() {
        return kinesisConfiguration.getEndpoint();
    }
    public ImmutableMap<QueueName,String> getQueues() {
        return ImmutableMap.copyOf(kinesisConfiguration.getStreams());
    }


    @Valid
    @NotNull
    @JsonProperty("firehose")
    private FirehoseConfiguration firehoseConfiguration;
    public String getFirehoseRegion() { return firehoseConfiguration.getRegion(); }
    public String getFirehoseStream() { return firehoseConfiguration.getStream(); }


    @Valid
    @JsonProperty("debug")
    private Boolean debug = Boolean.FALSE;
    public Boolean getDebug() { return debug; }

    @Valid
    @JsonProperty("dynamodb")
    private NewDynamoDBConfiguration dynamoDBConfiguration;
    public NewDynamoDBConfiguration dynamoDBConfiguration(){
        return dynamoDBConfiguration;
    }

    @Valid
    @NotNull
    @Max(1000)
    @JsonProperty("max_records")
    private Integer maxRecords;
    public Integer getMaxRecords() {
        return maxRecords;
    }

    @JsonProperty("trim_horizon")
    private Boolean trimHorizon = Boolean.TRUE;
    public Boolean getTrimHorizon() {return trimHorizon;}
}
