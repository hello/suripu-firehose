package com.hello.suripu.firehose.sense;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.codahale.metrics.MetricRegistry;
import com.hello.suripu.core.db.DeviceDataIngestDAO;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;

/**
 * Created by jakepiccolo on 11/25/15.
 */
public class SenseDataRecordProcessorFactory implements IRecordProcessorFactory {

    private final DeviceDataIngestDAO deviceDataDAO;
    private final MergedUserInfoDynamoDB mergedInfoDynamoDB;
    private final Integer maxRecords;
    private final MetricRegistry metrics;

    public SenseDataRecordProcessorFactory(final MergedUserInfoDynamoDB mergedUserInfoDynamoDB,
                                           final DeviceDataIngestDAO deviceDataDAO,
                                           final Integer maxRecords,
                                           final MetricRegistry metrics) {
        this.deviceDataDAO = deviceDataDAO;
        this.mergedInfoDynamoDB = mergedUserInfoDynamoDB;
        this.maxRecords = maxRecords;
        this.metrics = metrics;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new SenseDataRecordProcessor(mergedInfoDynamoDB, deviceDataDAO , maxRecords, metrics);
    }
}
