package com.hello.suripu.firehose.workers.pill;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.codahale.metrics.MetricRegistry;
import com.hello.suripu.core.db.DeviceDAO;
import com.hello.suripu.core.db.KeyStore;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.firehose.FirehoseDAO;

/**
 * Created by ksg via jakey on 05/02/16
 */
public class PillProcessorFactory implements IRecordProcessorFactory {
    private final MergedUserInfoDynamoDB mergedUserInfoDynamoDB;
    private final KeyStore pillKeyStore;
    private final DeviceDAO deviceDAO;
    private final FirehoseDAO firehoseDAO;
    private final MetricRegistry metrics;

    public PillProcessorFactory(final MergedUserInfoDynamoDB mergedUserInfoDynamoDB,
                                final KeyStore pillKeyStore,
                                final DeviceDAO deviceDAO,
                                final FirehoseDAO firehoseDAO, final MetricRegistry metrics) {
        this.mergedUserInfoDynamoDB = mergedUserInfoDynamoDB;
        this.pillKeyStore = pillKeyStore;
        this.deviceDAO = deviceDAO;
        this.firehoseDAO = firehoseDAO;
        this.metrics = metrics;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return PillProcessor.create(mergedUserInfoDynamoDB, pillKeyStore, deviceDAO, firehoseDAO, metrics);
    }
}
