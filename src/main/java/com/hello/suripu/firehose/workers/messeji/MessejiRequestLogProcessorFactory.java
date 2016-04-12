package com.hello.suripu.firehose.workers.messeji;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.codahale.metrics.MetricRegistry;
import com.hello.suripu.firehose.FirehoseDAO;

/**
 * Created by jakepiccolo on 4/12/16.
 */
public class MessejiRequestLogProcessorFactory implements IRecordProcessorFactory {
    private final FirehoseDAO firehoseDAO;
    private final MetricRegistry metrics;

    public MessejiRequestLogProcessorFactory(final FirehoseDAO firehoseDAO, final MetricRegistry metrics) {
        this.firehoseDAO = firehoseDAO;
        this.metrics = metrics;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return MessejiRequestLogProcessor.create(firehoseDAO, metrics);
    }
}
