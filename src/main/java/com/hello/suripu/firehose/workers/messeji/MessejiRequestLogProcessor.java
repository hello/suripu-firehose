package com.hello.suripu.firehose.workers.messeji;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesisfirehose.model.InvalidArgumentException;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.messeji.api.Logging;
import com.hello.suripu.firehose.FirehoseDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by jakepiccolo on 4/11/16.
 */
public class MessejiRequestLogProcessor implements IRecordProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(MessejiRequestLogProcessor.class);

    private final FirehoseDAO firehoseDAO;
    private String shardId = "";

    private final Meter recordsProcessed;
    private final Meter batchSaved;
    private final Meter batchSaveFailures;

    protected MessejiRequestLogProcessor(final FirehoseDAO firehoseDAO, final Meter recordsProcessed,
                                         final Meter batchSaved, final Meter batchSaveFailures) {
        this.firehoseDAO = firehoseDAO;
        this.recordsProcessed = recordsProcessed;
        this.batchSaved = batchSaved;
        this.batchSaveFailures = batchSaveFailures;
    }

    public static MessejiRequestLogProcessor create(final FirehoseDAO firehoseDAO, final MetricRegistry metricRegistry) {
        final Meter recordsProcessed = metricRegistry.meter(MetricRegistry.name(MessejiRequestLogProcessor.class, "records", "records-processed"));
        final Meter batchSaved = metricRegistry.meter(MetricRegistry.name(MessejiRequestLogProcessor.class, "records", "batch-saved"));
        final Meter batchSaveFailures = metricRegistry.meter(MetricRegistry.name(MessejiRequestLogProcessor.class, "records", "batch-save-failures"));
        return new MessejiRequestLogProcessor(firehoseDAO, recordsProcessed, batchSaved, batchSaveFailures);
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
            LOGGER.info("checkpoint=success");
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint=failure error=InvalidStateException exception={}", e);
        } catch (ShutdownException e) {
            LOGGER.error("checkpoint=failure error=ShutdownException exception={}", e);
        }
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        final List<Logging.RequestLog> requestLogs = Lists.newArrayListWithExpectedSize(records.size());
        for (final Record record : records) {
            final byte[] bytes = record.getData().array();
            try {
                requestLogs.add(Logging.RequestLog.parseFrom(bytes));
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error("error=InvalidProtocolBufferException partition-key={} sequence-number={} record-size={}",
                        record.getPartitionKey(), record.getSequenceNumber(), bytes.length);
            }
        }

        final List<com.amazonaws.services.kinesisfirehose.model.Record> recordsToInsert = MessejiRequestFirehoseUtil.toRecords(requestLogs);

        try {
            final List<com.amazonaws.services.kinesisfirehose.model.Record> failedRecords = firehoseDAO.batchInsertAllRecords(recordsToInsert);
            checkpoint(checkpointer);
            recordsProcessed.mark(records.size());
            batchSaved.mark(recordsToInsert.size() - failedRecords.size());
            batchSaveFailures.mark(failedRecords.size());
        } catch (InvalidArgumentException iae) {
            LOGGER.error("error=InvalidArgumentException exception={}", iae);
            checkpoint(checkpointer);
        } catch (ResourceNotFoundException rnfe) {
            // Do not checkpoint
            LOGGER.error("error=ResourceNotFoundException exception={}", rnfe);
        } catch (Exception e) {
            checkpoint(checkpointer);
            LOGGER.error("error=Exception exception={}", e);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOGGER.warn("shutdown-reason={}", reason.toString());
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }
}
