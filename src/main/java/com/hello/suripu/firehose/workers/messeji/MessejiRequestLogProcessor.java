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
import com.google.common.base.Optional;
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

    private Integer maxRecords;
    private final Meter recordsProcessed;
    private final Meter batchSaved;
    private final Meter batchSaveFailures;

    protected MessejiRequestLogProcessor(final FirehoseDAO firehoseDAO, final Integer maxRecords, final Meter recordsProcessed,
                                         final Meter batchSaved, final Meter batchSaveFailures) {
        this.firehoseDAO = firehoseDAO;
        this.maxRecords = maxRecords;
        this.recordsProcessed = recordsProcessed;
        this.batchSaved = batchSaved;
        this.batchSaveFailures = batchSaveFailures;
    }

    public static MessejiRequestLogProcessor create(final FirehoseDAO firehoseDAO, final Integer maxRecords, final MetricRegistry metricRegistry) {
        final Meter recordsProcessed = metricRegistry.meter(MetricRegistry.name(MessejiRequestLogProcessor.class, "records", "records-processed"));
        final Meter batchSaved = metricRegistry.meter(MetricRegistry.name(MessejiRequestLogProcessor.class, "records", "batch-saved"));
        final Meter batchSaveFailures = metricRegistry.meter(MetricRegistry.name(MessejiRequestLogProcessor.class, "records", "batch-save-failures"));
        return new MessejiRequestLogProcessor(firehoseDAO, maxRecords, recordsProcessed, batchSaved, batchSaveFailures);
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer, final Optional<String> sequenceNumber) {
        final String sequenceNumberString = sequenceNumber.isPresent() ? sequenceNumber.get() : "";
        try {
            if (sequenceNumber.isPresent()) {
                checkpointer.checkpoint(sequenceNumber.get());
            } else {
                checkpointer.checkpoint();
            }
            LOGGER.info("checkpoint=success sequence-number={}", sequenceNumberString);
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint=failure sequence-number={} error=InvalidStateException exception={}", sequenceNumberString, e);
        } catch (ShutdownException e) {
            LOGGER.error("checkpoint=failure sequence-number={} error=ShutdownException exception={}", sequenceNumberString, e);
        }
    }

    @Override
    public void processRecords(final List<Record> records, final IRecordProcessorCheckpointer checkpointer) {
        final Optional<String> lastSequenceNumber = records.isEmpty()
                ? Optional.absent()
                : Optional.of(records.get(records.size()-1).getSequenceNumber());

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
            checkpoint(checkpointer, lastSequenceNumber);
            recordsProcessed.mark(records.size());
            batchSaved.mark(recordsToInsert.size() - failedRecords.size());
            batchSaveFailures.mark(failedRecords.size());
        } catch (InvalidArgumentException iae) {
            LOGGER.error("error=InvalidArgumentException exception={}", iae);
            checkpoint(checkpointer, lastSequenceNumber);
        } catch (ResourceNotFoundException rnfe) {
            // Do not checkpoint
            LOGGER.error("error=ResourceNotFoundException exception={}", rnfe);
        } catch (Exception e) {
            checkpoint(checkpointer, lastSequenceNumber);
            LOGGER.error("error=Exception exception={}", e);
        }

        final int batchCapacity = Math.round(records.size() / (float) maxRecords * 100.0f);
        LOGGER.info("shard={} batch_capacity={}%", shardId, batchCapacity);
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOGGER.warn("shutdown-reason={}", reason.toString());
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer, Optional.absent());
        }
    }
}
