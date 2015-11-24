package com.hello.suripu.firehose.sense;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.hello.suripu.core.db.DeviceDataIngestDAO;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.core.util.SenseProcessorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by jakepiccolo on 11/23/15.
 */
public class SenseDataRecordProcessor implements IRecordProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(SenseDataRecordProcessor.class);
    private final DeviceDataIngestDAO deviceDataDAO;
    private final MergedUserInfoDynamoDB mergedInfoDynamoDB;
    private final Integer maxRecords;

    private String shardId = "";


    public SenseDataRecordProcessor(final MergedUserInfoDynamoDB mergedInfoDynamoDB, final DeviceDataIngestDAO deviceDataDAO, final Integer maxRecords) {
        this.mergedInfoDynamoDB = mergedInfoDynamoDB;
        this.deviceDataDAO = deviceDataDAO;
        this.maxRecords = maxRecords;
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        // TODO
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        // TODO
    }
}
