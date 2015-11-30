package com.hello.suripu.firehose.sense;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.suripu.api.input.DataInputProtos;
import com.hello.suripu.core.ObjectGraphRoot;
import com.hello.suripu.core.db.DeviceDataIngestDAO;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.core.flipper.FeatureFlipper;
import com.hello.suripu.core.models.DeviceData;
import com.hello.suripu.core.util.SenseProcessorUtils;
import com.librato.rollout.RolloutClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jakepiccolo on 11/23/15.
 */
public class SenseDataRecordProcessor implements IRecordProcessor {

    @Inject
    private RolloutClient flipper;

    private final static Logger LOGGER = LoggerFactory.getLogger(SenseDataRecordProcessor.class);
    private final DeviceDataIngestDAO deviceDataDAO;
    private final MergedUserInfoDynamoDB mergedInfoDynamoDB;
    private final Integer maxRecords;

    private String shardId = "";

    private final Meter messagesProcessed;
    private final Meter batchSaved;
    private final Meter batchSaveFailures;
    private final Meter clockOutOfSync;
    private final Timer fetchTimezones;
    private final Meter capacity;


    public SenseDataRecordProcessor(final MergedUserInfoDynamoDB mergedInfoDynamoDB,
                                    final DeviceDataIngestDAO deviceDataDAO,
                                    final Integer maxRecords,
                                    final MetricRegistry metrics) {
        this.mergedInfoDynamoDB = mergedInfoDynamoDB;
        this.deviceDataDAO = deviceDataDAO;
        this.maxRecords = maxRecords;

        ObjectGraphRoot.getInstance().inject(this);

        final Class klass = deviceDataDAO.name();
        this.messagesProcessed = metrics.meter(MetricRegistry.name(klass, "messages", "messages-processed"));
        this.batchSaved = metrics.meter(MetricRegistry.name(klass, "batch", "batch-saved"));
        this.batchSaveFailures = metrics.meter(MetricRegistry.name(klass, "batch-failure", "batch-save-failure"));
        this.clockOutOfSync = metrics.meter(MetricRegistry.name(klass, "clock", "clock-out-of-sync"));
        this.fetchTimezones = metrics.timer(MetricRegistry.name(klass, "fetch-timezones"));
        this.capacity = metrics.meter(MetricRegistry.name(klass, "capacity", "capacity"));
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        final LinkedList<DeviceData> deviceDataList = new LinkedList<>();

        for(final Record record : records) {
            DataInputProtos.BatchPeriodicDataWorker batchPeriodicDataWorker;
            try {
                batchPeriodicDataWorker = DataInputProtos.BatchPeriodicDataWorker.parseFrom(record.getData().array());
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error("Failed parsing protobuf: {}", e.getMessage());
                LOGGER.error("Moving to next record");
                continue;
            }

            final String deviceName = batchPeriodicDataWorker.getData().getDeviceId();

            final List<Long> accounts = Lists.newArrayList();
            for (final DataInputProtos.AccountMetadata metadata: batchPeriodicDataWorker.getTimezonesList()) {
                accounts.add(metadata.getAccountId());
            }

            final Map<Long, DateTimeZone> timezonesByUser;
            try (final Timer.Context context = fetchTimezones.time()) {
                timezonesByUser = SenseProcessorUtils.getTimezonesByUser(
                        deviceName, batchPeriodicDataWorker, accounts, mergedInfoDynamoDB, hasKinesisTimezonesEnabled(deviceName));
            }

            if(timezonesByUser.isEmpty()) {
                LOGGER.warn("Device {} is not stored in DynamoDB or doesn't have any accounts linked.", deviceName);
            }


            //LOGGER.info("Protobuf message {}", TextFormat.shortDebugString(batchPeriodicDataWorker));

            for(final DataInputProtos.periodic_data periodicData : batchPeriodicDataWorker.getData().getDataList()) {

                final long createdAtTimestamp = batchPeriodicDataWorker.getReceivedAt();
                final DateTime createdAtRounded = new DateTime(createdAtTimestamp, DateTimeZone.UTC);

                final DateTime periodicDataSampleDateTime = SenseProcessorUtils.getSampleTime(createdAtRounded, periodicData, attemptToRecoverSenseReportedTimeStamp(deviceName));

                if(SenseProcessorUtils.isClockOutOfSync(periodicDataSampleDateTime, createdAtRounded)) {
                    LOGGER.error("The clock for device {} is not within reasonable bounds (2h)", batchPeriodicDataWorker.getData().getDeviceId());
                    LOGGER.error("Created time = {}, sample time = {}, now = {}", createdAtRounded, periodicDataSampleDateTime, DateTime.now());
                    clockOutOfSync.mark();
                    continue;
                }

                final Integer firmwareVersion = SenseProcessorUtils.getFirmwareVersion(batchPeriodicDataWorker, periodicData);

                for (final Long accountId: accounts) {
                    if(!timezonesByUser.containsKey(accountId)) {
                        LOGGER.warn("No timezone info for account {} paired with device {}, account may already unpaired with device but merge table not updated.",
                                accountId,
                                deviceName);
                        continue;
                    }

                    final DateTimeZone userTimeZone = timezonesByUser.get(accountId);

                    final DeviceData.Builder builder = SenseProcessorUtils.periodicDataToDeviceDataBuilder(periodicData)
                            .withAccountId(accountId)
                            .withExternalDeviceId(deviceName)
                            .withOffsetMillis(userTimeZone.getOffset(periodicDataSampleDateTime))
                            .withDateTimeUTC(periodicDataSampleDateTime)
                            .withFirmwareVersion(firmwareVersion);

                    final DeviceData deviceData = builder.build();

                    deviceDataList.add(deviceData);
                }
            }
        }


        try {
            int inserted = deviceDataDAO.batchInsertAll(deviceDataList);

            if(inserted == deviceDataList.size()) {
                LOGGER.trace("Batch saved {} data to DB", inserted);
            }else{
                LOGGER.warn("Batch save failed, save {} data using itemize insert.", inserted);
            }

            batchSaved.mark(inserted);
            batchSaveFailures.mark(deviceDataList.size() - inserted);
        } catch (Exception exception) {
            LOGGER.error("Error saving data from {} to {}, {} data discarded",
                    deviceDataList.getFirst().dateTimeUTC,
                    deviceDataList.getLast().dateTimeUTC,  // I love linkedlist
                    deviceDataList.size());
        }

        messagesProcessed.mark(records.size());

        try {
            checkpointer.checkpoint();
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint {}", e.getMessage());
        } catch (ShutdownException e) {
            LOGGER.error("Received shutdown command at checkpoint, bailing. {}", e.getMessage());
        }

        final int batchCapacity = Math.round(records.size() / (float) maxRecords * 100.0f) ;
        LOGGER.info("{} - capacity: {}%", shardId, batchCapacity);
        capacity.mark(batchCapacity);
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOGGER.warn("SHUTDOWN: {}", reason.toString());
        if(reason== ShutdownReason.TERMINATE) {
            LOGGER.warn("Going to checkpoint");
            try {
                checkpointer.checkpoint();
                LOGGER.warn("Checkpointed successfully");
            } catch (InvalidStateException e) {
                LOGGER.error(e.getMessage());
            } catch (ShutdownException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }


    private Boolean hasKinesisTimezonesEnabled(final String senseId) {
        return flipper.deviceFeatureActive(FeatureFlipper.WORKER_KINESIS_TIMEZONES, senseId, Collections.EMPTY_LIST);
    }

    private Boolean attemptToRecoverSenseReportedTimeStamp(final String senseId) {
        return flipper.deviceFeatureActive(FeatureFlipper.ATTEMPT_TO_CORRECT_SENSE_REPORTED_TIMESTAMP, senseId, Collections.EMPTY_LIST);
    }
}
