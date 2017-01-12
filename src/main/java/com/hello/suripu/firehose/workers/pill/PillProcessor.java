package com.hello.suripu.firehose.workers.pill;

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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hello.suripu.api.ble.SenseCommandProtos;
import com.hello.suripu.core.db.DeviceDAO;
import com.hello.suripu.core.db.KeyStore;
import com.hello.suripu.core.db.MergedUserInfoDynamoDB;
import com.hello.suripu.core.models.DeviceAccountPair;
import com.hello.suripu.core.models.TrackerMotion;
import com.hello.suripu.core.models.UserInfo;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ksg via jakey on 05/02/16
 */
public class PillProcessor implements IRecordProcessor {

    private final static Logger LOGGER = LoggerFactory.getLogger(PillProcessor.class);

    private final MergedUserInfoDynamoDB mergedUserInfoDynamoDB;
    private final KeyStore pillKeyStore;
    private final DeviceDAO deviceDAO;

    private final PillDataDAOFirehose firehoseDAO;
    private String shardId = "";

    private final Meter recordsProcessed;
    private final Meter batchSaved;
    private final Meter batchSaveFailures;

    protected PillProcessor(final MergedUserInfoDynamoDB mergedUserInfoDynamoDB,
                            final KeyStore pillKeyStore,
                            final DeviceDAO deviceDAO,
                            final PillDataDAOFirehose firehoseDAO, final Meter recordsProcessed,
                            final Meter batchSaved, final Meter batchSaveFailures) {
        this.mergedUserInfoDynamoDB = mergedUserInfoDynamoDB;
        this.pillKeyStore = pillKeyStore;
        this.deviceDAO = deviceDAO;
        this.firehoseDAO = firehoseDAO;
        this.recordsProcessed = recordsProcessed;
        this.batchSaved = batchSaved;
        this.batchSaveFailures = batchSaveFailures;
    }

    public static PillProcessor create(final MergedUserInfoDynamoDB mergedUserInfoDynamoDB,
                                       final KeyStore pillKeyStore,
                                       final DeviceDAO deviceDAO,
                                       final PillDataDAOFirehose firehoseDAO,
                                       final MetricRegistry metricRegistry) {
        final Meter recordsProcessed = metricRegistry.meter(MetricRegistry.name(PillProcessor.class, "records", "records-processed"));
        final Meter batchSaved = metricRegistry.meter(MetricRegistry.name(PillProcessor.class, "records", "batch-saved"));
        final Meter batchSaveFailures = metricRegistry.meter(MetricRegistry.name(PillProcessor.class, "records", "batch-save-failures"));
        return new PillProcessor(mergedUserInfoDynamoDB, pillKeyStore, deviceDAO, firehoseDAO, recordsProcessed, batchSaved, batchSaveFailures);
    }

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer, final String sequenceNumberString) {
        try {
            if (sequenceNumberString.isEmpty()) {
                checkpointer.checkpoint(sequenceNumberString);
            } else {
                checkpointer.checkpoint();
            }
            LOGGER.info("checkpoint=success sequence_number={}", sequenceNumberString);
        } catch (InvalidStateException e) {
            LOGGER.error("checkpoint=failure sequence_number={} error=InvalidStateException exception={}", sequenceNumberString, e);
        } catch (ShutdownException e) {
            LOGGER.error("checkpoint=failure sequence_number={} error=ShutdownException exception={}", sequenceNumberString, e);
        }
    }

    @Override
    public void processRecords(final List<Record> records, final IRecordProcessorCheckpointer checkpointer) {
        final String lastSequenceNumber = records.isEmpty() ? "" : records.get(records.size()-1).getSequenceNumber();

        // parse kinesis records -- from pill worker
        final ArrayList<TrackerMotion> trackerData = Lists.newArrayListWithExpectedSize(records.size());
        final List<SenseCommandProtos.pill_data> pillData = Lists.newArrayList();
        final Map<String, Optional<DeviceAccountPair>> pairs = Maps.newHashMap();
        final Map<String, Optional<UserInfo>> userInfos = Maps.newHashMap();
        final Map<String, String> pillIdToSenseId = Maps.newHashMap();

        for (final Record record : records) {
            try {
                final SenseCommandProtos.batched_pill_data batched_pill_data = SenseCommandProtos.batched_pill_data.parseFrom(record.getData().array());
                for (final SenseCommandProtos.pill_data data : batched_pill_data.getPillsList()) {
                    pillData.add(data);
                    pillIdToSenseId.put(data.getDeviceId(), batched_pill_data.getDeviceId());
                }
            } catch (InvalidProtocolBufferException e) {
                LOGGER.error("error=fail-to-decode-protobuf error_msg={}", e.getMessage());
            } catch (IllegalArgumentException e) {
                LOGGER.error("error=fail-to-decrypt-pill-data bytes={} error_msg={}", record.getData().array(), e.getMessage());
            }
        }

        // process pill data
        if (!pillData.isEmpty()) {
            try {
                // get decryption key from keystore 100 at a time
                final List<String> pillIds = Lists.newArrayList(pillIdToSenseId.keySet());
                final List<List<String>> pillIdsList = Lists.partition(pillIds, 100);

                final Map<String, Optional<byte[]>> pillKeys = Maps.newHashMap();

                for (final List<String> batch : pillIdsList) {
                    final Map<String, Optional<byte[]>> keys = pillKeyStore.getBatch(Sets.newHashSet(batch));
                    if (keys.isEmpty()) {
                        LOGGER.error("error=fail-to-retrieve-decryption-keys-bailing");
                        System.exit(1);
                    }

                    pillKeys.putAll(keys);
                }

                // get external_pill_id to account_id mapping (commonDB)
                for (final String pillId : pillIds) {
                    final Optional<DeviceAccountPair> optionalPair = deviceDAO.getInternalPillId(pillId);
                    pairs.put(pillId, optionalPair);
                }

                // get timezones from alarm_info via external_sense_id
                for (final String pillId : pillIds) {
                    final String senseId = pillIdToSenseId.get(pillId);
                    final Optional<DeviceAccountPair> pair = pairs.get(pillId);
                    if (pair.isPresent()) {
                        final Optional<UserInfo> userInfoOptional = mergedUserInfoDynamoDB.getInfo(senseId, pair.get().accountId);
                        userInfos.put(pillId, userInfoOptional);
                    } else {
                        userInfos.put(pillId, Optional.absent());
                    }
                }

                for (final SenseCommandProtos.pill_data data : pillData) {
                    final String external_pill_id = data.getDeviceId();

                    final Optional<byte[]> optionalDecryptionKey = pillKeys.get(external_pill_id);

                    // The key should not be null
                    if (!optionalDecryptionKey.isPresent()) {
                        LOGGER.error("error=missing-decryption-key pill_id={}", external_pill_id);
                        continue;
                    }
                    final byte[] decryptKey = optionalDecryptionKey.get();


                    final Optional<DeviceAccountPair> optionalPair = pairs.get(external_pill_id);
                    if (!optionalPair.isPresent()) {
                        LOGGER.error("error=missing-pairing-in-account-tracker-map pill_id={}", external_pill_id);
                        continue;
                    }
                    final DeviceAccountPair pair = optionalPair.get(); // for account_id

                    final Optional<UserInfo> userInfoOptional = userInfos.get(external_pill_id);
                    if (!userInfoOptional.isPresent()) {
                        final String senseId = pillIdToSenseId.get(external_pill_id);
                        LOGGER.error("error=missing-UserInfo account_id={} pill_external_id={} and sense_id={}", pair.accountId, pair.externalDeviceId, senseId);
                        continue;
                    }

                    final UserInfo userInfo = userInfoOptional.get();
                    final Optional<DateTimeZone> timeZoneOptional = userInfo.timeZone;
                    if (!timeZoneOptional.isPresent()) {
                        LOGGER.error("error=no-timezone account_id={} pill_id={}", pair.accountId, pair.externalDeviceId);
                        continue;
                    }
                    final DateTimeZone timeZone = timeZoneOptional.get();


                    if (data.hasMotionDataEntrypted()) {
                        try {
                            final TrackerMotion trackerMotion = TrackerMotion.create(data, pair, timeZone, decryptKey);
                            trackerData.add(trackerMotion);
                        } catch (TrackerMotion.InvalidEncryptedPayloadException exception) {
                            LOGGER.error("error=fail-to-decrypt-tracker-motion-payload pill_id={} account_id={}", pair.externalDeviceId, pair.accountId);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("error=fail-processing-pill error_msg={} exception={}", e.getMessage(), e);
            }
        }


        try {
            if (!trackerData.isEmpty()) {
                final int inserted = firehoseDAO.batchInsertAll(trackerData);
                final int failures = trackerData.size() - inserted;
                LOGGER.debug("action=batch-insert-firehose batch_size={} inserted={} failures={}", trackerData.size(), inserted, failures);
                batchSaved.mark(inserted);
                batchSaveFailures.mark(failures);
            }
            checkpoint(checkpointer, lastSequenceNumber);
            recordsProcessed.mark(records.size());
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
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOGGER.warn("warning=shutdown reason={}", reason.toString());
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer, "");
        }
    }
}
