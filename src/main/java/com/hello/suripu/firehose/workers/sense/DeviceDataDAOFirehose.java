package com.hello.suripu.firehose.workers.sense;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamDescription;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.hello.suripu.core.db.DeviceDataIngestDAO;
import com.hello.suripu.core.models.DeviceData;

import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.hello.suripu.firehose.FirehoseDAO;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by jakepiccolo on 11/17/15.
 */
public class DeviceDataDAOFirehose extends FirehoseDAO implements DeviceDataIngestDAO {

    private static final String DATE_TIME_STRING_TEMPLATE = "yyyy-MM-dd HH:mm";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(DATE_TIME_STRING_TEMPLATE);

    public DeviceDataDAOFirehose(final String deliveryStreamName, final AmazonKinesisFirehose firehose) {
        super(deliveryStreamName, firehose);
    }


    //region DeviceDataIngestDAO implementation
    //-------------------------------------------------------------------------
    @Override
    public int batchInsertAll(final List<DeviceData> allDeviceData) {
        final List<Record> records = Lists.newArrayListWithCapacity(allDeviceData.size());

        for (final DeviceData data : allDeviceData) {
            records.add(toRecord(data));
        }

        final List<Record> failedRecords = batchInsertAllRecords(records);

        return allDeviceData.size() - failedRecords.size();
    }

    @Override
    public Class name() {
        return DeviceDataDAOFirehose.class;
    }
    //endregion

    private static String toString(final DateTime dateTime) {
        return dateTime.toString(DATE_TIME_FORMATTER);
    }

    private static String toString(final Integer value) {
        if (value == null) {
            return "0";
        }
        return value.toString();
    }

    /*
    CREATE TABLE <tableName> (
         account_id                 BIGINT NOT NULL,
         external_device_id         VARCHAR(100) NOT NULL,
         ambient_temp               INTEGER,
         ambient_light              INTEGER,
         ambient_humidity           INTEGER,
         ambient_air_quality        INTEGER,
         ts                         TIMESTAMP NOT NULL,
         local_utc_ts               TIMESTAMP,
         offset_millis              INTEGER,
         ambient_light_variance     INTEGER,
         ambient_light_peakiness    INTEGER,
         ambient_air_quality_raw    INTEGER,
         ambient_dust_variance      INTEGER,
         ambient_dust_min           INTEGER,
         ambient_dust_max           INTEGER,
         firmware_version           INTEGER,
         wave_count                 INTEGER,
         hold_count                 INTEGER,
         audio_num_disturbances     INTEGER,
         audio_peak_disturbances_db INTEGER,
         audio_peak_background_db   INTEGER
    )
    DISTKEY (account_id)
    SORTKEY (account_id, local_utc_ts);
     */
    private static Record toRecord(final DeviceData model) {
        return toPipeDelimitedRecord(
                model.accountId.toString(),
                model.externalDeviceId,
                toString(model.ambientTemperature),
                toString(model.ambientLight),
                toString(model.ambientHumidity),
                toString(model.ambientAirQuality),
                toString(model.dateTimeUTC),
                toString(model.localTime()),
                toString(model.offsetMillis),
                toString(model.ambientLightVariance),
                toString(model.ambientLightPeakiness),
                toString(model.ambientAirQualityRaw),
                toString(model.ambientDustVariance),
                toString(model.ambientDustMin),
                toString(model.ambientDustMax),
                toString(model.firmwareVersion),
                toString(model.waveCount),
                toString(model.holdCount),
                toString(model.audioNumDisturbances),
                toString(model.audioPeakDisturbancesDB),
                toString(model.audioPeakBackgroundDB)
        );
    }
}
