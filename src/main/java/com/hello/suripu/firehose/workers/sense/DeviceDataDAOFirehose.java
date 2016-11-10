package com.hello.suripu.firehose.workers.sense;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Lists;
import com.hello.suripu.core.db.DeviceDataIngestDAO;
import com.hello.suripu.core.firmware.HardwareVersion;
import com.hello.suripu.core.models.DeviceData;
import com.hello.suripu.firehose.FirehoseDAO;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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
    ALTER TABLE dev_sense_data ADD COLUMN hw_version INTEGER DEFAULT 1;
    ALTER TABLE dev_sense_data ADD COLUMN pressure INTEGER DEFAULT 0;
    ALTER TABLE dev_sense_data ADD COLUMN tvoc INTEGER DEFAULT 0;
    ALTER TABLE dev_sense_data ADD COLUMN co2 INTEGER DEFAULT 0;
    ALTER TABLE dev_sense_data ADD COLUMN rgb VARCHAR(64) DEFAULT '0';
    ALTER TABLE dev_sense_data ADD COLUMN ir INTEGER DEFAULT 0;
    ALTER TABLE dev_sense_data ADD COLUMN clear INTEGER DEFAULT 0;
    ALTER TABLE dev_sense_data ADD COLUMN lux_count INTEGER DEFAULT 0;
    ALTER TABLE dev_sense_data ADD COLUMN uv_count INTEGER DEFAULT 0;


     */
    private static Record toRecord(final DeviceData model) {
        final List<String> recordParameters = Lists.newArrayList();
        recordParameters.add(model.accountId.toString());
        recordParameters.add(model.externalDeviceId);
        recordParameters.add(toString(model.ambientTemperature));
        recordParameters.add(toString(model.ambientLight));
        recordParameters.add(toString(model.ambientHumidity));
        recordParameters.add(toString(model.ambientAirQuality));
        recordParameters.add(toString(model.dateTimeUTC));
        recordParameters.add(toString(model.localTime()));
        recordParameters.add(toString(model.offsetMillis));
        recordParameters.add(toString(model.ambientLightVariance));
        recordParameters.add(toString(model.ambientLightPeakiness));
        recordParameters.add(toString(model.ambientAirQualityRaw));
        recordParameters.add(toString(model.ambientDustVariance));
        recordParameters.add(toString(model.ambientDustMin));
        recordParameters.add(toString(model.ambientDustMax));
        recordParameters.add(toString(model.firmwareVersion));
        recordParameters.add(toString(model.waveCount));
        recordParameters.add(toString(model.holdCount));
        recordParameters.add(toString(model.audioNumDisturbances));
        recordParameters.add(toString(model.audioPeakDisturbancesDB));
        recordParameters.add(toString(model.audioPeakBackgroundDB));

        if (model.hasExtra()) {
            // sense 1.5
            recordParameters.add(toString(model.hardwareVersion().value));
            recordParameters.add(toString(model.extra().pressure()));
            recordParameters.add(toString(model.extra().tvoc()));
            recordParameters.add(toString(model.extra().co2()));
            recordParameters.add(model.extra().rgb());
            recordParameters.add(toString(model.extra().ir()));
            recordParameters.add(toString(model.extra().clear()));
            recordParameters.add(toString(model.extra().luxCount()));
            recordParameters.add(toString(model.extra().uvCount()));
        } else {
            recordParameters.add(toString(HardwareVersion.SENSE_ONE.value));
            recordParameters.add("0");
            recordParameters.add("0");
            recordParameters.add("0");
            recordParameters.add("0");
            recordParameters.add("0");
            recordParameters.add("0");
            recordParameters.add("0");
            recordParameters.add("0");
        }
        return toPipeDelimitedRecord(recordParameters);
    }
}
