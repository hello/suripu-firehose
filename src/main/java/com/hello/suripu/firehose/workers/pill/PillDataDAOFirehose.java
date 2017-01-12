package com.hello.suripu.firehose.workers.pill;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Lists;
import com.hello.suripu.core.models.TrackerMotion;
import com.hello.suripu.firehose.FirehoseDAO;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

/**
 * Created by ksg via jakey on 05/02/16
 */
public class PillDataDAOFirehose extends FirehoseDAO {

    private static final String DATE_TIME_STRING_TEMPLATE = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(DATE_TIME_STRING_TEMPLATE);

    public PillDataDAOFirehose(final String deliveryStreamName, final AmazonKinesisFirehose firehose) {
        super(deliveryStreamName, firehose);
    }


    public int batchInsertAll(final List<TrackerMotion> pillData) {
        final List<Record> records = Lists.newArrayListWithCapacity(pillData.size());

        for (final TrackerMotion data : pillData) {
            records.add(toRecord(data));
        }

        final List<Record> failedRecords = batchInsertAllRecords(records);

        return pillData.size() - failedRecords.size();

    }

    public Class name() {
        return PillDataDAOFirehose.class;
    }


    private static String toString(final DateTime dateTime) {
        return dateTime.toString(DATE_TIME_FORMATTER);
    }

    private static String toString(final Integer value) {
        if (value == null) {
            return "0";
        }
        return value.toString();
    }

    private static String toString(final Long value) {
        if (value == null) {
            return "0";
        }
        return value.toString();
    }

    /*  -- redshift table
        CREATE TABLE prod_pill_data (
          account_id BIGINT,
          external_tracker_id VARCHAR(100),
          svm_no_gravity INTEGER,
          ts TIMESTAMP WITHOUT TIME ZONE,
          offset_millis INTEGER,
          local_utc_ts TIMESTAMP WITHOUT TIME ZONE,
          motion_range BIGINT,
          kickoff_counts INTEGER,
          on_duration_seconds INTEGER
        ) DISTSTYLE KEY DISTKEY (account_id)
        COMPOUND SORTKEY (local_utc_ts, account_id);

        -- 2017-01-11 Add two new columns
        ALTER TABLE dev_pill_data ADD COLUMN motion_mask BIGINT DEFAULT 0;
        ALTER TABLE dev_pill_data ADD COLUMN cost_theta BIGINT DEFAULT 0;
     */
    private static Record toRecord(final TrackerMotion model) {
        final DateTime utcTime = new DateTime(model.timestamp, DateTimeZone.UTC).withMillisOfSecond(0);
        final DateTime localUTCDateTIme = utcTime.plusMillis(model.offsetMillis);

        // note, motionMask should be non-zero for pill 1.5, use this to determine if we can use cosTheta
        final Long motionMask = (model.motionMask.isPresent()) ? model.motionMask.get() : 0L;
        final Long cosTheta = (model.cosTheta.isPresent()) ? model.cosTheta.get() : 0L;

        return toPipeDelimitedRecord(
                toString(model.accountId),
                model.externalTrackerId,
                toString(model.value),
                toString(utcTime),
                toString(model.offsetMillis),
                toString(localUTCDateTIme),
                toString(model.motionRange),
                toString(model.kickOffCounts),
                toString(model.onDurationInSeconds),
                toString(motionMask),
                toString(cosTheta)
        );
    }
}
