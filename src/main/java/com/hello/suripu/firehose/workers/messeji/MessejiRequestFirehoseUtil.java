package com.hello.suripu.firehose.workers.messeji;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Lists;
import com.hello.messeji.api.Logging;
import com.hello.suripu.firehose.FirehoseDAO;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;

/**
 * Created by jakepiccolo on 4/12/16.
 */
public class MessejiRequestFirehoseUtil {

    private static final String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd HH:mm:ss.SSS";
    protected static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern(DATE_TIME_FORMAT_STRING);

    private static String toString(final Object object) {
        if (object == null) {
            return "";
        }
        return object.toString();
    }

    private static void addString(final List<String> columns, final Object object) {
        columns.add(toString(object));
    }

    private static String dateTimeString(final long millis) {
        return new DateTime(millis, DateTimeZone.UTC).toString(DATE_TIME_FORMATTER);
    }

    /*
     CREATE TABLE $table_name (
        request_log_type                        VARCHAR(50),
        timestamp                               TIMESTAMP,
        sense_id                                VARCHAR(100),
        message_type                            VARCHAR(50),
        message_sender_id                       VARCHAR(100),
        message_order                           BIGINT,
        message_id                              BIGINT,
        message_play_audio_file_path            VARCHAR(50),
        message_play_audio_volume_percent       INTEGER,
        message_play_audio_duration             INTEGER,
        message_play_audio_fade_in              INTEGER,
        message_play_audio_fade_out             INTEGER,
        message_play_audio_timeout_fade_out     INTEGER,
        message_stop_audio_fade_out             INTEGER,
        receive_message_sense_id                VARCHAR(100),
        receive_message_message_read_id         BIGINT
     )
     DISTKEY (sense_id)
     SORTKEY (sense_id, timestamp);
    */
    protected static List<Record> toRecords(final Logging.RequestLog requestLog) {
        final boolean hasMessage = requestLog.hasMessageRequest();
        final boolean hasMessageId = hasMessage && requestLog.getMessageRequest().hasMessageId();
        final boolean hasPlayAudio = hasMessage && requestLog.getMessageRequest().hasPlayAudio();
        final boolean hasStopAudio = hasMessage && requestLog.getMessageRequest().hasStopAudio();
        final boolean hasReceiveMessages = requestLog.hasReceiveMessageRequest();

        final List<String> columns = Lists.newArrayList();
        addString(columns, requestLog.getType());
        addString(columns, requestLog.hasTimestamp() ?  dateTimeString(requestLog.getTimestamp()): null);
        addString(columns, requestLog.getSenseId());
        addString(columns, hasMessage ? requestLog.getMessageRequest().getType() : null);
        addString(columns, hasMessage ? requestLog.getMessageRequest().getSenderId() : null);
        addString(columns, hasMessage ? requestLog.getMessageRequest().getOrder() : null);
        addString(columns, hasMessageId ? requestLog.getMessageRequest().getMessageId() : null);
        addString(columns, hasPlayAudio ? requestLog.getMessageRequest().getPlayAudio().getFilePath() : null);
        addString(columns, hasPlayAudio ? requestLog.getMessageRequest().getPlayAudio().getVolumePercent() : null);
        addString(columns, hasPlayAudio ? requestLog.getMessageRequest().getPlayAudio().getDurationSeconds() : null);
        addString(columns, hasPlayAudio ? requestLog.getMessageRequest().getPlayAudio().getFadeInDurationSeconds() : null);
        addString(columns, hasPlayAudio ? requestLog.getMessageRequest().getPlayAudio().getFadeOutDurationSeconds() : null);
        addString(columns, hasPlayAudio ? requestLog.getMessageRequest().getPlayAudio().getTimeoutFadeOutDurationSeconds() : null);
        addString(columns, hasStopAudio ? requestLog.getMessageRequest().getStopAudio().getFadeOutDurationSeconds() : null);
        addString(columns, hasReceiveMessages ? requestLog.getReceiveMessageRequest().getSenseId() : null);

        // Default is null for the messageReadId column
        addString(columns, null);
        final int messageReadIdIndex = columns.size() - 1;
        final List<Record> records = Lists.newArrayList();
        // Insert duplicate records for each messageReadId
        if (hasReceiveMessages && requestLog.getReceiveMessageRequest().getMessageReadIdCount() > 0) {
            for (final Long messageReadId : requestLog.getReceiveMessageRequest().getMessageReadIdList()) {
                columns.set(messageReadIdIndex, toString(messageReadId));
                records.add(FirehoseDAO.toPipeDelimitedRecord(columns));
            }
        } else {
            records.add(FirehoseDAO.toPipeDelimitedRecord(columns));
        }

        return records;
    }

    protected static List<Record> toRecords(final List<Logging.RequestLog> requestLogs) {
        final List<Record> records = Lists.newArrayList();
        for (final Logging.RequestLog requestLog: requestLogs) {
            records.addAll(toRecords(requestLog));
        }
        return records;
    }
}
