package com.hello.suripu.firehose.workers.messeji;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.base.Joiner;
import com.hello.messeji.api.AudioCommands;
import com.hello.messeji.api.Logging;
import com.hello.messeji.api.Messeji;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by jakepiccolo on 4/12/16.
 */
public class MessejiRequestFirehoseUtilTest {

    @Test
    public void testToRecordsPlayMessage() throws Exception {
        final String expectedDateString = "2016-01-01 01:01:01.001";
        final DateTime dateTime = new DateTime(2016, 1, 1, 1, 1, 1, 1);
        final String senseId = "sense";
        final Long order = 200L;
        final String senderId = "sender";
        final String filePath = "filePath";
        final int durationSeconds = 2;
        final int fadeOut = 3;
        final int fadeIn = 4;
        final int timeoutFadeOut = 5;
        final int volumePercent = 50;

        final Logging.RequestLog requestLog = Logging.RequestLog.newBuilder()
                .setSenseId(senseId)
                .setTimestamp(dateTime.getMillis())
                .setType(Logging.RequestLog.Type.MESSAGE)
                .setMessageRequest(Messeji.Message.newBuilder()
                        .setOrder(order)
                        .setSenderId(senderId)
                        .setType(Messeji.Message.Type.PLAY_AUDIO)
                        .setPlayAudio(AudioCommands.PlayAudio.newBuilder()
                                .setFilePath(filePath)
                                .setDurationSeconds(durationSeconds)
                                .setFadeInDurationSeconds(fadeIn)
                                .setFadeOutDurationSeconds(fadeOut)
                                .setTimeoutFadeOutDurationSeconds(timeoutFadeOut)
                                .setVolumePercent(volumePercent)
                                .build())
                        .build())
                .build();

        final List<Record> records = MessejiRequestFirehoseUtil.toRecords(requestLog);
        assertThat(records.size(), is(1));
        final String expectedRecord = Joiner.on("|").join(
                Logging.RequestLog.Type.MESSAGE, expectedDateString, senseId, Messeji.Message.Type.PLAY_AUDIO, senderId,
                order, "", filePath, volumePercent, durationSeconds, fadeIn, fadeOut, timeoutFadeOut, "", "", "") + "\n";
        assertThat(new String(records.get(0).getData().array()), is(expectedRecord));
    }

    @Test
    public void testToRecordsReceiveMessages() throws Exception {
        final String expectedDateString = "2016-01-01 01:01:01.001";
        final DateTime dateTime = new DateTime(2016, 1, 1, 1, 1, 1, 1);
        final String senseId = "sense";
        final Long messageId1 = 1L;
        final Long messageId2 = 2L;

        final Logging.RequestLog requestLog = Logging.RequestLog.newBuilder()
                .setSenseId(senseId)
                .setTimestamp(dateTime.getMillis())
                .setType(Logging.RequestLog.Type.RECEIVE_MESSAGE_REQUEST)
                .setReceiveMessageRequest(Messeji.ReceiveMessageRequest.newBuilder()
                        .setSenseId(senseId)
                        .addMessageReadId(1L)
                        .addMessageReadId(2L)
                        .build())
                .build();

        final List<Record> records = MessejiRequestFirehoseUtil.toRecords(requestLog);
        assertThat(records.size(), is(2));
        final String expectedRecord1 = Joiner.on("|").join(
                Logging.RequestLog.Type.RECEIVE_MESSAGE_REQUEST, expectedDateString, senseId, "", "",
                "", "", "", "", "", "", "", "", "", senseId, messageId1) + "\n";
        assertThat(new String(records.get(0).getData().array()), is(expectedRecord1));
        final String expectedRecord2 = Joiner.on("|").join(
                Logging.RequestLog.Type.RECEIVE_MESSAGE_REQUEST, expectedDateString, senseId, "", "",
                "", "", "", "", "", "", "", "", "", senseId, messageId2) + "\n";
        assertThat(new String(records.get(1).getData().array()), is(expectedRecord2));
    }
}