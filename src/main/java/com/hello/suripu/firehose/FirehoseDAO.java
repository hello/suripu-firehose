package com.hello.suripu.firehose;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.DeliveryStreamDescription;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ServiceUnavailableException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jakepiccolo on 4/8/16.
 */
public class FirehoseDAO {
    private final static Logger LOGGER = LoggerFactory.getLogger(FirehoseDAO.class);

    private final String deliveryStreamName;
    private final AmazonKinesisFirehose firehose;

    private static final Integer MAX_PUT_RECORDS = 500;
    private static final Integer MAX_BATCH_PUT_ATTEMPTS = 5;

    public FirehoseDAO(final String deliveryStreamName, final AmazonKinesisFirehose firehose) {
        this.deliveryStreamName = deliveryStreamName;
        this.firehose = firehose;
    }

    public DeliveryStreamDescription describeStream() {
        return firehose
                .describeDeliveryStream(new DescribeDeliveryStreamRequest().withDeliveryStreamName(deliveryStreamName))
                .getDeliveryStreamDescription();
    }

    /**
     * Partition and insert all records.
     * @return List of failed records.
     */
    public List<Record> batchInsertAllRecords(final List<Record> records) {
        final List<Record> uninserted = Lists.newArrayList();
        for (final List<Record> recordsToInsert : Lists.partition(records, MAX_PUT_RECORDS)) {
            uninserted.addAll(batchInsertRecords(recordsToInsert));
        }
        return uninserted;
    }

    /**
     * Insert records that are <= the limit set by Amazon (MAX_PUT_RECORDS).
     * @return the list of failed records.
     */
    public List<Record> batchInsertRecords(final List<Record> records) {
        int numAttempts = 0;
        List<Record> uninsertedRecords = records;

        while (!uninsertedRecords.isEmpty() && numAttempts < MAX_BATCH_PUT_ATTEMPTS) {
            numAttempts++;
            final PutRecordBatchRequest batchRequest = new PutRecordBatchRequest()
                    .withDeliveryStreamName(deliveryStreamName)
                    .withRecords(uninsertedRecords);

            try {
                final PutRecordBatchResult result = firehose.putRecordBatch(batchRequest);
                uninsertedRecords = failedRecords(uninsertedRecords, result);
            } catch (ServiceUnavailableException sue) {
                if (numAttempts < MAX_BATCH_PUT_ATTEMPTS) {
                    backoff(numAttempts);
                } else {
                    LOGGER.error("ServiceUnavailableException persists, out of retries. Failed to write {} records.",
                            uninsertedRecords.size());
                }
            }
        }

        return uninsertedRecords;
    }

    private static List<Record> failedRecords(final List<Record> attemptedRecords,
                                              final PutRecordBatchResult batchResult) {
        final List<Record> failed = Lists.newArrayList();
        if (batchResult.getFailedPutCount() == 0) {
            // All successful!
            return failed;
        }

        final List<PutRecordBatchResponseEntry> responseEntries = batchResult.getRequestResponses();
        for (int i = 0; i < responseEntries.size(); i++) {
            if (responseEntries.get(i).getErrorCode() != null) {
                LOGGER.error("Encountered error code while inserting record: {}", responseEntries.get(i).getErrorCode());
                failed.add(attemptedRecords.get(i));
            }
        }
        return failed;
    }

    private void backoff(int numberOfAttempts) {
        try {
            long sleepMillis = (long) Math.pow(2, numberOfAttempts) * 50;
            LOGGER.warn("Throttled by Firehose, sleeping for {} ms.", sleepMillis);
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while attempting exponential backoff.");
        }
    }

    public static Record toPipeDelimitedRecord(final Iterable<String> strings) {
        final String pipeDelimited = Joiner.on("|").join(strings);
        final String data = pipeDelimited + "\n";
        return createRecord(data);
    }

    public static Record toPipeDelimitedRecord(final String... strings) {
        return toPipeDelimitedRecord(Arrays.asList(strings));
    }

    private static Record createRecord(final String data) {
        return new Record().withData(ByteBuffer.wrap(data.getBytes()));
    }

}
