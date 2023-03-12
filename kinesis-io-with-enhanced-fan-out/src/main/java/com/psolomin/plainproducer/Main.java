package com.psolomin.plainproducer;

import com.psolomin.producer.LogEventEncoder;
import com.psolomin.records.LogEvent;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final Integer LOG_AFTER_REQUESTS_CNT = 100;

    public static void main(String[] args) throws InterruptedException, IOException {
        LogEventEncoder encoder = new LogEventEncoder();
        String streamName = args[0];
        Long intervalBetweenRequestsMs = Long.valueOf(args[1]);
        Long eventsToSendTotal = Long.valueOf(args[2]);
        Long counter = 0L;
        KinesisClient client = KinesisClient.create();

        while (counter < eventsToSendTotal) {
            byte[] payload = encoder.encode(new LogEvent(counter));
            send(payload, client, streamName);
            counter++;
            if (counter % LOG_AFTER_REQUESTS_CNT == 0) {
                LOG.info("Sent events total: {}", counter);
            }
            Thread.sleep(intervalBetweenRequestsMs);
        }
    }

    private static void send(byte[] payload, KinesisClient kinesisClient, String streamName) {
        PutRecordRequest putRecord = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(String.valueOf(Math.random()))
                .data(SdkBytes.fromByteArray(payload))
                .build();
        try {
            kinesisClient.putRecord(putRecord);
        } catch (SdkClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
}
