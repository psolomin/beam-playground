package com.psolomin.plainproducer;

import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final Integer LOG_AFTER_REQUESTS_CNT = 100;

    public static void main(String[] args) throws InterruptedException {
        String streamName = args[0];
        Long intervalBetweenRequestsMs = Long.valueOf(args[1]);
        Long eventsToSendTotal = Long.valueOf(args[2]);
        Long counter = 0L;
        KinesisClient client = KinesisClient.create();

        while (counter < eventsToSendTotal) {
            send(counter, client, streamName);
            counter++;
            if (counter % LOG_AFTER_REQUESTS_CNT == 0) {
                LOG.info("Sent events total: {}", counter);
            }
            Thread.sleep(intervalBetweenRequestsMs);
        }
    }

    private static void send(Long number, KinesisClient kinesisClient, String streamName) {
        byte[] bytes = number.toString().getBytes(StandardCharsets.UTF_8);

        PutRecordRequest putRecord = PutRecordRequest.builder()
                .streamName(streamName)
                .partitionKey(String.valueOf(Math.random()))
                .data(SdkBytes.fromByteArray(bytes))
                .build();
        try {
            kinesisClient.putRecord(putRecord);
        } catch (SdkClientException ex) {
            LOG.warn("Error sending record to Amazon Kinesis.", ex);
        }
    }
}
