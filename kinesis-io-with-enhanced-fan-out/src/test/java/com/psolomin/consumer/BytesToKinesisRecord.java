package com.psolomin.consumer;

import java.nio.ByteBuffer;
import java.time.Instant;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class BytesToKinesisRecord implements SerializableFunction<byte[], KinesisRecord> {
    @Override
    public KinesisRecord apply(byte[] input) {
        return kinesisRecord(input);
    }

    private static KinesisRecord kinesisRecord(byte[] payload) {
        return new KinesisRecord(
                KinesisClientRecord.builder()
                        .data(ByteBuffer.wrap(payload))
                        .partitionKey("abc")
                        .sequenceNumber("99")
                        .approximateArrivalTimestamp(Instant.now())
                        .build(),
                "stream-01",
                "shard-000");
    }
}
