package com.psolomin.producer;

import java.util.UUID;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner;

public class RandomPartitioner implements KinesisPartitioner<byte[]> {
    @Nonnull
    @Override
    public String getPartitionKey(byte[] record) {
        return UUID.randomUUID().toString();
    }
}
