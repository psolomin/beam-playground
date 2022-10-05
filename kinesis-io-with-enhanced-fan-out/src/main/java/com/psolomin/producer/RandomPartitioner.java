package com.psolomin.producer;

import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisPartitioner;

public class RandomPartitioner implements KinesisPartitioner<byte[]> {
    @Nonnull
    @Override
    public String getPartitionKey(byte[] record) {
        return String.valueOf(Arrays.hashCode(record));
    }

    @Nullable
    @Override
    public String getExplicitHashKey(byte[] record) {
        return String.valueOf(Arrays.hashCode(record));
    }
}
