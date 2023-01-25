package com.psolomin.plainconsumer.signals;

import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This exists because we must ack messages without records but with
 * continuation sequence number -> we must keep them until they are taken from
 * the pool and the pool advances the checkpoint.
 *
 * Original KinesisRecord assumes its data not to be null, so it can not be used
 * directly -> we must have redundant continuationSequenceNumber and
 * subSequenceNumber here.
 */
public class ExtendedKinesisRecord {
    private final String shardId;
    private final String continuationSequenceNumber;
    private final long subSequenceNumber;
    private final @Nullable KinesisRecord kinesisRecord;

    public ExtendedKinesisRecord(
            String shardId,
            String continuationSequenceNumber,
            long subSequenceNumber,
            @Nullable KinesisRecord kinesisRecord) {
        this.shardId = shardId;
        this.continuationSequenceNumber = continuationSequenceNumber;
        this.subSequenceNumber = subSequenceNumber;
        this.kinesisRecord = kinesisRecord;
    }

    public static ExtendedKinesisRecord fromRecord(KinesisRecord record) {
        return new ExtendedKinesisRecord(
                record.getShardId(), record.getSequenceNumber(), record.getSubSequenceNumber(), record);
    }

    public static ExtendedKinesisRecord fromEmpty(String shardId, String continuationSequenceNumber) {
        return new ExtendedKinesisRecord(shardId, continuationSequenceNumber, 0L, null);
    }

    public String getShardId() {
        return shardId;
    }

    public String getContinuationSequenceNumber() {
        return continuationSequenceNumber;
    }

    public long getSubSequenceNumber() {
        return subSequenceNumber;
    }

    public @Nullable KinesisRecord getKinesisRecord() {
        return kinesisRecord;
    }
}
