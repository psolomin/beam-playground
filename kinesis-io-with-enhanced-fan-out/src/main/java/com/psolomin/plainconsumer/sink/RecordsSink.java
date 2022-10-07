package com.psolomin.plainconsumer.sink;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

public interface RecordsSink {
    void submit(String shardId, KinesisClientRecord record);

    void submit(String shardId, Iterable<KinesisClientRecord> records);
}
