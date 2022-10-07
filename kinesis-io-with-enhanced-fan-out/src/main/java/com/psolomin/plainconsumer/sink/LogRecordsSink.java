package com.psolomin.plainconsumer.sink;

import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class LogRecordsSink implements RecordsSink {
    private static final Logger LOG = LoggerFactory.getLogger(LogRecordsSink.class);

    @Override
    public void submit(String shardId, KinesisClientRecord record) {
        printRecord(shardId, transformToRepr(record));
    }

    @Override
    public void submit(String shardId, Iterable<KinesisClientRecord> records) {
        records.forEach(r -> printRecord(shardId, transformToRepr(r)));
    }

    private String transformToRepr(KinesisClientRecord record) {
        return StandardCharsets.UTF_8.decode(record.data()).toString();
    }

    private void printRecord(String shardId, String recordRepr) {
        LOG.debug("Shard = {} got record with value {}", shardId, recordRepr);
    }
}
