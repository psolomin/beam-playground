package com.psolomin.plainconsumer.sink;

import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class LogCountRecordsSink implements RecordsSink {
    private static final Logger LOG = LoggerFactory.getLogger(LogCountRecordsSink.class);
    private static final AtomicLong counter = new AtomicLong(0);
    private static final Integer printEveryN = 100;

    @Override
    public void submit(String shardId, KinesisClientRecord record) {
        Long current = counter.incrementAndGet();
        if (current % printEveryN == 0) {
            LOG.info("Received events total: {}", current);
        }
    }

    @Override
    public void submit(String shardId, Iterable<KinesisClientRecord> records) {
        records.forEach(r -> submit(shardId, r));
    }

    @Override
    public long getTotalCnt() {
        return counter.get();
    }
}
