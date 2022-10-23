package com.psolomin.plainconsumer.sink;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class InMemCollectionRecordsSink implements RecordsSink {
    private final Map<String, Queue<KinesisClientRecord>> events;

    public InMemCollectionRecordsSink() {
        this.events = new ConcurrentHashMap<>();
    }

    @Override
    public void submit(String shardId, KinesisClientRecord record) {
        addSingleRecord(shardId, record);
    }

    @Override
    public void submit(String shardId, Iterable<KinesisClientRecord> records) {
        records.forEach(r -> addSingleRecord(shardId, r));
    }

    @Override
    public long getTotalCnt() {
        return events.values().stream().map(q -> (long) q.size()).reduce(0L, Long::sum);
    }

    public List<KinesisClientRecord> getAllRecords(String shardId) {
        return new ArrayList<>(events.get(shardId));
    }

    public Set<String> shardsSeen() {
        return events.keySet();
    }

    private void addSingleRecord(String shardId, KinesisClientRecord r) {
        events.computeIfAbsent(shardId, k -> new LinkedList<>()).add(r);
    }
}
