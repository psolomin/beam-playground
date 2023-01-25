package com.psolomin.plainconsumer.signals;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class RecordsShardEvent extends ShardEventAbs {
    private final List<ExtendedKinesisRecord> records;

    private RecordsShardEvent(String streamName, String shardId, SubscribeToShardEvent event) {
        super(shardId, ShardEventType.RECORDS);

        if (event.hasRecords() && !event.records().isEmpty()) {
            AggregatorUtil au = new AggregatorUtil();
            List<KinesisClientRecord> deAggregated = au.deaggregate(event.records().stream()
                    .map(KinesisClientRecord::fromRecord)
                    .collect(Collectors.toList()));
            this.records = deAggregated.stream()
                    .map(r -> ExtendedKinesisRecord.fromRecord(new KinesisRecord(r, streamName, shardId)))
                    .collect(Collectors.toList());
        } else {
            this.records = List.of(ExtendedKinesisRecord.fromEmpty(shardId, event.continuationSequenceNumber()));
        }
    }

    public static RecordsShardEvent fromNext(String streamName, String shardId, SubscribeToShardEvent event) {
        return new RecordsShardEvent(streamName, shardId, event);
    }

    public List<ExtendedKinesisRecord> getRecords() {
        return records;
    }
}
