package com.psolomin.plainconsumer;

import com.psolomin.plainconsumer.sink.RecordsSink;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.kinesis.retrieval.AggregatorUtil;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class ShardEventsConsumer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ShardEventsConsumer.class);
    private AtomicBoolean isRunning;
    private final Config config;
    private final String shardId;
    private final ShardSubscriber shardSubscriber;
    private final ShardProgress shardProgress;
    private final RecordsSink recordsSink;

    ShardEventsConsumer(
            StreamConsumer streamConsumer,
            Config config,
            ClientBuilder builder,
            RecordsSink recordsSink,
            String shardId,
            ShardProgress shardProgress) {
        this.config = config;
        this.shardId = shardId;
        this.shardSubscriber = new ShardSubscriber(
                streamConsumer, builder.build(), config.getStreamName(), config.getConsumerArn(), shardId);
        this.shardProgress = shardProgress;
        this.recordsSink = recordsSink;
        this.isRunning = new AtomicBoolean(true);
    }

    static ShardEventsConsumer fromShardProgress(
            StreamConsumer streamConsumer,
            Config config,
            ClientBuilder builder,
            RecordsSink recordsSink,
            ShardProgress progress) {
        return new ShardEventsConsumer(streamConsumer, config, builder, recordsSink, progress.getShardId(), progress);
    }

    @Override
    public void run() {
        while (isRunning.get()) {
            try {
                StartingPosition startingPosition = shardProgress.computeNextStartingPosition();
                LOG.info("Shard {} - Starting subscription with position = {}", shardId, startingPosition);
                boolean reSubscribe = shardSubscriber.subscribe(startingPosition, this::consume);
                if (!reSubscribe) {
                    isRunning.set(false);
                } else {
                    LOG.info("Will re-subscribe");
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void consume(SubscribeToShardEvent event) {
        long recordsArrivedInBatch = 0L;
        if (!event.records().isEmpty()) {
            List<KinesisClientRecord> clientRecords = new AggregatorUtil()
                    .deaggregate(event.records().stream()
                            .map(KinesisClientRecord::fromRecord)
                            .collect(Collectors.toList()));
            processClientRecords(clientRecords);
            recordsArrivedInBatch = event.records().size();
        }

        shardProgress.setLastSequenceNumber(event.continuationSequenceNumber(), recordsArrivedInBatch);
    }

    private void processClientRecords(List<KinesisClientRecord> clientRecords) {
        recordsSink.submit(shardId, clientRecords);
    }

    void initiateGracefulShutdown() {
        isRunning.set(false);
        shardSubscriber.cancel();
    }
}
