package com.psolomin.plainconsumer;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;

class ShardProgress {
    private static final Logger LOG = LoggerFactory.getLogger(ShardProgress.class);

    private final Config config;
    private final String shardId;
    private Optional<String> lastSequenceNumber;
    private Long eventsCnt;

    ShardProgress(Config config, String shardId, String lastSequenceNumber) {
        this.config = config;
        this.shardId = shardId;
        this.lastSequenceNumber = Optional.of(lastSequenceNumber);
        this.eventsCnt = 0L;
    }

    ShardProgress(Config config, String shardId) {
        this.config = config;
        this.shardId = shardId;
        this.lastSequenceNumber = Optional.empty();
        this.eventsCnt = 0L;
    }

    void setLastSequenceNumber(String sequenceNumber, long recordsIncrement) {
        lastSequenceNumber = Optional.of(sequenceNumber);
        eventsCnt += recordsIncrement;
        LOG.debug("Events cnt = {}. Updated lastSequenceNumber to {}", this.eventsCnt, lastSequenceNumber);
    }

    String getShardId() {
        return shardId;
    }

    StartingPosition computeNextStartingPosition() {
        return lastSequenceNumber
                .map(sn -> StartingPosition.builder()
                        .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                        .sequenceNumber(sn)
                        .build())
                .orElse(buildInitialStartingPosition());
    }

    private StartingPosition buildInitialStartingPosition() {
        switch (config.getStartType()) {
            case LATEST:
                return StartingPosition.builder().type(ShardIteratorType.LATEST).build();

            case AT_TIMESTAMP:
                return StartingPosition.builder()
                        .timestamp(config.getStartTimestamp())
                        .type(ShardIteratorType.AT_TIMESTAMP)
                        .build();

            case TRIM_HORIZON:
                return StartingPosition.builder()
                        .type(ShardIteratorType.TRIM_HORIZON)
                        .build();
            default:
                throw new IllegalStateException(String.format("Invalid config %s", config));
        }
    }
}
