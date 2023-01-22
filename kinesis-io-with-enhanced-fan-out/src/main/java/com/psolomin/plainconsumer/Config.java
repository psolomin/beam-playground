package com.psolomin.plainconsumer;

import static com.psolomin.plainconsumer.Checkers.checkNotNull;

import java.io.Serializable;
import java.time.Instant;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import software.amazon.kinesis.common.InitialPositionInStream;

/** This class is immutable. */
public class Config implements Serializable {
    private final String streamName;
    private final String consumerArn;
    private final StartingPoint startingPoint;
    private final Optional<Instant> startTimestamp;

    private static final int POOL_SIGNALS_QUEUE_CAPACITY_DEFAULT = 1000;
    private final int poolSignalsQueueCapacity;
    private static final long POOL_SIGNALS_OFFER_TIMEOUT_MS_DEFAULT = 10_000L;
    private final long poolSignalsOfferTimeoutMs;
    private static final long POOL_SIGNALS_POLL_TIMEOUT_MS_DEFAULT = 10_000L;
    private final long poolSignalsPollTimeoutMs;
    private static final long POOL_START_TIMEOUT_MS_DEFAULT = 10_000L;
    private final long poolStartTimeoutMs;
    private static final long POOL_AWAIT_TERMINATION_TIMEOUT_MS_DEFAULT = 30_000;
    private final long poolAwaitTerminationTimeoutMs;

    // If network is OK, but the shard has no records, it will still receive
    // empty-msg every ~ 5 seconds which we should use for checkpointing.
    // If this timeout is exceeded, better to try to re-subscribe.
    private static final long SHARD_SUBSCRIBER_BUFFER_POLL_TIMEOUT_MS_DEFAULT = 7_000L;
    private final long shardSubscriberBufferPollTimeoutMs;

    private static final long RECORDS_BUFFER_POLL_TIMEOUT_MS_DEFAULT = 1_000L;
    private final long recordsBufferPollTimeoutMs;
    private static final int RECORDS_BUFFER_MAX_CAPACITY_DEFAULT = 10_000;
    private final int recordsBufferMaxCapacity;
    private static final long RECORDS_BUFFER_OFFER_TIMEOUT_MS_DEFAULT = 5_000L;
    private final long recordsBufferOfferTimeoutMs;

    public Config(
            String streamName,
            String consumerArn,
            StartingPoint startingPoint,
            Optional<Instant> startTimestamp,
            long poolSignalsPollTimeoutMs,
            long shardSubscriberBufferPollTimeoutMs,
            int recordsBufferMaxCapacity,
            long recordsBufferOfferTimeoutMs,
            long recordsBufferPollTimeoutMs) {
        if (startingPoint.getPosition().equals(InitialPositionInStream.AT_TIMESTAMP) && !startTimestamp.isPresent()) {
            throw new IllegalStateException("Timestamp must not be empty");
        }

        this.streamName = streamName;
        this.consumerArn = consumerArn;
        this.startingPoint = startingPoint;
        this.startTimestamp = startTimestamp;
        this.poolSignalsPollTimeoutMs = poolSignalsPollTimeoutMs;
        this.poolSignalsQueueCapacity = POOL_SIGNALS_QUEUE_CAPACITY_DEFAULT;
        this.poolSignalsOfferTimeoutMs = POOL_SIGNALS_OFFER_TIMEOUT_MS_DEFAULT;
        this.poolStartTimeoutMs = POOL_START_TIMEOUT_MS_DEFAULT;
        this.poolAwaitTerminationTimeoutMs = POOL_AWAIT_TERMINATION_TIMEOUT_MS_DEFAULT;
        this.shardSubscriberBufferPollTimeoutMs = shardSubscriberBufferPollTimeoutMs;
        this.recordsBufferMaxCapacity = recordsBufferMaxCapacity;
        this.recordsBufferOfferTimeoutMs = recordsBufferOfferTimeoutMs;
        this.recordsBufferPollTimeoutMs = recordsBufferPollTimeoutMs;
    }

    public Config(String streamName, String consumerArn, StartingPoint startingPoint) {
        this(
                streamName,
                consumerArn,
                startingPoint,
                Optional.absent(),
                POOL_SIGNALS_POLL_TIMEOUT_MS_DEFAULT,
                SHARD_SUBSCRIBER_BUFFER_POLL_TIMEOUT_MS_DEFAULT,
                RECORDS_BUFFER_MAX_CAPACITY_DEFAULT,
                RECORDS_BUFFER_OFFER_TIMEOUT_MS_DEFAULT,
                RECORDS_BUFFER_POLL_TIMEOUT_MS_DEFAULT);
    }

    public static Config fromIOSpec(KinesisIO.Read spec) {
        return new Config(
                checkNotNull(spec.getStreamName(), "streamName is null"),
                checkNotNull(spec.getConsumerArn(), "consumer ARN is null"),
                checkNotNull(spec.getInitialPosition(), "initial position is null"));
    }

    public String getStreamName() {
        return streamName;
    }

    public String getConsumerArn() {
        return consumerArn;
    }

    public StartingPoint getStartingPoint() {
        return startingPoint;
    }

    public Instant getStartTimestamp() {
        return startTimestamp.get();
    }

    public long getShardSubscriberBufferPollTimeoutMs() {
        return shardSubscriberBufferPollTimeoutMs;
    }

    public long getRecordsBufferPollTimeoutMs() {
        return recordsBufferPollTimeoutMs;
    }

    public int getRecordsBufferMaxCapacity() {
        return recordsBufferMaxCapacity;
    }

    public long getRecordsBufferOfferTimeoutMs() {
        return recordsBufferOfferTimeoutMs;
    }

    public int getPoolSignalsQueueCapacity() {
        return poolSignalsQueueCapacity;
    }

    public long getPoolSignalsOfferTimeoutMs() {
        return poolSignalsOfferTimeoutMs;
    }

    public long getPoolSignalsPollTimeoutMs() {
        return poolSignalsPollTimeoutMs;
    }

    public long getPoolStartTimeoutMs() {
        return poolStartTimeoutMs;
    }

    public long getPoolAwaitTerminationTimeoutMs() {
        return poolAwaitTerminationTimeoutMs;
    }
}
