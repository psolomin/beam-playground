package com.psolomin.plainconsumer;

import java.time.Instant;
import java.util.Optional;

public class Config {
    private final String streamName;
    private final String consumerArn;
    private final StartType startType;
    private final Optional<Instant> startTimestamp;

    public Config(String streamName, String consumerArn, StartType startType, Optional<Instant> startTimestamp) {
        if (startType.equals(StartType.AT_TIMESTAMP) && startTimestamp.isEmpty())
            throw new IllegalStateException("Timestamp must not be empty");

        this.streamName = streamName;
        this.consumerArn = consumerArn;
        this.startType = startType;
        this.startTimestamp = startTimestamp;
    }

    public Config(String streamName, String consumerArn, StartType startType) {
        this(streamName, consumerArn, startType, Optional.empty());
    }

    public String getStreamName() {
        return streamName;
    }

    public String getConsumerArn() {
        return consumerArn;
    }

    public StartType getStartType() {
        return startType;
    }

    public Instant getStartTimestamp() {
        return startTimestamp.get();
    }
}
