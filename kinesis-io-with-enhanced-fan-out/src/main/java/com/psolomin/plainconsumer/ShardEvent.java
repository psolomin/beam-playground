package com.psolomin.plainconsumer;

import java.util.Optional;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

class ShardEvent {
    private final ShardEventType type;
    private final Optional<SubscribeToShardEvent> event;
    private final Optional<Throwable> err;

    private ShardEvent(ShardEventType type, Optional<SubscribeToShardEvent> event, Optional<Throwable> err) {
        this.type = type;
        this.event = event;
        this.err = err;
    }

    static ShardEvent fromNext(SubscribeToShardEvent event) {
        if (isReShard(event)) {
            return new ShardEvent(ShardEventType.RE_SHARD, Optional.of(event), Optional.empty());
        } else if (isEventWithRecords(event)) {
            return new ShardEvent(ShardEventType.RECORDS, Optional.of(event), Optional.empty());
        } else {
            throw new IllegalStateException(String.format("Unknown event type, no scenario: %s", event));
        }
    }

    static ShardEvent subscriptionComplete() {
        return new ShardEvent(ShardEventType.SUBSCRIPTION_COMPLETE, Optional.empty(), Optional.empty());
    }

    static ShardEvent error(Throwable err) {
        return new ShardEvent(ShardEventType.ERROR, Optional.empty(), Optional.of(err));
    }

    ShardEventType type() {
        return type;
    }

    SubscribeToShardEvent getWrappedEvent() {
        if (type.equals(ShardEventType.RECORDS) && event.isPresent()) {
            return event.get();
        } else if (type.equals(ShardEventType.RE_SHARD) && event.isPresent()) {
            return event.get();
        } else {
            throw new IllegalStateException("Invalid");
        }
    }

    Throwable getError() {
        if (type.equals(ShardEventType.ERROR) && err.isPresent()) {
            return err.get();
        } else {
            throw new IllegalStateException("Invalid");
        }
    }

    @Override
    public String toString() {
        return "ShardEvent{" + "type=" + type + ", event=" + event + ", err=" + err + '}';
    }

    private static boolean isReShard(SubscribeToShardEvent event) {
        return event.continuationSequenceNumber() == null && event.hasChildShards();
    }

    private static boolean isEventWithRecords(SubscribeToShardEvent event) {
        return event.continuationSequenceNumber() != null && event.hasRecords();
    }
}
