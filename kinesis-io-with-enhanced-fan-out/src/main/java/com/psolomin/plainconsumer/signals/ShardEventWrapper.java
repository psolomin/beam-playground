package com.psolomin.plainconsumer.signals;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

public class ShardEventWrapper {
    private final String shardId;
    private final ShardEventType type;
    private final @Nullable SubscribeToShardEvent event;
    private final @Nullable Throwable err;

    private ShardEventWrapper(
            String shardId, ShardEventType type, @Nullable SubscribeToShardEvent event, @Nullable Throwable err) {
        this.shardId = shardId;
        this.type = type;
        this.event = event;
        this.err = err;
    }

    public static ShardEventWrapper fromNext(String shardId, SubscribeToShardEvent event) {
        if (isReShard(event)) {
            return new ShardEventWrapper(shardId, ShardEventType.RE_SHARD, event, null);
        } else if (isEventWithRecords(event)) {
            return new ShardEventWrapper(shardId, ShardEventType.RECORDS, event, null);
        } else {
            throw new IllegalStateException(String.format("Unknown event type, no scenario: %s", event));
        }
    }

    public static ShardEventWrapper subscriptionComplete(String shardId) {
        return new ShardEventWrapper(shardId, ShardEventType.SUBSCRIPTION_COMPLETE, null, null);
    }

    public static ShardEventWrapper error(String shardId, Throwable err) {
        return new ShardEventWrapper(shardId, ShardEventType.ERROR, null, err);
    }

    public ShardEventType type() {
        return type;
    }

    public SubscribeToShardEvent getWrappedEvent() {
        if (type.equals(ShardEventType.RECORDS) && event != null) {
            return event;
        } else if (type.equals(ShardEventType.RE_SHARD) && event != null) {
            return event;
        } else {
            throw new IllegalStateException("Invalid");
        }
    }

    public Throwable getError() {
        if (type.equals(ShardEventType.ERROR) && err != null) {
            return err;
        } else {
            throw new IllegalStateException("Invalid");
        }
    }

    public String getShardId() {
        return shardId;
    }

    private static boolean isReShard(SubscribeToShardEvent event) {
        return event.continuationSequenceNumber() == null && event.hasChildShards();
    }

    private static boolean isEventWithRecords(SubscribeToShardEvent event) {
        return event.continuationSequenceNumber() != null && event.hasRecords();
    }

    @Override
    public String toString() {
        return "ShardEventWrapper{" + "shardId='"
                + shardId + '\'' + ", type="
                + type + ", event="
                + event + ", err="
                + err + '}';
    }
}
