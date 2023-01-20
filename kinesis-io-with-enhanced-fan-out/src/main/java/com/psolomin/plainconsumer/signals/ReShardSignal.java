package com.psolomin.plainconsumer.signals;

import java.util.List;
import software.amazon.awssdk.services.kinesis.model.ChildShard;

public class ReShardSignal implements ShardSubscriberSignal {
    private final String senderId;
    private final String continuationSequenceNumber;
    private final List<ChildShard> childShards;

    ReShardSignal(String senderId, String continuationSequenceNumber, List<ChildShard> childShards) {
        this.senderId = senderId;
        this.continuationSequenceNumber = continuationSequenceNumber;
        this.childShards = childShards;
    }

    public static ReShardSignal fromShardEvent(ShardEventWrapper event) {
        return new ReShardSignal(
                event.getShardId(),
                event.getWrappedEvent().continuationSequenceNumber(),
                event.getWrappedEvent().childShards());
    }

    @Override
    public String getSenderId() {
        return senderId;
    }

    @Override
    public String toString() {
        return "ReShardSignal{" + "senderId='" + senderId + '\'' + ", childShards=" + childShards + '}';
    }

    public String getContinuationSequenceNumber() {
        return continuationSequenceNumber;
    }

    public List<ChildShard> getChildShards() {
        return childShards;
    }
}
