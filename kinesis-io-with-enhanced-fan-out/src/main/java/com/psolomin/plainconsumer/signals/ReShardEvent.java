package com.psolomin.plainconsumer.signals;

import java.util.List;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

public class ReShardEvent extends ShardEventAbs {
    private final List<ChildShard> childShards;

    private ReShardEvent(String shardId, List<ChildShard> childShards) {
        super(shardId, ShardEventType.RE_SHARD);
        this.childShards = childShards;
    }

    public static boolean isReShard(SubscribeToShardEvent event) {
        return event.continuationSequenceNumber() == null && event.hasChildShards();
    }

    public static ReShardEvent fromNext(String shardId, SubscribeToShardEvent event) {
        return new ReShardEvent(shardId, event.childShards());
    }

    public List<ChildShard> getChildShards() {
        return childShards;
    }
}
