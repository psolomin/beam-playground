package com.psolomin.plainconsumer.signals;

public class SubscriptionCompleteShardEvent extends ShardEventAbs {
    private SubscriptionCompleteShardEvent(String shardId) {
        super(shardId, ShardEventType.SUBSCRIPTION_COMPLETE);
    }

    public static SubscriptionCompleteShardEvent create(String shardId) {
        return new SubscriptionCompleteShardEvent(shardId);
    }
}
