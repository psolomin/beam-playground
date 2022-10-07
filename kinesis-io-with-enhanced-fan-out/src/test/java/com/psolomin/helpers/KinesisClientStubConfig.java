package com.psolomin.helpers;

class KinesisClientStubConfig {
    private final Integer initialShardsCnt;
    private final Integer finalShardId;
    private final Integer subscriptionsPerShard;
    private final Integer recordsPerSubscriptionPerShardToSend;

    KinesisClientStubConfig(
            Integer initialShardsCnt,
            Integer finalShardId,
            Integer subscriptionsPerShard,
            Integer recordsPerSubscriptionPerShardToSend) {
        this.initialShardsCnt = initialShardsCnt;
        this.finalShardId = finalShardId;
        this.subscriptionsPerShard = subscriptionsPerShard;
        this.recordsPerSubscriptionPerShardToSend = recordsPerSubscriptionPerShardToSend;
    }

    public Integer getInitialShardsCnt() {
        return initialShardsCnt;
    }

    public Integer getFinalShardId() {
        return finalShardId;
    }

    public Integer getSubscriptionsPerShard() {
        return subscriptionsPerShard;
    }

    public Integer getRecordsPerSubscriptionPerShardToSend() {
        return recordsPerSubscriptionPerShardToSend;
    }
}
