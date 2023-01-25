package com.psolomin.plainconsumer.signals;

public abstract class ShardEventAbs implements ShardEvent {
    private final String shardId;
    private final ShardEventType type;

    protected ShardEventAbs(String shardId, ShardEventType type) {
        this.shardId = shardId;
        this.type = type;
    }

    @Override
    public String getShardId() {
        return shardId;
    }

    @Override
    public ShardEventType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ShardEvent{" + "shardId='" + shardId + '\'' + ", type=" + type + '}';
    }
}
