package com.psolomin.plainconsumer.signals;

public interface ShardEvent {
    String getShardId();

    ShardEventType getType();
}
