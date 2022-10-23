package com.psolomin.plainconsumer.signals;

import com.psolomin.plainconsumer.ShardEvent;
import java.util.List;
import software.amazon.awssdk.services.kinesis.model.ChildShard;

public class ReShardSignal implements ShardSubscriberSignal {
    private final String senderId;
    private final List<ChildShard> childShards;

    ReShardSignal(String senderId, List<ChildShard> childShards) {
        this.senderId = senderId;
        this.childShards = childShards;
    }

    public static ReShardSignal fromShardEvent(String senderId, ShardEvent event) {
        return new ReShardSignal(senderId, event.getWrappedEvent().childShards());
    }

    public String getSenderId() {
        return senderId;
    }

    public List<ChildShard> getChildShards() {
        return childShards;
    }

    @Override
    public String toString() {
        return "ReShardSignal{" + "senderId='" + senderId + '\'' + ", childShards=" + childShards + '}';
    }
}
