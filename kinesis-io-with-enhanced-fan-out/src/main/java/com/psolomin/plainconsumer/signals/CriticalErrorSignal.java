package com.psolomin.plainconsumer.signals;

import com.psolomin.plainconsumer.ShardEvent;

public class CriticalErrorSignal implements ShardSubscriberSignal {
    private final String senderId;
    private final Throwable error;

    CriticalErrorSignal(String senderId, Throwable error) {
        this.senderId = senderId;
        this.error = error;
    }

    public static CriticalErrorSignal fromError(String senderId, ShardEvent event) {
        return new CriticalErrorSignal(senderId, event.getError());
    }

    public String getSenderId() {
        return senderId;
    }

    public Throwable getError() {
        return error;
    }

    @Override
    public String toString() {
        return "ReShardSignal{" + "senderId='" + senderId + '\'' + ", childShards=" + error.getCause() + '}';
    }
}
