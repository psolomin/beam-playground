package com.psolomin.plainconsumer.signals;

public class ErrorShardEvent extends ShardEventAbs {
    private final Throwable err;

    private ErrorShardEvent(String shardId, Throwable err) {
        super(shardId, ShardEventType.ERROR);
        this.err = err;
    }

    public static ErrorShardEvent fromErr(String shardId, Throwable err) {
        return new ErrorShardEvent(shardId, err);
    }

    public Throwable getErr() {
        return err;
    }
}
