package com.psolomin.plainconsumer.signals;

public class RecoverableConsumerError extends ConsumerError {
    public RecoverableConsumerError(Throwable cause) {
        super(cause);
    }
}
