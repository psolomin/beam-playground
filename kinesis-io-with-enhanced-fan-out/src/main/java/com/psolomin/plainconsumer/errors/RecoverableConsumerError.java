package com.psolomin.plainconsumer.errors;

public class RecoverableConsumerError extends ConsumerError {
    public RecoverableConsumerError(Throwable cause) {
        super(cause);
    }
}
