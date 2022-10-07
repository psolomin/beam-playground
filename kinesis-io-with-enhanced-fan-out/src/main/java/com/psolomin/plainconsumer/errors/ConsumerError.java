package com.psolomin.plainconsumer.errors;

import io.netty.handler.timeout.ReadTimeoutException;

public class ConsumerError extends Exception {
    public ConsumerError(Throwable cause) {
        super(cause);
    }

    public static ConsumerError toConsumerError(Throwable e) {
        if (e instanceof ReadTimeoutException) {
            return new RecoverableConsumerError(e);
        } else {
            return new CriticalConsumerError(e);
        }
    }
}
