package com.psolomin.plainconsumer;

public class ClientBuilderImpl implements ClientBuilder {
    @Override
    public SimplifiedKinesisAsyncClient build() {
        return new SimplifiedKinesisAsyncClientImpl();
    }
}
