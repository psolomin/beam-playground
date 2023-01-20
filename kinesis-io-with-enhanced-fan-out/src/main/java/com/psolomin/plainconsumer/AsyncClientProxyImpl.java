package com.psolomin.plainconsumer;

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public class AsyncClientProxyImpl implements AsyncClientProxy {
    private final KinesisAsyncClient client;

    public AsyncClientProxyImpl(KinesisAsyncClient client) {
        this.client = client;
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest request) {
        return client.listShards(request);
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(
            SubscribeToShardRequest request, SubscribeToShardResponseHandler handler) {
        return client.subscribeToShard(request, handler);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
