package com.psolomin.plainconsumer;

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public interface SimplifiedKinesisAsyncClient extends AutoCloseable {
    CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest);

    CompletableFuture<Void> subscribeToShard(
            SubscribeToShardRequest request, SubscribeToShardResponseHandler responseHandler);
}
