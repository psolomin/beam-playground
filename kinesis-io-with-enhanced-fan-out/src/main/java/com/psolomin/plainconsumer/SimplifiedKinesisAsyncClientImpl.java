package com.psolomin.plainconsumer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient.Builder;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public class SimplifiedKinesisAsyncClientImpl implements SimplifiedKinesisAsyncClient {
    private final KinesisAsyncClient client;

    public SimplifiedKinesisAsyncClientImpl() {
        RetryPolicy retryPolicy = RetryPolicy.builder()
                .backoffStrategy(BackoffStrategy.defaultStrategy())
                .throttlingBackoffStrategy(BackoffStrategy.defaultThrottlingStrategy())
                .retryCondition(RetryCondition.defaultRetryCondition())
                .numRetries(10)
                .build();

        ClientOverrideConfiguration conf = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .apiCallTimeout(Duration.ofSeconds(6 * 60)) // must be > 5 minutes
                .build();

        Builder<NettyNioAsyncHttpClient.Builder> customHttpBuilder = NettyNioAsyncHttpClient.builder()
                .connectionMaxIdleTime(Duration.ofSeconds(180))
                .connectionAcquisitionTimeout(Duration.ofSeconds(60))
                .connectionTimeout(Duration.ofSeconds(120))
                .maxConcurrency(20)
                .maxPendingConnectionAcquires(120);

        client = KinesisAsyncClient.builder()
                .httpClientBuilder(customHttpBuilder)
                .overrideConfiguration(conf)
                .build();
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
