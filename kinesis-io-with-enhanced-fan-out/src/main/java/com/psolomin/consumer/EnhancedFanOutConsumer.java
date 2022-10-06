package com.psolomin.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public class EnhancedFanOutConsumer {
    private static final String STREAM_NAME = "stream-01";
    private static final String CONSUMER_ARN =
            "arn:aws:kinesis:eu-west-1:790288347884:stream/stream-01/consumer/consumer-01:1665080534";

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {

        KinesisAsyncClient client = KinesisAsyncClient.create();
        ListShardsRequest listShardsRequest = ListShardsRequest
                .builder().streamName(STREAM_NAME)
                .build();

        List<String> shardIds = new ArrayList<>();
        ListShardsResponse response = client.listShards(listShardsRequest).get(5000, TimeUnit.MILLISECONDS);
        response.shards().stream().map(Shard::shardId).forEach(shardIds::add);

        SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                .consumerARN(CONSUMER_ARN)
                .shardId(shardIds.get(0))
                .startingPosition(s -> s.type(ShardIteratorType.LATEST)).build();

        // Call SubscribeToShard iteratively to renew the subscription periodically.
        while(true) {
            // Wait for the CompletableFuture to complete normally or exceptionally.
            callSubscribeToShardWithVisitor(client, request).join();
        }

        // Close the connection before exiting.
        // client.close();
    }


    /**
     * Subscribes to the stream of events by implementing the SubscribeToShardResponseHandler.Visitor interface.
     */
    private static CompletableFuture<Void> callSubscribeToShardWithVisitor(
            KinesisAsyncClient client,
            SubscribeToShardRequest request
    ) {
        SubscribeToShardResponseHandler.Visitor visitor = new SubscribeToShardResponseHandler.Visitor() {
            @Override
            public void visit(SubscribeToShardEvent event) {
                event.records();
                System.out.println("Received subscribe to shard event " + event);
            }
        };

        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler
                .builder()
                .onError(t -> System.err.println("Error during stream - " + t.getMessage()))
                .subscriber(visitor)
                .build();

        return client.subscribeToShard(request, responseHandler);
    }
}
