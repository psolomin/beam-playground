package com.psolomin.helpers;

import com.psolomin.plainconsumer.ClientBuilder;
import com.psolomin.plainconsumer.SimplifiedKinesisAsyncClient;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

public class KinesisClientBuilderStub implements ClientBuilder {
    private final KinesisClientStubConfig config;
    private final Function<KinesisClientStubShardState, Void> eventsSubmitter;
    private final BlockingQueue<SubscribeToShardRequest> subscribeRequestsSeenFromAllCreatedClients =
            new LinkedBlockingQueue<>(Integer.MAX_VALUE);
    private final AtomicInteger seqNumber = new AtomicInteger();

    KinesisClientBuilderStub(
            KinesisClientStubConfig config, Function<KinesisClientStubShardState, Void> eventsSubmitter) {
        this.config = config;
        this.eventsSubmitter = eventsSubmitter;
    }

    @Override
    public SimplifiedKinesisAsyncClient build() {
        return new KinesisClientStub(
                config,
                new AtomicInteger(config.getSubscriptionsPerShard()),
                seqNumber,
                subscribeRequestsSeenFromAllCreatedClients,
                eventsSubmitter);
    }

    public List<SubscribeToShardRequest> subscribeRequestsSeen() {
        return new ArrayList<>(subscribeRequestsSeenFromAllCreatedClients);
    }
}
