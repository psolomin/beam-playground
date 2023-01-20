package com.psolomin.plainconsumer;

import com.psolomin.plainconsumer.signals.ReShardSignal;
import com.psolomin.plainconsumer.signals.ShardEventWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

public class ShardSubscribersPoolImpl {
    private static final Logger LOG = LoggerFactory.getLogger(ShardSubscribersPoolImpl.class);
    private static final int BUFFER_SIZE = 10_000;
    private static final long INITIAL_DEMAND_PER_SHARD = 5L;
    private static final long BUFFER_POLL_WAIT_MS = 1_000L;
    private static final long ENQUEUE_TIMEOUT_MS = 3_000;

    private final Config config;
    private final AsyncClientProxy kinesis;
    private final List<String> shardsIds;
    private final ConcurrentMap<String, KinesisShardEventsSubscriber> subscribers;
    private final ConcurrentMap<String, CompletableFuture<Void>> subscriptionFutures;
    private final BlockingQueue<ShardEventWrapper> eventsBuffer;

    public ShardSubscribersPoolImpl(Config config, AsyncClientProxy kinesis, List<String> initialShardsIds) {
        this.config = config;
        this.kinesis = kinesis;
        this.shardsIds = initialShardsIds;
        this.subscribers = new ConcurrentHashMap<>();
        this.subscriptionFutures = new ConcurrentHashMap<>();
        this.eventsBuffer = new LinkedBlockingQueue<>(BUFFER_SIZE);
    }

    public boolean start() {
        shardsIds.forEach(shardId -> subscriptionFutures.put(
                shardId,
                createSubscription(
                        shardId,
                        StartingPosition.builder()
                                .type(ShardIteratorType.LATEST)
                                .build())));
        return true;
    }

    public boolean stop() {
        subscribers.values().forEach(KinesisShardEventsSubscriber::cancel);
        subscriptionFutures.values().forEach(f -> f.cancel(false));
        return true;
    }

    /**
     * This is ultimately called by Netty threads via {@link KinesisShardEventsSubscriber} callbacks.
     *
     * @param event
     * @throws InterruptedException
     */
    public void handleEvent(ShardEventWrapper event) throws InterruptedException {
        switch (event.type()) {
            case SUBSCRIPTION_COMPLETE:
                CompletableFuture.runAsync(() -> subscriptionFutures.computeIfPresent(
                        event.getShardId(),
                        (k, v) -> createSubscription(
                                k,
                                StartingPosition.builder()
                                        .type(ShardIteratorType.LATEST)
                                        .build())));
                break;
            case RE_SHARD:
                CompletableFuture.runAsync(() -> {
                    ReShardSignal reShardSignal = ReShardSignal.fromShardEvent(event);
                    List<String> successorShards = computeSuccessorShardsIds(reShardSignal);
                    successorShards.forEach(s -> subscriptionFutures.computeIfAbsent(
                            s,
                            k -> createSubscription(
                                    k,
                                    StartingPosition.builder()
                                            .type(ShardIteratorType.LATEST)
                                            .build())));
                });
                break;
            case RECORDS:
                if (!eventsBuffer.offer(event, ENQUEUE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("This should never happen");
                }
                break;
            default:
                LOG.warn("Unknown event type {}", event.type());
        }
    }

    private static List<String> computeSuccessorShardsIds(ReShardSignal reShardSignal) {
        // FIXME: If future completes with unrecoverable error, entire reader should fail
        // FIXME: Down-sharding can cause shards to be "lost"! Re-visit this logic!
        LOG.info("Processing re-shard signal {}", reShardSignal);

        List<String> successorShardsIds = new ArrayList<>();

        for (ChildShard childShard : reShardSignal.getChildShards()) {
            if (childShard.parentShards().contains(reShardSignal.getSenderId())) {
                if (childShard.parentShards().size() > 1) {
                    // This is the case of merging two shards into one.
                    // when there are 2 parent shards, we only pick it up if
                    // its max shard equals to sender shard ID
                    String maxId = childShard.parentShards().stream()
                            .max(String::compareTo)
                            .get();
                    if (reShardSignal.getSenderId().equals(maxId)) {
                        successorShardsIds.add(childShard.shardId());
                    }
                } else {
                    // This is the case when shard is split
                    successorShardsIds.add(childShard.shardId());
                }
            }
        }

        if (successorShardsIds.isEmpty()) {
            LOG.info("Found no successors for shard {}", reShardSignal.getSenderId());
        } else {
            LOG.info("Found successors for shard {}: {}", reShardSignal.getSenderId(), successorShardsIds);
        }
        return successorShardsIds;
    }

    private CompletableFuture<Void> createSubscription(String shardId, StartingPosition startingPosition) {
        UUID subscribeRequestId = UUID.randomUUID();
        SubscribeToShardRequest request = SubscribeToShardRequest.builder()
                .consumerARN(config.getConsumerArn())
                .shardId(shardId)
                .startingPosition(startingPosition)
                .build();

        CountDownLatch eventsHandlerReadyLatch = new CountDownLatch(1);

        KinesisShardEventsSubscriber shardEventsSubscriber = new KinesisShardEventsSubscriber(
                this, eventsHandlerReadyLatch, config.getStreamName(), config.getConsumerArn(), shardId);

        SubscribeToShardResponseHandler responseHandler = SubscribeToShardResponseHandler.builder()
                .onError(e -> LOG.error("Failed to execute subscribe request {} - {}", subscribeRequestId, request, e))
                .subscriber(() -> shardEventsSubscriber)
                .build();

        CompletableFuture<Void> f = kinesis.subscribeToShard(request, responseHandler);
        boolean subscriptionWasEstablished = false;
        try {
            subscriptionWasEstablished =
                    eventsHandlerReadyLatch.await(config.getPoolStartTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // handled below
        }

        if (!subscriptionWasEstablished) {
            LOG.error("Subscribe request {} failed.", subscribeRequestId);
            if (!f.isCompletedExceptionally()) {
                LOG.warn("subscribeToShard request {} failed, but future was not complete.", subscribeRequestId);
            }
            shardEventsSubscriber.cancel();
            throw new RuntimeException();
        }

        subscribers.put(shardId, shardEventsSubscriber);
        shardEventsSubscriber.requestRecords(INITIAL_DEMAND_PER_SHARD);
        LOG.info("Subscription for shard {} established", shardId);
        return f;
    }

    public Optional<ShardEventWrapper> nextRecord() {
        try {
            ShardEventWrapper maybeEvent = eventsBuffer.poll(BUFFER_POLL_WAIT_MS, TimeUnit.MILLISECONDS);
            if (maybeEvent != null) {
                subscribers.get(maybeEvent.getShardId()).requestRecords(1L);
                return Optional.of(maybeEvent);
            } else return Optional.empty();
        } catch (InterruptedException e) {
            return Optional.empty();
        }
    }
}
