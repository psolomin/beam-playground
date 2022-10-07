package com.psolomin.plainconsumer;

import static com.psolomin.plainconsumer.ShardsProgressTracker.getShardsAfterParent;

import com.psolomin.plainconsumer.sink.RecordsSink;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ChildShard;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;

public class StreamConsumer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamConsumer.class);
    private static final String LOG_MSG_TEMPLATE = "Stream = {} consumer = {}";

    private static final Long signalsOfferTimeoutMs = 1_000L;
    private static final Long signalsPollTimeoutMs = 10_000L;
    private static final int awaitTerminationTimeoutMs = 30_000;

    private final Config config;
    private final ClientBuilder clientBuilder;
    private final ExecutorService executorService;
    private final ShardsProgressTracker progressTracker;
    private Map<String, ShardEventsConsumer> consumers = new HashMap<>();
    private final BlockingQueue<ReShardSignal> signals = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final RecordsSink recordsSink;

    public StreamConsumer(Config config, ClientBuilder clientBuilder, RecordsSink recordsSink) {
        LOG.info(LOG_MSG_TEMPLATE + " Creating pool", config.getStreamName(), config.getConsumerArn());
        this.config = config;
        this.clientBuilder = clientBuilder;
        this.executorService = createThreadPool(config);
        this.progressTracker = ShardsProgressTracker.initSubscribedShardsProgressInfo(config, clientBuilder);
        this.recordsSink = recordsSink;
    }

    public static StreamConsumer init(Config config, ClientBuilder clientBuilder, RecordsSink recordsSink) {
        String coordinatorName = String.format(
                "shard-consumers-pool-coordinator-%s-%s", config.getStreamName(), config.getConsumerArn());
        StreamConsumer streamConsumer = new StreamConsumer(config, clientBuilder, recordsSink);
        Thread t = new Thread(streamConsumer, coordinatorName);
        t.setDaemon(true);
        t.start();
        return streamConsumer;
    }

    public void initiateGracefulShutdown() {
        LOG.info(LOG_MSG_TEMPLATE + " Initiating shutdown", config.getStreamName(), config.getConsumerArn());
        isRunning.set(false);
        try {
            try {
                cleanUpResources();
            } catch (Exception e) {
                LOG.warn(
                        LOG_MSG_TEMPLATE + " Error while cleaning up {}",
                        config.getStreamName(),
                        config.getConsumerArn(),
                        e);
            }
        } finally {
            executorService.shutdown();
        }

        LOG.info(LOG_MSG_TEMPLATE + " Shutdown complete", config.getStreamName(), config.getConsumerArn());
    }

    public void awaitTermination() throws InterruptedException {
        if (!executorService.awaitTermination(awaitTerminationTimeoutMs, TimeUnit.MILLISECONDS)) {
            LOG.warn("Unable to gracefully shut down");
        }
    }

    private void cleanUpResources() {
        consumers.forEach((k, v) -> v.initiateGracefulShutdown());
    }

    private ExecutorService createThreadPool(Config config) {
        return Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicLong threadCount = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable runnable) {
                String name = String.format(
                        "shard-consumer-%s-%s-%s",
                        threadCount.getAndIncrement(), config.getStreamName(), config.getConsumerArn());
                Thread thread = new Thread(runnable, name);
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    void handleReShard(String shardId, ShardEvent event) {
        SubscribeToShardEvent wrappedEvent = event.getWrappedEvent();
        LOG.info("Re-shard event from {}. New shards: {}.", shardId, wrappedEvent.childShards());
        try {
            if (!signals.offer(
                    ReShardSignal.fromShardEvent(shardId, event), signalsOfferTimeoutMs, TimeUnit.MILLISECONDS)) {
                LOG.warn(
                        "Re-shard event from {} was not pushed to the queue, timeout after {}ms",
                        shardId,
                        signalsOfferTimeoutMs);
            }
        } catch (InterruptedException e) {
            LOG.warn("Re-shard event from {} was not pushed to the queue, {}", shardId, e);
        }
    }

    @Override
    public void run() {
        submitShardConsumersTasks();
        isRunning.set(true);
        LOG.info(LOG_MSG_TEMPLATE + " Started", config.getStreamName(), config.getConsumerArn());

        while (isRunning.get()) {
            try {
                ReShardSignal reShardSignal = signals.poll(signalsPollTimeoutMs, TimeUnit.MILLISECONDS);
                if (reShardSignal != null) {
                    processReShardSignal(reShardSignal);
                } else {
                    LOG.info("No re-shard events to handle. Consumers cnt = {}", consumers.size());
                }
            } catch (InterruptedException e) {
                LOG.warn("Failed to take re-shard signal from queue. ", e);
            }
        }
    }

    private void submitShardConsumersTasks() {
        Map<String, ShardEventsConsumer> shardConsumerMap = new HashMap<>();
        progressTracker
                .shardsProgress()
                .forEach((k, v) -> shardConsumerMap.put(
                        k, ShardEventsConsumer.fromShardProgress(this, config, clientBuilder, recordsSink, v)));
        consumers = shardConsumerMap;
        consumers.values().forEach(executorService::submit);
    }

    private void processReShardSignal(ReShardSignal reShardSignal) throws InterruptedException {
        LOG.info("Processing re-shard signal {}", reShardSignal);
        String receivedFromShardId = reShardSignal.getSenderId();
        List<ChildShard> childShards = reShardSignal.getChildShards();
        List<Shard> newShards = waitForNewShards(receivedFromShardId, childShards);

        if (consumers.containsKey(receivedFromShardId)) {
            LOG.info("Stopping {} upon signal {}", receivedFromShardId, reShardSignal);
            ShardEventsConsumer oldConsumer = consumers.get(receivedFromShardId);
            oldConsumer.initiateGracefulShutdown();
            consumers.remove(receivedFromShardId);
        }

        newShards.forEach(newShard -> {
            String id = newShard.shardId();
            if (!consumers.containsKey(id)) {
                LOG.info("Starting {} upon signal {}", id, reShardSignal);
                String beginningSeqNumber = newShard.sequenceNumberRange().startingSequenceNumber();
                ShardProgress progress = new ShardProgress(config, id, beginningSeqNumber);
                progressTracker.addShard(id, progress);

                ShardEventsConsumer newConsumer =
                        ShardEventsConsumer.fromShardProgress(this, config, clientBuilder, recordsSink, progress);
                consumers.put(id, newConsumer);
                executorService.submit(newConsumer);
            }
        });
    }

    private List<Shard> waitForNewShards(String receivedFromShardId, List<ChildShard> expectedShards)
            throws InterruptedException {
        int maxAttempts = 3;
        int waitBetweenAttemptsMs = 1_000;
        int currentAttempt = 1;

        Set<String> expectedShardsIds =
                expectedShards.stream().map(ChildShard::shardId).collect(Collectors.toSet());
        while (currentAttempt <= maxAttempts) {
            List<Shard> newShards = getShardsAfterParent(receivedFromShardId, config, clientBuilder);
            Set<String> newShardsIds = newShards.stream().map(Shard::shardId).collect(Collectors.toSet());
            expectedShardsIds.removeAll(newShardsIds);
            if (expectedShardsIds.size() == 0) return newShards;
            else {
                Thread.sleep(waitBetweenAttemptsMs);
            }
            currentAttempt++;
        }

        String msg =
                String.format("After %s attempts still not found shard info for %s", currentAttempt, expectedShardsIds);
        throw new RuntimeException(msg);
    }
}
