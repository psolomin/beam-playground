package com.psolomin.helpers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.psolomin.plainconsumer.SimplifiedKinesisAsyncClient;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.SequenceNumberRange;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

class KinesisClientStub implements SimplifiedKinesisAsyncClient {
    private final KinesisClientStubConfig config;
    private final BlockingQueue<SubscribeToShardRequest> subscribeRequestsCollector;
    private final AtomicInteger seqNumber;
    private final AtomicInteger subscriptionsPerShardCountdown;
    private final Function<KinesisClientStubShardState, Void> eventsSubmitter;

    KinesisClientStub(
            KinesisClientStubConfig config,
            AtomicInteger subscriptionsPerShardCountdown,
            AtomicInteger seqNumber,
            BlockingQueue<SubscribeToShardRequest> subscribeRequestsCollector,
            Function<KinesisClientStubShardState, Void> eventsSubmitter) {
        this.config = config;
        this.seqNumber = seqNumber;
        this.subscriptionsPerShardCountdown = subscriptionsPerShardCountdown;
        this.subscribeRequestsCollector = subscribeRequestsCollector;
        this.eventsSubmitter = eventsSubmitter;
    }

    @Override
    public CompletableFuture<ListShardsResponse> listShards(ListShardsRequest listShardsRequest) {
        if (listShardsRequest.shardFilter().shardId() == null) {
            return CompletableFuture.completedFuture(ListShardsResponse.builder()
                    .shards(buildShards(0, config.getInitialShardsCnt()))
                    .build());
        } else {
            return CompletableFuture.completedFuture(ListShardsResponse.builder()
                    .shards(buildShards(config.getInitialShardsCnt(), config.getFinalShardId() + 1))
                    .build());
        }
    }

    @Override
    public CompletableFuture<Void> subscribeToShard(
            SubscribeToShardRequest request, SubscribeToShardResponseHandler responseHandler) {
        subscribeRequestsCollector.add(request);

        return CompletableFuture.supplyAsync(() -> buildEventsSupplier(
                request.shardId(),
                config.getRecordsPerSubscriptionPerShardToSend(),
                seqNumber,
                subscriptionsPerShardCountdown,
                responseHandler,
                eventsSubmitter));
    }

    @Override
    public void close() throws Exception {}

    public List<SubscribeToShardRequest> subscribeRequestsSeen() {
        return new ArrayList<>(subscribeRequestsCollector);
    }

    private static List<Shard> buildShards(int startId, int endId) {
        return IntStream.range(startId, endId)
                .mapToObj(i -> Shard.builder()
                        .shardId(String.format("shard-%03d", i))
                        .sequenceNumberRange(SequenceNumberRange.builder()
                                .startingSequenceNumber(String.format("%03d", i))
                                .build())
                        .build())
                .collect(Collectors.toList());
    }

    private static Void buildEventsSupplier(
            final String shardId,
            final Integer recordsPerShardToSend,
            final AtomicInteger globalSeqNumber,
            final AtomicInteger subscriptionsCountdown,
            final SubscribeToShardResponseHandler responseHandler,
            final Function<KinesisClientStubShardState, Void> eventsSubmitter) {
        responseHandler.responseReceived(SubscribeToShardResponse.builder().build());
        responseHandler.onEventStream(subscriber -> attachSubscriber(
                shardId, globalSeqNumber, recordsPerShardToSend, subscriptionsCountdown, subscriber, eventsSubmitter));

        return null;
    }

    private static void attachSubscriber(
            String shardId,
            AtomicInteger seqNumber,
            Integer recordsPerShardToSend,
            AtomicInteger subscriptionsCountdown,
            Subscriber<? super SubscribeToShardEventStream> subscriber,
            Function<KinesisClientStubShardState, Void> eventsSubmitter) {
        List<SubscribeToShardEvent> eventsToSend;
        if (subscriptionsCountdown.getAndDecrement() > 0) {
            eventsToSend = generateEvents(recordsPerShardToSend, seqNumber);
        } else {
            eventsToSend = List.of(SubscribeToShardEvent.builder()
                    .millisBehindLatest(0L)
                    .continuationSequenceNumber(null)
                    .build());
        }

        Subscription subscription = mock(Subscription.class);
        Iterator<SubscribeToShardEvent> iterator = eventsToSend.iterator();
        KinesisClientStubShardState state = new KinesisClientStubShardState(shardId, subscriber, iterator);
        doAnswer(a -> eventsSubmitter.apply(state)).when(subscription).request(1);
        subscriber.onSubscribe(subscription);
    }

    private static List<SubscribeToShardEvent> generateEvents(
            final Integer numberOfEvents, AtomicInteger sequenceNumber) {
        Stream<SubscribeToShardEvent> recordsWithData = IntStream.range(0, numberOfEvents)
                .mapToObj(i -> SubscribeToShardEvent.builder()
                        .records(createRecord(sequenceNumber))
                        .continuationSequenceNumber(String.valueOf(i))
                        .build());

        Stream<SubscribeToShardEvent> recordsWithOutData = IntStream.range(0, numberOfEvents)
                .mapToObj(i -> SubscribeToShardEvent.builder()
                        .records(List.of())
                        .continuationSequenceNumber(String.valueOf(i))
                        .build());

        return Stream.concat(recordsWithData, recordsWithOutData).collect(Collectors.toList());
    }

    private static Record createRecord(AtomicInteger sequenceNumber) {
        return Record.builder()
                .partitionKey("foo")
                .approximateArrivalTimestamp(Instant.now())
                .sequenceNumber(String.valueOf(sequenceNumber.incrementAndGet()))
                .data(SdkBytes.fromByteArray(sequenceNumber.toString().getBytes(UTF_8)))
                .build();
    }
}
