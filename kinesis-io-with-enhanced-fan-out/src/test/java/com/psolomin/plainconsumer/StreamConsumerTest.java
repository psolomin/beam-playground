package com.psolomin.plainconsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.psolomin.helpers.KinesisClientBuilderStub;
import com.psolomin.helpers.SimplifiedKinesisAsyncClientStubBehaviours;
import com.psolomin.plainconsumer.sink.InMemCollectionRecordsSink;
import java.util.List;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StartingPosition;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardRequest;

@RunWith(JUnit4.class)
public class StreamConsumerTest {
    private static final String STREAM_NAME = SimplifiedKinesisAsyncClientStubBehaviours.STREAM_NAME;
    private static final String CONSUMER_ARN = SimplifiedKinesisAsyncClientStubBehaviours.CONSUMER_ARN;
    private StreamConsumer consumer;

    @After
    public void tearDown() throws InterruptedException {
        consumer.initiateGracefulShutdown();
        consumer.awaitTermination();
        assertFalse(consumer.isRunning());
    }

    @Test
    public void consumesAllEventsFromMultipleShards() throws InterruptedException {
        Config config = new Config(STREAM_NAME, CONSUMER_ARN, StartType.LATEST);
        KinesisClientBuilderStub builder = SimplifiedKinesisAsyncClientStubBehaviours.twoShardsWithRecords();

        InMemCollectionRecordsSink sink = new InMemCollectionRecordsSink();
        consumer = StreamConsumer.init(config, builder, sink);
        Thread.sleep(1_000L);
        int expectedRecordsCntPerShard = 6;
        checkEventsCnt(expectedRecordsCntPerShard, List.of("shard-000", "shard-001"), List.of(), sink);
        // 2 shards x (1 initial subscribe + 2 re-subscribes)
        assertEquals(6, builder.subscribeRequestsSeen().size());
        List<SubscribeToShardRequest> expectedSubscribeRequests = List.of(
                subscribeLatest("shard-000"),
                subscribeLatest("shard-001"),
                subscribeSeqNumber("shard-000", "2"),
                subscribeSeqNumber("shard-001", "2"),
                subscribeSeqNumber("shard-000", "2"),
                subscribeSeqNumber("shard-001", "2"));
        assertTrue(expectedSubscribeRequests.containsAll(builder.subscribeRequestsSeen()));
        assertTrue(consumer.isRunning());
    }

    @Test
    public void consumesAllEventsFromChildShards() throws InterruptedException {
        Config config = new Config(STREAM_NAME, CONSUMER_ARN, StartType.LATEST);
        KinesisClientBuilderStub builder = SimplifiedKinesisAsyncClientStubBehaviours.twoShardsWithRecordsAndShardUp();

        InMemCollectionRecordsSink sink = new InMemCollectionRecordsSink();
        consumer = StreamConsumer.init(config, builder, sink);
        Thread.sleep(1_000L);
        int expectedRecordsCntPerShard = 7;
        List<String> parentShards = List.of("shard-000", "shard-001");
        List<String> childShards = List.of("shard-002", "shard-003", "shard-004", "shard-005");
        checkEventsCnt(expectedRecordsCntPerShard, parentShards, childShards, sink);
        // 2 shards with initial subscribe + 4 new shards with initial subscribe
        assertEquals(6, builder.subscribeRequestsSeen().size());
        List<SubscribeToShardRequest> expectedSubscribeRequests = List.of(
                subscribeLatest("shard-000"),
                subscribeLatest("shard-001"),
                subscribeSeqNumber("shard-002", "002"),
                subscribeSeqNumber("shard-003", "003"),
                subscribeSeqNumber("shard-004", "004"),
                subscribeSeqNumber("shard-005", "005"));
        assertTrue(expectedSubscribeRequests.containsAll(builder.subscribeRequestsSeen()));
        assertTrue(consumer.isRunning());
    }

    @Test
    public void consumesAllEventsFromChildShardsAfterMerge() throws InterruptedException {
        Config config = new Config(STREAM_NAME, CONSUMER_ARN, StartType.LATEST);
        KinesisClientBuilderStub builder =
                SimplifiedKinesisAsyncClientStubBehaviours.fourShardsWithRecordsAndShardDown();

        InMemCollectionRecordsSink sink = new InMemCollectionRecordsSink();
        consumer = StreamConsumer.init(config, builder, sink);
        Thread.sleep(1_000L);
        int expectedRecordsCntPerShard = 11;
        List<String> parentShards = List.of("shard-000", "shard-001", "shard-002", "shard-003");
        List<String> childShards = List.of("shard-004", "shard-005", "shard-006");
        checkEventsCnt(expectedRecordsCntPerShard, parentShards, childShards, sink);
        // 4 shards with initial subscribe + 3 new shards with initial subscribe
        int expectedSubscribeRequests = 7;
        assertEquals(expectedSubscribeRequests, builder.subscribeRequestsSeen().size());
        assertTrue(consumer.isRunning());
    }

    @Test
    public void consumersNoEventsFromEmptyShards() throws InterruptedException {
        Config config = new Config(STREAM_NAME, CONSUMER_ARN, StartType.LATEST);
        KinesisClientBuilderStub builder = SimplifiedKinesisAsyncClientStubBehaviours.twoShardsEmpty();

        InMemCollectionRecordsSink sink = new InMemCollectionRecordsSink();
        consumer = StreamConsumer.init(config, builder, sink);
        Thread.sleep(1_000L);
        assertTrue(sink.shardsSeen().isEmpty());
        // 2 shards x (1 initial subscribe + 2 re-subscribes)
        int expectedSubscribeRequests = 6;
        assertEquals(expectedSubscribeRequests, builder.subscribeRequestsSeen().size());
        assertTrue(consumer.isRunning());
    }

    @Test
    public void stopsUponUnRecoverableError() throws InterruptedException {
        Config config = new Config(STREAM_NAME, CONSUMER_ARN, StartType.LATEST);
        KinesisClientBuilderStub builder =
                SimplifiedKinesisAsyncClientStubBehaviours.twoShardsWithRecordsOneShardError();

        InMemCollectionRecordsSink sink = new InMemCollectionRecordsSink();
        consumer = StreamConsumer.init(config, builder, sink);
        Thread.sleep(1_000L);
        assertFalse(consumer.isRunning());
    }

    @Test
    public void continuesUponRecoverableError() throws InterruptedException {
        Config config = new Config(STREAM_NAME, CONSUMER_ARN, StartType.LATEST);
        KinesisClientBuilderStub builder =
                SimplifiedKinesisAsyncClientStubBehaviours.twoShardsWithRecordsOneShardRecoverableError();

        InMemCollectionRecordsSink sink = new InMemCollectionRecordsSink();
        consumer = StreamConsumer.init(config, builder, sink);
        Thread.sleep(1_000L);
        int expectedRecordsCntPerShard = 10;
        checkEventsCnt(expectedRecordsCntPerShard, List.of("shard-000", "shard-001"), List.of(), sink);
        // 2 shards x (1 initial subscribe + 2 re-subscribes)
        int expectedSubscribeRequests = 6;
        assertEquals(expectedSubscribeRequests, builder.subscribeRequestsSeen().size());
        assertTrue(consumer.isRunning());
    }

    private static void checkEventsCnt(
            int expectedRecordsCntPerShard,
            List<String> parentShards,
            List<String> childShards,
            InMemCollectionRecordsSink sink) {
        Stream.of(parentShards, childShards)
                .flatMap(List::stream)
                .forEach(shardId -> assertEquals(
                        expectedRecordsCntPerShard, sink.getAllRecords(shardId).size()));
    }

    private static SubscribeToShardRequest subscribeLatest(String shardId) {
        return SubscribeToShardRequest.builder()
                .consumerARN("consumer-01")
                .shardId(shardId)
                .startingPosition(StartingPosition.builder()
                        .type(ShardIteratorType.LATEST)
                        .build())
                .build();
    }

    private static SubscribeToShardRequest subscribeSeqNumber(String shardId, String seqNumber) {
        return SubscribeToShardRequest.builder()
                .consumerARN("consumer-01")
                .shardId(shardId)
                .startingPosition(StartingPosition.builder()
                        .type(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                        .sequenceNumber(seqNumber)
                        .build())
                .build();
    }
}
