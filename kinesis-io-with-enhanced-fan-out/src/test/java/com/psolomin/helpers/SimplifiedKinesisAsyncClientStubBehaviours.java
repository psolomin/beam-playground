package com.psolomin.helpers;

public class SimplifiedKinesisAsyncClientStubBehaviours {
    public static final String STREAM_NAME = "stream-01";
    public static final String CONSUMER_ARN = "consumer-01";

    public static KinesisClientBuilderStub twoShardsWithRecords() {
        KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 2, 3);
        return new KinesisClientBuilderStub(config, KinesisClientStubShardState::submitEventsAndThenComplete);
    }

    public static KinesisClientBuilderStub twoShardsWithRecordsAndShardUp() {
        KinesisClientStubConfig config = new KinesisClientStubConfig(2, 5, 2, 7);
        return new KinesisClientBuilderStub(config, KinesisClientStubShardState::submitEventsAndThenShardUp);
    }

    public static KinesisClientBuilderStub fourShardsWithRecordsAndShardDown() {
        KinesisClientStubConfig config = new KinesisClientStubConfig(4, 6, 2, 11);
        return new KinesisClientBuilderStub(config, KinesisClientStubShardState::submitEventsAndThenShardDown);
    }

    public static KinesisClientBuilderStub twoShardsEmpty() {
        KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 2, 0);
        return new KinesisClientBuilderStub(config, KinesisClientStubShardState::submitEventsAndThenComplete);
    }

    public static KinesisClientBuilderStub twoShardsWithRecordsOneShardError() {
        KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 2, 5);
        return new KinesisClientBuilderStub(config, KinesisClientStubShardState::submitEventsAndThenSendError);
    }

    public static KinesisClientBuilderStub twoShardsWithRecordsOneShardRecoverableError() {
        KinesisClientStubConfig config = new KinesisClientStubConfig(2, 1, 2, 5);
        return new KinesisClientBuilderStub(
                config, KinesisClientStubShardState::submitEventsAndThenSendRecoverableError);
    }
}
