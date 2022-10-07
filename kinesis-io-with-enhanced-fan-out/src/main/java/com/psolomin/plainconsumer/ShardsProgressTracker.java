package com.psolomin.plainconsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

public class ShardsProgressTracker {
    private static final Logger LOG = LoggerFactory.getLogger(ShardsProgressTracker.class);

    private static final int shardListingTimeoutMs = 10_000;
    private final Map<String, ShardProgress> shardsProgress;

    private ShardsProgressTracker(Map<String, ShardProgress> shardsProgress) {
        this.shardsProgress = shardsProgress;
    }

    private static ShardFilter buildFilter(Config config) {
        switch (config.getStartType()) {
            case LATEST:
                return ShardFilter.builder().type(ShardFilterType.AT_LATEST).build();
            case AT_TIMESTAMP:
                return ShardFilter.builder()
                        .type(ShardFilterType.AT_TIMESTAMP)
                        .timestamp(config.getStartTimestamp())
                        .build();
            case TRIM_HORIZON:
                return ShardFilter.builder()
                        .type(ShardFilterType.AT_TRIM_HORIZON)
                        .build();
            default:
                throw new IllegalStateException(String.format("Invalid config %s", config));
        }
    }

    private static ShardFilter buildSingleShardFilter(String shardId) {
        return ShardFilter.builder()
                .shardId(shardId)
                .type(ShardFilterType.AFTER_SHARD_ID)
                .build();
    }

    static List<Shard> getShardsAfterParent(String parentShardId, Config config, ClientBuilder builder) {
        ListShardsRequest listShardsRequest = ListShardsRequest.builder()
                .streamName(config.getStreamName())
                .shardFilter(buildSingleShardFilter(parentShardId))
                .build();

        return tryListingShards(listShardsRequest, builder).shards();
    }

    static ShardsProgressTracker initSubscribedShardsProgressInfo(Config config, ClientBuilder builder) {
        ListShardsRequest listShardsRequest = ListShardsRequest.builder()
                .streamName(config.getStreamName())
                .shardFilter(buildFilter(config))
                .build();
        ListShardsResponse response = tryListingShards(listShardsRequest, builder);
        Map<String, ShardProgress> progressMap = new HashMap<>();
        response.shards().stream()
                .map(s -> new ShardProgress(config, s.shardId()))
                .forEach(s -> progressMap.put(s.getShardId(), s));
        return new ShardsProgressTracker(progressMap);
    }

    private static ListShardsResponse tryListingShards(ListShardsRequest listShardsRequest, ClientBuilder builder) {
        try (SimplifiedKinesisAsyncClient c = builder.build()) {
            ListShardsResponse response =
                    c.listShards(listShardsRequest).get(shardListingTimeoutMs, TimeUnit.MILLISECONDS);
            LOG.debug("Shards found = {}", response.shards());
            return response;
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            LOG.error("Error listing shards {}", e.getMessage());
            throw new RuntimeException("Error listing shards. Stopping");
        } catch (Exception e) {
            LOG.error("Unexpected error {}", e.getMessage());
            throw new RuntimeException("Error listing shards. Stopping");
        }
    }

    Map<String, ShardProgress> shardsProgress() {
        return shardsProgress;
    }

    void addShard(String shardId, ShardProgress progress) {
        shardsProgress.put(shardId, progress);
    }
}
