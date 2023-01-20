package com.psolomin.plainconsumer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardFilter;
import software.amazon.awssdk.services.kinesis.model.ShardFilterType;

public class ShardsListingUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ShardsListingUtils.class);
    private static final int shardListingTimeoutMs = 10_000;

    static List<String> getListOfShards(Config config, AsyncClientProxy kinesis) {
        ListShardsRequest listShardsRequest = ListShardsRequest.builder()
                .streamName(config.getStreamName())
                .shardFilter(buildFilter(config))
                .build();

        ListShardsResponse response = tryListingShards(listShardsRequest, kinesis);
        return response.shards().stream().map(Shard::shardId).collect(Collectors.toList());
    }

    private static ListShardsResponse tryListingShards(ListShardsRequest listShardsRequest, AsyncClientProxy kinesis) {
        try {
            ListShardsResponse response =
                    kinesis.listShards(listShardsRequest).get(shardListingTimeoutMs, TimeUnit.MILLISECONDS);
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

    private static ShardFilter buildFilter(Config config) {
        switch (config.getStartingPoint().getPosition()) {
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
}
