package com.psolomin.plainconsumer;

import static com.psolomin.plainconsumer.ShardsListingUtils.getListOfShards;

import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.StartingPoint;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.KinesisClientUtil;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        AsyncClientProxy kinesis =
                new AsyncClientProxyImpl(KinesisClientUtil.adjustKinesisClientBuilder(KinesisAsyncClient.builder())
                        .build());
        Config config = new Config(
                "stream-01",
                "arn:aws:kinesis:eu-west-1:790288347884:stream/stream-01/consumer/consumer-01:1665959636",
                new StartingPoint(InitialPositionInStream.LATEST));
        List<String> shardsIds = getListOfShards(config, kinesis);
        ShardSubscribersPoolImpl pool = new ShardSubscribersPoolImpl(config, kinesis, shardsIds);
        pool.start();
        int cnt = 0;
        int max = 3600;
        while (cnt < max) {
            pool.nextRecord();
            cnt++;
        }
        pool.stop();
    }
}
