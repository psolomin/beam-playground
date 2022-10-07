package com.psolomin.plainconsumer;

import com.psolomin.plainconsumer.sink.LogCountRecordsSink;
import com.psolomin.plainconsumer.sink.RecordsSink;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        String streamName = args[0];
        String consumerArn = args[1];

        Config config = new Config(streamName, consumerArn, StartType.LATEST);
        RecordsSink sink = new LogCountRecordsSink();
        StreamConsumer streamConsumer = StreamConsumer.init(config, new ClientBuilderImpl(), sink);
        Thread.sleep(1000L * 60 * 20);
        streamConsumer.initiateGracefulShutdown();
        streamConsumer.awaitTermination();
    }
}
