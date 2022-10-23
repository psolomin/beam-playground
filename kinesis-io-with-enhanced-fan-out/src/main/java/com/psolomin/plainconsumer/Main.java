package com.psolomin.plainconsumer;

import com.psolomin.plainconsumer.sink.LogCountRecordsSink;
import com.psolomin.plainconsumer.sink.RecordsSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws InterruptedException {
        String streamName = args[0];
        String consumerArn = args[1];

        int uptimeMin = 8;
        Config config = new Config(streamName, consumerArn, StartType.LATEST);
        RecordsSink sink = new LogCountRecordsSink();
        StreamConsumer streamConsumer = StreamConsumer.init(config, new ClientBuilderImpl(), sink);
        Thread.sleep(1000L * 60 * uptimeMin);
        LOG.info("Total events received {}", sink.getTotalCnt());
        streamConsumer.initiateGracefulShutdown();
        streamConsumer.awaitTermination();
    }
}
