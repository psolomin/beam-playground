package com.psolomin.consumer;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerParDo extends DoFn<KinesisRecord, Long> {
    private Logger logger;

    @Setup
    public void setup() {
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    @ProcessElement
    public void processElement(@Element KinesisRecord record, OutputReceiver<Long> out) {
        String s = new String(record.getDataAsBytes(), StandardCharsets.UTF_8);
        Long l = Long.valueOf(s);
        logger.info(String.format("Received %s", l));
        out.output(l);
    }
}
