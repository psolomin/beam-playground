package com.psolomin.consumer;

import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class AssignTimestampsDoFn extends DoFn<KinesisRecord, KinesisRecord> {
    @ProcessElement
    public void processElement(@Element KinesisRecord input, OutputReceiver<KinesisRecord> out) {
        out.outputWithTimestamp(input, Instant.now());
    }

    @Override
    public Duration getAllowedTimestampSkew() {
        return Duration.standardSeconds(5);
    }
}
