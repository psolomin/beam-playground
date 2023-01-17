package com.psolomin.consumer;

import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class PayloadExtractor extends DoFn<KinesisRecord, byte[]> {
    @ProcessElement
    public void processElement(@Element KinesisRecord record, OutputReceiver<byte[]> out) {
        out.output(record.getDataAsBytes());
    }
}
