package com.psolomin.producer;

import com.psolomin.records.LogEvent;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;

public class LogEventSerializer extends DoFn<LogEvent, byte[]> {
    private LogEventEncoder logEventEncoder;

    @Setup
    public void setup() {
        logEventEncoder = new LogEventEncoder();
    }

    @ProcessElement
    public void processElement(@Element LogEvent record, OutputReceiver<byte[]> out) throws IOException {
        out.output(logEventEncoder.encode(record));
    }
}
