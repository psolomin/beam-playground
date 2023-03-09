package com.psolomin.consumer;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * This will make pipeline to crash and re-started by the runner.
 */
public class FailingProcessor extends DoFn<byte[], byte[]> {
    private final int failAfterRecordsSeenCnt;
    private transient AtomicInteger recordsSeenCnt;

    public FailingProcessor(int failAfterRecordsSeenCnt) {
        this.failAfterRecordsSeenCnt = failAfterRecordsSeenCnt;
    }

    @Setup
    public void setup() {
        recordsSeenCnt = new AtomicInteger(0);
    }

    @ProcessElement
    public void processElement(@Element byte[] input, OutputReceiver<byte[]> out) {
        if (failAfterRecordsSeenCnt > 0) {
            if (recordsSeenCnt.incrementAndGet() >= failAfterRecordsSeenCnt) {
                throw new RuntimeException("Go away!");
            }
        }
        out.output(input);
    }
}
