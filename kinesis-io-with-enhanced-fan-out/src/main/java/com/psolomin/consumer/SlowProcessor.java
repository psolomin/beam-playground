package com.psolomin.consumer;

import org.apache.beam.sdk.transforms.DoFn;

public class SlowProcessor extends DoFn<byte[], byte[]> {
    private final long delay;

    public SlowProcessor(long delay) {
        this.delay = delay;
    }

    @ProcessElement
    public void processElement(@Element byte[] input, OutputReceiver<byte[]> out) {
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        out.output(input);
    }
}
