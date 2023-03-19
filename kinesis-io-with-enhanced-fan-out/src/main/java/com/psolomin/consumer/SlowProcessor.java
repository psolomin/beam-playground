package com.psolomin.consumer;

import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class SlowProcessor extends DoFn<KinesisRecord, KinesisRecord> {
    private final long delay;

    public SlowProcessor(long delay) {
        this.delay = delay;
    }

    /**
     * Checkpoints are created and then finalized.
     * Before they are finalized, all bundle must be processed.
     *
     * We can config maxBundleSize to be small, such that N records can be
     * checkpoint-ed. Or - try finishBundleBeforeCheckpointing option
     *
     * Source: https://beam.apache.org/documentation/runners/flink/#pipeline-options-for-the-flink-runner
     *
     * @param input
     * @param out
     */
    @ProcessElement
    public void processElement(@Element KinesisRecord input, OutputReceiver<KinesisRecord> out) {
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
