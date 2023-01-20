package com.psolomin.consumer;

import static com.psolomin.consumer.KinesisToFilePipeline.addPipelineSteps;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

public class Main {
    public static void main(String[] args) {
        ConsumerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ConsumerOpts.class);
        PipelineOptionsValidator.validate(ConsumerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);
        addPipelineSteps(p, opts);
        p.run().waitUntilFinish();
    }
}
