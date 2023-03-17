package com.psolomin.consumer;

import static com.psolomin.consumer.KinesisToFilePipeline.addPipelineSteps;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOG.info("Received args {}", Arrays.toString(args));
        ConsumerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ConsumerOpts.class);
        PipelineOptionsValidator.validate(ConsumerOpts.class, opts);
        LOG.info("Parsed opts {}", opts);
        Pipeline p = Pipeline.create(opts);
        addPipelineSteps(p, opts);
        p.run().waitUntilFinish();
    }
}
