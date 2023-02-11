package com.psolomin.flink;

import static com.psolomin.consumer.KinesisToFilePipeline.addPipelineSteps;

import com.psolomin.consumer.ConsumerOpts;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(FlinkConsumer.class);

    public interface FlinkConsumerOpts extends ConsumerOpts, FlinkPipelineOptions {}

    public static void main(String[] args) {
        LOG.info("Starting application {}", args);
        FlinkConsumerOpts options = PipelineOptionsFactory.fromArgs(args).as(FlinkConsumerOpts.class);
        options.setRunner(FlinkRunner.class);
        options.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);

        PipelineOptionsValidator.validate(FlinkConsumer.FlinkConsumerOpts.class, options);
        Pipeline p = Pipeline.create(options);
        addPipelineSteps(p, options);
        p.run().waitUntilFinish();
    }
}
