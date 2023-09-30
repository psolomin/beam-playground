package com.kfk.consumer;

import static com.kfk.consumer.MyPipeline.addSteps;

import java.util.Arrays;
import java.util.Map;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkRunnerMain {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkRunnerMain.class);

    public interface FlinkConsumerOpts extends MyPipelineOpts, FlinkPipelineOptions {}

    public static void main(String[] args) {
        LOG.info("Received args {}", Arrays.toString(args));
        FlinkConsumerOpts opts = PipelineOptionsFactory.fromArgs(args).as(FlinkConsumerOpts.class);
        opts.setRunner(FlinkRunner.class);
        opts.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);
        PipelineOptionsValidator.validate(FlinkConsumerOpts.class, opts);
        LOG.info("Parsed opts {}", opts);
        Pipeline p = Pipeline.create(opts);
        Map<String, Object> consumerProps = Map.of("group.id", "my_beam_app_1");

        Map<String, Object> securityProps = Map.of(
                // "security.protocol", "SSL",
                // "ssl.keystore.location", "...",
                // "ssl.keystore.password", "...",
                // "ssl.key.password", "...",
                // "ssl.truststore.location", "...",
                // "ssl.truststore.password", "..."
                );

        // New files will be written every ...
        int windowSizeSeconds = 10;
        addSteps(p, opts, consumerProps, securityProps, windowSizeSeconds);
        p.run().waitUntilFinish();
    }
}
