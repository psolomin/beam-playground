package com.psolomin.flink;

import static com.psolomin.producer.Producer.buildProducerP;

import com.psolomin.producer.Main;
import com.psolomin.producer.RandomPartitioner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkProducer {
    public static final Logger LOG = LoggerFactory.getLogger(FlinkProducer.class);

    public interface FlinkProducerOpts extends Main.ProducerOpts, FlinkPipelineOptions {}

    public static void main(String[] args) {
        LOG.info("Starting application {}", args);
        FlinkProducer.FlinkProducerOpts options =
                PipelineOptionsFactory.fromArgs(args).as(FlinkProducer.FlinkProducerOpts.class);
        options.setRunner(FlinkRunner.class);
        options.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);
        PipelineOptionsValidator.validate(FlinkProducer.FlinkProducerOpts.class, options);
        Pipeline p = Pipeline.create(options);

        buildProducerP(p, options)
                .apply(KinesisIO.<byte[]>write()
                        .withStreamName(options.getOutputStream())
                        .withSerializer(r -> r)
                        .withPartitioner(new RandomPartitioner()));

        p.run().waitUntilFinish();
    }
}
