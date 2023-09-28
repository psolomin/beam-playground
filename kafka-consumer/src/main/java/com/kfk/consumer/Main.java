package com.kfk.consumer;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        LOG.info("Received args {}", Arrays.toString(args));
        MyPipelineOpts opts = PipelineOptionsFactory.fromArgs(args).as(MyPipelineOpts.class);
        PipelineOptionsValidator.validate(MyPipelineOpts.class, opts);
        LOG.info("Parsed opts {}", opts);
        Pipeline p = Pipeline.create(opts);

        p.apply(KafkaIO.<byte[], byte[]>read()
                        .withBootstrapServers(opts.getBootstrapServers())
                        .withTopic(opts.getInputTopic())
                        .withKeyDeserializer(ByteArrayDeserializer.class)
                        .withValueDeserializer(ByteArrayDeserializer.class)
                        .withConsumerConfigUpdates(ImmutableMap.of("group.id", "my_beam_app_1"))
                        .withProcessingTime()
                        .withReadCommitted()
                        .commitOffsetsInFinalize())
                .apply(ParDo.of(new LogDoFn()));

        p.run().waitUntilFinish();
    }
}
