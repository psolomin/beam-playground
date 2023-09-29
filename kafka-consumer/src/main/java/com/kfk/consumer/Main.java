package com.kfk.consumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.ParDo;
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

        Map<String, Object> consumerProps = Map.of("group.id", "my_beam_app_1");

        Map<String, Object> securityProps = Map.of(
                // "security.protocol", "SSL",
                // "ssl.keystore.location", "...",
                // "ssl.keystore.password", "...",
                // "ssl.key.password", "...",
                // "ssl.truststore.location", "...",
                // "ssl.truststore.password", "..."
                );

        Map<String, Object> allProps = new HashMap<>();
        allProps.putAll(consumerProps);
        allProps.putAll(securityProps);

        List<String> topics = Arrays.asList(opts.getInputTopics().split(","));

        p.apply(KafkaIO.<byte[], byte[]>read()
                        .withBootstrapServers(opts.getBootstrapServers())
                        .withTopics(topics)
                        .withKeyDeserializer(ByteArrayDeserializer.class)
                        .withValueDeserializer(ByteArrayDeserializer.class)
                        .withConsumerConfigUpdates(allProps)
                        .withOffsetConsumerConfigOverrides(securityProps)
                        .withProcessingTime()
                        .withReadCommitted()
                        .commitOffsetsInFinalize())
                .apply(ParDo.of(new LogDoFn()));

        p.run().waitUntilFinish();
    }
}
