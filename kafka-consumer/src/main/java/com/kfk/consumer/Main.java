package com.kfk.consumer;

import static org.apache.beam.sdk.io.FileIO.Write.defaultNaming;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;
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

        // Put gc://... here
        String finalDestinationDir = "data/output";
        // New files will be written every ...
        int windowSizeSeconds = 10;

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
                .apply(ParDo.of(new LogDoFn()))
                .apply(
                        "Fixed windows",
                        Window.<KafkaRecord<byte[], byte[]>>into(
                                        FixedWindows.of(Duration.standardSeconds(windowSizeSeconds))))
                .apply(
                        "Write files",
                        FileIO.<String, KafkaRecord<byte[], byte[]>>writeDynamic()
                                .by(KafkaRecord::getTopic)
                                .withDestinationCoder(StringUtf8Coder.of())
                                .via(
                                        Contextful.fn(new SerializableFunction<KafkaRecord<byte[], byte[]>, String>() {
                                            @Override
                                            public String apply(KafkaRecord<byte[], byte[]> input) {
                                                // TODO: before this sink filter out records with null / empty values
                                                return new String(input.getKV().getValue(), StandardCharsets.UTF_8);
                                            }
                                        }),
                                        Contextful.fn(new SerializableFunction<String, FileIO.Sink<String>>() {
                                            @Override
                                            public FileIO.Sink<String> apply(String input) {
                                                return TextIO.sink();
                                            }
                                        }))
                                .to(finalDestinationDir)
                                .withNaming(topic -> defaultNaming(topic + "/", ".json")));

        p.run().waitUntilFinish();
    }
}
