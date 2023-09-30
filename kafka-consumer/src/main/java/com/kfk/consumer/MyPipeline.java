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
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;

public class MyPipeline {
    static WriteFilesResult<String> addSteps(
            Pipeline p,
            MyPipelineOpts opts,
            Map<String, Object> consumerProps,
            Map<String, Object> securityProps,
            int windowSizeSeconds) {
        Map<String, Object> allProps = new HashMap<>();
        allProps.putAll(consumerProps);
        allProps.putAll(securityProps);

        List<String> topics = Arrays.asList(opts.getInputTopics().split(","));

        return p.apply(
                        "Read topics",
                        KafkaIO.<byte[], byte[]>read()
                                .withBootstrapServers(opts.getBootstrapServers())
                                .withTopics(topics)
                                .withKeyDeserializer(ByteArrayDeserializer.class)
                                .withValueDeserializer(ByteArrayDeserializer.class)
                                .withConsumerConfigUpdates(allProps)
                                .withOffsetConsumerConfigOverrides(securityProps)
                                .withProcessingTime()
                                .withReadCommitted()
                                .commitOffsetsInFinalize())
                .apply("Logger", ParDo.of(new LogDoFn()))
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
                                .to(opts.getOutputDir())
                                // topic name will be a sub-dir with data received from that topic only
                                .withNaming(topic -> defaultNaming(topic + "/data", ".json")));
    }
}
