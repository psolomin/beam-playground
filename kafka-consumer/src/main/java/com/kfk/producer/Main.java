package com.kfk.producer;

import java.util.Arrays;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public interface FlinkProducerOpts extends ProducerOpts, FlinkPipelineOptions {}

    public static void main(String[] args) {
        LOG.info("Received args {}", Arrays.toString(args));
        FlinkProducerOpts opts = PipelineOptionsFactory.fromArgs(args).as(FlinkProducerOpts.class);
        opts.setRunner(FlinkRunner.class);
        opts.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);
        PipelineOptionsValidator.validate(FlinkProducerOpts.class, opts);
        LOG.info("Parsed opts {}", opts);
        Pipeline p = Pipeline.create(opts);

        GenerateSequence seq = GenerateSequence.from(0)
                .to(opts.getMsgsToWrite())
                .withRate(opts.getMsgsPerSec(), Duration.standardSeconds(1L));

        p.apply("Emitter", seq)
                .apply("Convert to records", ParDo.of(new IdToRecordDoFn(opts.getOutputTopic())))
                .setCoder(ProducerRecordCoder.of(VarLongCoder.of(), StringUtf8Coder.of()))
                .apply(
                        "Write to Kafka",
                        KafkaIO.<Long, String>writeRecords()
                                .withBootstrapServers(opts.getBootstrapServers())
                                .withTopic(opts.getOutputTopic())
                                .withKeySerializer(LongSerializer.class)
                                .withValueSerializer(StringSerializer.class));

        p.run().waitUntilFinish();
    }
}
