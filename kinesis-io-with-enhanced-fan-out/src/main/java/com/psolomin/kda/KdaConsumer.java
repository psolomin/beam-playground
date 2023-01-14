package com.psolomin.kda;

import com.psolomin.consumer.LoggerParDo;
import com.psolomin.consumer.Main;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KdaConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(KdaConsumer.class);
    public static final String BEAM_APPLICATION_PROPERTIES = "ConsumerProperties";

    public interface FlinkConsumerOpts extends Main.ConsumerOpts, FlinkPipelineOptions {}

    public static void main(String[] args) {
        LOG.info("Starting application {}", args);
        String[] kinesisArgs = BasicBeamStreamingJobOptionsParser.argsFromKinesisApplicationProperties(
                args, BEAM_APPLICATION_PROPERTIES);

        FlinkConsumerOpts options = PipelineOptionsFactory.fromArgs(ArrayUtils.addAll(args, kinesisArgs))
                .as(FlinkConsumerOpts.class);

        options.setRunner(FlinkRunner.class);
        options.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);

        PipelineOptionsValidator.validate(KdaConsumer.FlinkConsumerOpts.class, options);
        Pipeline p = Pipeline.create(options);

        p.apply(
                        "Source",
                        KinesisIO.read()
                                .withStreamName(options.getInputStream())
                                .withConsumerArn(options.getConsumerArn())
                                .withInitialPositionInStream(InitialPositionInStream.LATEST))
                .apply("Print", ParDo.of(new LoggerParDo()));

        p.run().waitUntilFinish();
    }
}
