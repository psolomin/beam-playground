package com.psolomin.kda;

import static com.psolomin.producer.Producer.buildProducerP;

import com.psolomin.producer.Main.ProducerOpts;
import com.psolomin.producer.RandomPartitioner;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.commons.lang3.ArrayUtils;

public class KdaProducer {
    public static final String BEAM_APPLICATION_PROPERTIES = "ProducerProperties";

    public interface FlinkProducerOpts extends ProducerOpts, FlinkPipelineOptions {}

    public static void main(String[] args) {
        String[] kinesisArgs = BasicBeamStreamingJobOptionsParser.argsFromKinesisApplicationProperties(
                args, BEAM_APPLICATION_PROPERTIES);
        FlinkProducerOpts options = PipelineOptionsFactory.fromArgs(ArrayUtils.addAll(args, kinesisArgs))
                .as(FlinkProducerOpts.class);

        options.setRunner(FlinkRunner.class);

        PipelineOptionsValidator.validate(FlinkProducerOpts.class, options);
        Pipeline p = Pipeline.create(options);

        buildProducerP(p, options)
                .apply(KinesisIO.<byte[]>write()
                        .withStreamName(options.getOutputStream())
                        .withSerializer(r -> r)
                        .withPartitioner(new RandomPartitioner()));

        p.run().waitUntilFinish();
    }
}
