package com.psolomin.kda;

import com.psolomin.producer.RandomPartitioner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import com.psolomin.producer.Main.ProducerOpts;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;

import static com.psolomin.producer.Producer.buildProducerP;

public class KdaProducer {
    public static void main(String[] args) {
        ProducerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ProducerOpts.class);
        PipelineOptionsValidator.validate(ProducerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);

        buildProducerP(p, opts)
                .apply(KinesisIO.<byte[]>write()
                        .withStreamName(opts.getOutputStream())
                        .withSerializer(r -> r)
                        .withPartitioner(new RandomPartitioner()));

        p.run().waitUntilFinish();
    }
}
