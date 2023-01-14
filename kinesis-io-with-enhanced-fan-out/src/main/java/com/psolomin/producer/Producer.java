package com.psolomin.producer;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class Producer {
    public static PCollection<byte[]> buildProducerP(Pipeline p, Main.ProducerOpts opts) {
        GenerateSequence seq = GenerateSequence.from(0)
                .to(opts.getMsgsToWrite())
                .withRate(opts.getMsgsPerSec(), Duration.standardSeconds(1L));

        return p.apply("Emitter", seq).apply("Convert to Bytes", ParDo.of(new DoFn<Long, byte[]>() {
            @ProcessElement
            public void processElement(@Element Long record, OutputReceiver<byte[]> out) {
                out.output(record.toString().getBytes(StandardCharsets.UTF_8));
            }
        }));
    }
}
