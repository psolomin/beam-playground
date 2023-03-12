package com.psolomin.producer;

import com.psolomin.records.LogEvent;
import java.io.IOException;
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

        return p.apply("Emitter", seq)
                .apply("Convert to Records", ParDo.of(new DoFn<Long, LogEvent>() {
                    @ProcessElement
                    public void processElement(@Element Long id, OutputReceiver<LogEvent> out) throws IOException {
                        out.output(LogEvent.newBuilder().setId(id).build());
                    }
                }))
                .apply("Convert to Bytes", ParDo.of(new LogEventSerializer()));
    }
}
