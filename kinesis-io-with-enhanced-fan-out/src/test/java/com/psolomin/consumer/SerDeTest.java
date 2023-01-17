package com.psolomin.consumer;

import com.psolomin.producer.LogEventSerializer;
import com.psolomin.records.LogEvent;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SerDeTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void eventCanBeEncodedAndDecoded() {
        List<LogEvent> records = List.of(new LogEvent(0L), new LogEvent(1L), new LogEvent(2L));
        PCollection<Long> result = p.apply(Create.of(records).withCoder(AvroCoder.of(LogEvent.class)))
                .apply(ParDo.of(new LogEventSerializer()))
                .apply(ParDo.of(new LogEventDeserializer()))
                .setCoder(AvroCoder.of(GenericRecord.class, LogEvent.SCHEMA$))
                .apply(MapElements.into(TypeDescriptors.longs()).via(new GenericRecordToId()));

        PAssert.that(result).containsInAnyOrder(Arrays.asList(0L, 1L, 2L));
        p.run();
    }
}
