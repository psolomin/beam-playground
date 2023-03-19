package com.psolomin.consumer;

import com.psolomin.producer.LogEventSerializer;
import com.psolomin.records.ConsumedEvent;
import com.psolomin.records.LogEvent;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
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
        PCollection<ConsumedEvent> result = p.apply(Create.of(records).withCoder(AvroCoder.of(LogEvent.class)))
                .apply(ParDo.of(new LogEventSerializer()))
                .apply(
                        "KinesisRecords",
                        MapElements.into(TypeDescriptor.of(KinesisRecord.class)).via(new BytesToKinesisRecord()))
                .setCoder(KinesisRecordCoder.of())
                .apply(ParDo.of(new ConsumedEventDeserializer()))
                .setCoder(AvroCoder.of(GenericRecord.class, ConsumedEvent.SCHEMA$))
                .apply(MapElements.into(TypeDescriptor.of(ConsumedEvent.class))
                        .via(new GenericRecordToConsumedEvent()));

        PAssert.that(result)
                .containsInAnyOrder(List.of(
                        new ConsumedEvent(0L, "shard-000"),
                        new ConsumedEvent(1L, "shard-000"),
                        new ConsumedEvent(2L, "shard-000")));
        p.run();
    }
}
