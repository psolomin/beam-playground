package com.psolomin.consumer;

import static com.psolomin.producer.Producer.buildProducerP;

import com.psolomin.producer.Main;
import com.psolomin.records.ConsumedEvent;
import java.util.List;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KinesisToFilePipelineTest {
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule
    public TestPipeline writerP = TestPipeline.create();

    @Rule
    public TestPipeline readerP = TestPipeline.create();

    @Test
    public void generatedEventsCanBeStoredAsFiles() {
        String path = tmpFolder.getRoot().getAbsolutePath();
        Main.ProducerOpts producerOpts = PipelineOptionsFactory.create().as(Main.ProducerOpts.class);
        producerOpts.setMsgsPerSec(100);
        producerOpts.setMsgsToWrite(3);
        producerOpts.setOutputStream("stream-01");

        ConsumerOpts consumerOpts = PipelineOptionsFactory.create().as(ConsumerOpts.class);
        consumerOpts.setSinkLocation(path);

        PCollection<KinesisRecord> records = buildProducerP(writerP, producerOpts)
                .apply(
                        "KinesisRecords",
                        MapElements.into(TypeDescriptor.of(KinesisRecord.class)).via(new BytesToKinesisRecord()))
                .setCoder(KinesisRecordCoder.of());

        KinesisToFilePipeline.write(records, consumerOpts);
        writerP.run();

        PCollection<ConsumedEvent> recordsFromFiles =
                readerP.apply(ParquetIO.parseGenericRecords(new GenericRecordToConsumedEvent())
                        .from(path + "/*"));

        PAssert.that(recordsFromFiles)
                .containsInAnyOrder(List.of(
                        new ConsumedEvent(0L, "shard-000"),
                        new ConsumedEvent(1L, "shard-000"),
                        new ConsumedEvent(2L, "shard-000")));
        readerP.run();
    }
}
