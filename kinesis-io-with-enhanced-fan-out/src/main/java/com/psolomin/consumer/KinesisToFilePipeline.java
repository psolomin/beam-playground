package com.psolomin.consumer;

import com.psolomin.records.LogEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.common.RetryConfiguration;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.Duration;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisToFilePipeline {
    public static WriteFilesResult<Void> write(PCollection<KinesisRecord> records, String sinkLocation) {
        return records.apply("Get payloads", ParDo.of(new PayloadExtractor()))
                .apply("Parse payloads", ParDo.of(new LogEventDeserializer()))
                .setCoder(AvroCoder.of(GenericRecord.class, LogEvent.SCHEMA$))
                .apply(
                        "Sink to S3",
                        FileIO.<GenericRecord>write()
                                .via(ParquetIO.sink(LogEvent.SCHEMA$).withCompressionCodec(CompressionCodecName.SNAPPY))
                                .to(sinkLocation)
                                .withNaming(new NoColonFileNaming()));
    }

    public static void addPipelineSteps(Pipeline p, ConsumerOpts opts) {
        String consumerArn = opts.getConsumerArn();
        ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .retry(RetryConfiguration.builder()
                        .baseBackoff(Duration.standardSeconds(3))
                        .numRetries(10)
                        .build())
                .build();

        KinesisIO.Read reader = KinesisIO.read()
                .withStreamName(opts.getInputStream())
                .withInitialPositionInStream(InitialPositionInStream.LATEST)
                .withClientConfiguration(clientConfiguration)
                .withProcessingTimeWatermarkPolicy();

        if (!consumerArn.equals("none")) {
            reader = reader.withConsumerArn(consumerArn);
        }

        PCollection<KinesisRecord> windowedRecords = p.apply("Source", reader)
                .apply(
                        "Fixed windows",
                        Window.<KinesisRecord>into(FixedWindows.of(Duration.standardSeconds(60)))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes()
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardSeconds(60)))));

        KinesisToFilePipeline.write(windowedRecords, opts.getSinkLocation());
    }
}
