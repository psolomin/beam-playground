package com.psolomin.consumer;

import com.psolomin.records.ConsumedEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.Duration;
import org.joda.time.Instant;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisToFilePipeline {
    public static WriteFilesResult<Void> write(PCollection<KinesisRecord> records, ConsumerOpts opts) {
        String runId = RandomStringUtils.randomAlphabetic(5).toLowerCase();
        return records.apply("Process payloads", ParDo.of(new SlowProcessor(opts.getProcessTimePerRecord())))
                .apply("Maybe fail", ParDo.of(new FailingProcessor(opts.getFailAfterRecordsSeenCnt())))
                .apply("Parse payloads", ParDo.of(new ConsumedEventDeserializer()))
                .setCoder(AvroCoder.of(GenericRecord.class, ConsumedEvent.SCHEMA$))
                .apply(
                        "Sink to S3",
                        FileIO.<GenericRecord>write()
                                .via(ParquetIO.sink(ConsumedEvent.SCHEMA$)
                                        .withCompressionCodec(CompressionCodecName.SNAPPY))
                                .to(opts.getSinkLocation())
                                .withNaming(new NoColonFileNaming(runId)));
    }

    public static void addPipelineSteps(Pipeline p, ConsumerOpts opts) {
        ClientConfiguration clientConfiguration = ClientConfiguration.builder()
                .retry(RetryConfiguration.builder()
                        .baseBackoff(Duration.standardSeconds(3))
                        .numRetries(10)
                        .build())
                .build();

        InitialPositionInStream positionInStream = fromConfig(opts.getStartMode());
        KinesisIO.Read reader = KinesisIO.read()
                .withStreamName(opts.getInputStream())
                .withClientConfiguration(clientConfiguration)
                .withInitialPositionInStream(positionInStream)
                .withProcessingTimeWatermarkPolicy();

        if (opts.getStartTs() != null) {
            Instant ts = Instant.parse(opts.getStartTs());
            reader = reader.withInitialTimestampInStream(ts);
        }

        PCollection<KinesisRecord> windowedRecords = p.apply("Source", reader)
                .apply(
                        "Fixed windows",
                        Window.<KinesisRecord>into(FixedWindows.of(Duration.standardSeconds(60)))
                                .withAllowedLateness(Duration.standardHours(1))
                                .discardingFiredPanes()
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardSeconds(60)))));

        KinesisToFilePipeline.write(windowedRecords, opts);
    }

    private static InitialPositionInStream fromConfig(String confOpt) {
        switch (confOpt) {
            case "LATEST":
                return InitialPositionInStream.LATEST;
            case "TRIM_HORIZON":
                return InitialPositionInStream.TRIM_HORIZON;
            case "AT_TIMESTAMP":
                return InitialPositionInStream.AT_TIMESTAMP;
            default:
                throw new IllegalStateException("Not supported : " + confOpt);
        }
    }
}
