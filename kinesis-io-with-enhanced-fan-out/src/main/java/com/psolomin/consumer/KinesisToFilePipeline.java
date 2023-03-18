package com.psolomin.consumer;

import com.psolomin.records.LogEvent;
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
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.Duration;
import software.amazon.kinesis.common.InitialPositionInStream;

public class KinesisToFilePipeline {
    public static WriteFilesResult<Void> write(PCollection<KinesisRecord> records, ConsumerOpts opts) {
        String runId = RandomStringUtils.randomAlphabetic(5);
        return records.apply("Get payloads", ParDo.of(new PayloadExtractor()))
                .apply("Process payloads", ParDo.of(new SlowProcessor(opts.getProcessTimePerRecord())))
                .apply("Maybe fail", ParDo.of(new FailingProcessor(opts.getFailAfterRecordsSeenCnt())))
                .apply("Parse payloads", ParDo.of(new LogEventDeserializer()))
                .setCoder(AvroCoder.of(GenericRecord.class, LogEvent.SCHEMA$))
                .apply(
                        "Sink to S3",
                        FileIO.<GenericRecord>write()
                                .via(ParquetIO.sink(LogEvent.SCHEMA$).withCompressionCodec(CompressionCodecName.SNAPPY))
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

        PCollection<KinesisRecord> windowedRecords = p.apply("Source", reader)
                .apply("Fixed windows", Window.<KinesisRecord>into(FixedWindows.of(Duration.standardSeconds(60))));

        KinesisToFilePipeline.write(windowedRecords, opts);
    }

    private static InitialPositionInStream fromConfig(String confOpt) {
        if (confOpt.equals("LATEST")) {
            return InitialPositionInStream.LATEST;
        } else if (confOpt.equals("TRIM_HORIZON")) {
            return InitialPositionInStream.TRIM_HORIZON;
        } else {
            throw new IllegalStateException("Not supported : " + confOpt);
        }
    }
}
