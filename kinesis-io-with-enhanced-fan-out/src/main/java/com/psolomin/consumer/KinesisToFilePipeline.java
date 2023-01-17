package com.psolomin.consumer;

import com.psolomin.records.LogEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

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
}
