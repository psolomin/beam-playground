package com.psolomin.consumer;

import com.psolomin.records.ConsumedEvent;
import com.psolomin.records.LogEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class ConsumedEventDeserializer extends DoFn<KinesisRecord, GenericRecord> {
    private DatumReader<LogEvent> datumReader;

    @Setup
    public void setup() {
        datumReader = new SpecificDatumReader<>(LogEvent.class);
    }

    @ProcessElement
    public void processElement(@Element KinesisRecord kinesisRecord, OutputReceiver<GenericRecord> out)
            throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(kinesisRecord.getDataAsBytes());
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(stream, null);
        LogEvent logEvent = datumReader.read(null, binaryDecoder);
        GenericRecord avroRecord = new GenericData.Record(ConsumedEvent.SCHEMA$);
        avroRecord.put("id", logEvent.getId());
        avroRecord.put("shard_id", kinesisRecord.getShardId());
        out.output(avroRecord);
    }
}
