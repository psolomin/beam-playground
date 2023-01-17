package com.psolomin.consumer;

import com.psolomin.records.LogEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.beam.sdk.transforms.DoFn;

public class LogEventDeserializer extends DoFn<byte[], GenericRecord> {
    private DatumReader<GenericRecord> datumReader;

    @Setup
    public void setup() {
        datumReader = new SpecificDatumReader<>(LogEvent.SCHEMA$);
    }

    @ProcessElement
    public void processElement(@Element byte[] payload, OutputReceiver<GenericRecord> out) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(payload);
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(stream, null);
        GenericRecord record = datumReader.read(null, binaryDecoder);
        out.output(record);
    }
}
