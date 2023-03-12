package com.psolomin.producer;

import com.psolomin.records.LogEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

public class LogEventEncoder {
    private final DatumWriter<LogEvent> datumWriter;
    private final ByteArrayOutputStream outStream;
    private final Encoder encoder;

    public LogEventEncoder() {
        datumWriter = new SpecificDatumWriter<>(LogEvent.SCHEMA$);
        outStream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(outStream, null);
    }

    public byte[] encode(LogEvent event) throws IOException {
        datumWriter.write(event, encoder);
        encoder.flush();
        byte[] payload = outStream.toByteArray();
        outStream.reset();
        return payload;
    }
}
