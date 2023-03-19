package com.psolomin.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.joda.time.Instant;

/** Copy of Beam's KinesisRecordCoder since it's not public. */
class KinesisRecordCoder extends AtomicCoder<KinesisRecord> {

    private static final StringUtf8Coder STRING_CODER = StringUtf8Coder.of();
    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();
    private static final InstantCoder INSTANT_CODER = InstantCoder.of();
    private static final VarLongCoder VAR_LONG_CODER = VarLongCoder.of();

    static KinesisRecordCoder of() {
        return new KinesisRecordCoder();
    }

    @Override
    public void encode(KinesisRecord value, OutputStream outStream) throws IOException {
        BYTE_ARRAY_CODER.encode(value.getDataAsBytes(), outStream);
        STRING_CODER.encode(value.getSequenceNumber(), outStream);
        STRING_CODER.encode(value.getPartitionKey(), outStream);
        INSTANT_CODER.encode(value.getApproximateArrivalTimestamp(), outStream);
        VAR_LONG_CODER.encode(value.getSubSequenceNumber(), outStream);
        INSTANT_CODER.encode(value.getReadTime(), outStream);
        STRING_CODER.encode(value.getStreamName(), outStream);
        STRING_CODER.encode(value.getShardId(), outStream);
    }

    @Override
    public KinesisRecord decode(InputStream inStream) throws IOException {
        ByteBuffer data = ByteBuffer.wrap(BYTE_ARRAY_CODER.decode(inStream));
        String sequenceNumber = STRING_CODER.decode(inStream);
        String partitionKey = STRING_CODER.decode(inStream);
        Instant approximateArrivalTimestamp = INSTANT_CODER.decode(inStream);
        long subSequenceNumber = VAR_LONG_CODER.decode(inStream);
        Instant readTimestamp = INSTANT_CODER.decode(inStream);
        String streamName = STRING_CODER.decode(inStream);
        String shardId = STRING_CODER.decode(inStream);
        return new KinesisRecord(
                data,
                sequenceNumber,
                subSequenceNumber,
                partitionKey,
                approximateArrivalTimestamp,
                readTimestamp,
                streamName,
                shardId);
    }
}
