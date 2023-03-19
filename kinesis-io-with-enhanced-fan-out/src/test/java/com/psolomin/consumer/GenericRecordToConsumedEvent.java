package com.psolomin.consumer;

import com.psolomin.records.ConsumedEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class GenericRecordToConsumedEvent implements SerializableFunction<GenericRecord, ConsumedEvent> {
    public ConsumedEvent apply(GenericRecord record) {
        return new ConsumedEvent((Long) record.get("id"), record.get("shard_id").toString());
    }
}
