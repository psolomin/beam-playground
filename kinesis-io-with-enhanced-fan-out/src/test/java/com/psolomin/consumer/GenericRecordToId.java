package com.psolomin.consumer;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class GenericRecordToId implements SerializableFunction<GenericRecord, Long> {
    public Long apply(GenericRecord record) {
        return (Long) record.get("id");
    }
}
