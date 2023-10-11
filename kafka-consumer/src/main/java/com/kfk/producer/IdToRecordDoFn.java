package com.kfk.producer;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.kafka.clients.producer.ProducerRecord;

public class IdToRecordDoFn extends DoFn<Long, ProducerRecord<Long, String>> {
    private final String topic;

    public IdToRecordDoFn(String topic) {
        this.topic = topic;
    }

    @ProcessElement
    public void processElement(@Element Long input, OutputReceiver<ProducerRecord<Long, String>> out) {
        Long key = input % 11;
        String value = String.format("{\"id\": %s}", input);
        ProducerRecord<Long, String> producerRecord = new ProducerRecord<>(topic, key, value);
        out.output(producerRecord);
    }
}
