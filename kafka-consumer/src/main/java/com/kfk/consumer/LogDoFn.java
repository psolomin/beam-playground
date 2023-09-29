package com.kfk.consumer;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDoFn extends DoFn<KafkaRecord<byte[], byte[]>, KafkaRecord<byte[], byte[]>> {
    private static final Logger LOG = LoggerFactory.getLogger(LogDoFn.class);

    @ProcessElement
    public void processElement(
            @Element KafkaRecord<byte[], byte[]> input, OutputReceiver<KafkaRecord<byte[], byte[]>> out) {
        byte[] key = input.getKV().getKey();
        byte[] val = input.getKV().getValue();
        String k = key != null ? new String(key, StandardCharsets.UTF_8) : "<no-key>";
        String v = val != null ? new String(val, StandardCharsets.UTF_8) : "<no-val>";
        LOG.info("Got k = {} v = {} from topic {} partition {}", k, v, input.getTopic(), input.getPartition());
        out.output(input);
    }
}
