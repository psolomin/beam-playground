package com.psolomin.example;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Requirements;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

public class MyConverters {
    public static class RecordsConverter implements Contextful.Fn<Integer, GenericRecord> {
        private final PCollectionView<Map<Integer, String>> schemasView;

        public RecordsConverter(PCollectionView<Map<Integer, String>> schemasView) {
            this.schemasView = schemasView;
        }

        @Override
        public GenericRecord apply(Integer element, Context c) throws Exception {
            Integer schemaKey = element % 2;
            Schema schema = new Schema.Parser().parse(c.sideInput(schemasView).get(schemaKey));
            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put("my_int", element);
            return genericRecord;
        }
    }

    public static class SinkBuilder implements Contextful.Fn<Integer, FileIO.Sink<GenericRecord>> {
        private final PCollectionView<Map<Integer, String>> schemasView;

        public SinkBuilder(PCollectionView<Map<Integer, String>> schemasView) {
            this.schemasView = schemasView;
        }

        @Override
        public FileIO.Sink<GenericRecord> apply(Integer schemaKey, Context c) throws Exception {
            String schema = c.sideInput(schemasView).get(schemaKey);
            return AvroIO.sink(schema);
        }
    }

    public static PCollectionView<Map<Integer, String>> schemasView(PCollection<Integer> input) {
        return input.apply(ParDo.of(new DoFn<Integer, KV<Integer, Integer>>() {
            @ProcessElement
            public void processElement(@Element Integer record, OutputReceiver<KV<Integer, Integer>> out) {
                // Get schema ID from record:
                out.output(KV.of(record % 2, 0));
            }
        }))
                .apply("Added to avoid duplicate map records error", Combine.perKey(new SerializableBiFunction<Integer, Integer, Integer>() {
                    @Override
                    public Integer apply(Integer s, Integer s2) {
                        return 0;
                    }
                }))
                .apply(Keys.create())
                .apply("Get schemas via schema ids", ParDo.of(new DoFn<Integer, KV<Integer, String>>() {
                    @ProcessElement
                    public void processElement(@Element Integer schemaId, OutputReceiver<KV<Integer, String>> out) {
                        // This schema can be same for all records, it doesn't matter
                        String schema = "{\"type\": \"record\", \"namespace\": \"com.pavel.records\", \"name\": \"Record\", \"fields\": [{\"name\": \"my_int\", \"type\": \"int\"}]}";
                        if (schemaId == 0) {
                            out.output(KV.of(schemaId, schema));
                        } else if (schemaId == 1) {
                            out.output(KV.of(schemaId, schema));
                        } else {
                            throw new RuntimeException();
                        }
                    }
                }))
                .apply("As view", View.asMap());
    }

    public static void applyWriter(PCollection<Integer> input, String basePath, PCollectionView<Map<Integer, String>> schemasView) {
        input.apply(FileIO.<Integer,  Integer>writeDynamic()
                .by(new SerializableFunction<Integer, Integer>() {
                    @Override
                    public Integer apply(Integer input) {
                        return input % 2;
                    }
                })
                .withDestinationCoder(AvroCoder.of(Integer.class))
                .via(
                        Contextful.of(new RecordsConverter(schemasView), Requirements.requiresSideInputs(schemasView)),
                        Contextful.of(new SinkBuilder(schemasView), Requirements.requiresSideInputs(schemasView))
                )
                .to(basePath)
                .withNumShards(1)
                .withNaming(schemaId -> FileIO.Write.defaultNaming(String.format("p_schema_id=%s/", schemaId), ".avro")));
    }
}
