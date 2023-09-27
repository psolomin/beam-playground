package com.kfk.consumer;

import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DummyTest {
    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void eventCanBeEncodedAndDecoded() {
        List<Integer> records = List.of(1, 2, 3);
        PCollection<Integer> result = p.apply(Create.of(records));
        PAssert.that(result).containsInAnyOrder(List.of(1, 2, 3));
        p.run();
    }
}
