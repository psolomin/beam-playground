package com.psolomin.flink;

import com.psolomin.sqs.SqsProducerOpts;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class FlinkSqsProducer {
    public interface FlinkSqsProducerOpts extends SqsProducerOpts, FlinkPipelineOptions {}

    public static void main(String[] args) {
        FlinkSqsProducerOpts opts = PipelineOptionsFactory.fromArgs(args).as(FlinkSqsProducerOpts.class);
        opts.setRunner(FlinkRunner.class);
        opts.setShutdownSourcesAfterIdleMs(Long.MAX_VALUE);
        PipelineOptionsValidator.validate(FlinkSqsProducerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);

        PCollection<Long> df =
                p.apply("Generate Sequence", GenerateSequence.from(0).to(opts.getMsgsToWrite()));
        if (!opts.getEnableBatchedWrites()) {
            write(opts.getOutputQueueUrl(), df);
        } else {
            writeBatches(opts.getOutputQueueUrl(), df);
        }
        p.run().waitUntilFinish();
    }

    static PDone write(String queueUrl, PCollection<Long> df) {
        return df.apply(
                        "Prepare SQS message",
                        MapElements.into(TypeDescriptor.of(SendMessageRequest.class))
                                .via(row -> SendMessageRequest.builder()
                                        .queueUrl(queueUrl)
                                        .messageBody(row.toString())
                                        .build()))
                .apply("Write to SQS", SqsIO.write());
    }

    static SqsIO.WriteBatches.Result writeBatches(String queueUrl, PCollection<Long> df) {
        return df.apply(
                "Write to SQS",
                SqsIO.<Long>writeBatches((b, row) -> b.messageBody(row.toString()))
                        .to(queueUrl));
    }
}
