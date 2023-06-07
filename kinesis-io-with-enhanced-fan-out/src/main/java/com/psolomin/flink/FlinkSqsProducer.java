package com.psolomin.flink;

import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.io.aws2.sqs.SqsIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class FlinkSqsProducer {
    public interface ProducerOpts extends PipelineOptions, FlinkPipelineOptions, AwsOptions {
        @Validation.Required
        @Description("Queue URL for writing.")
        String getOutputQueueUrl();

        void setOutputQueueUrl(String value);

        @Validation.Required
        @Description("Total N of messages to write.")
        Integer getMsgsToWrite();

        void setMsgsToWrite(Integer value);

        @Description("Enable or disable batched writes. Disabled by default.")
        Boolean getEnableBatchedWrites();

        void setEnableBatchedWrites(Boolean value);
    }

    public static void main(String[] args) {
        ProducerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ProducerOpts.class);
        PipelineOptionsValidator.validate(ProducerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);

        PCollection<Long> df =
                p.apply("Generate Sequence", GenerateSequence.from(0).to(opts.getMsgsToWrite()));
        if (!opts.getEnableBatchedWrites()) {
            write(opts, df);
        } else {
            writeBatches(opts, df);
        }
        p.run().waitUntilFinish();
    }

    static PDone write(ProducerOpts opts, PCollection<Long> df) {
        return df.apply(
                        "Prepare SQS message",
                        MapElements.into(TypeDescriptor.of(SendMessageRequest.class))
                                .via(row -> SendMessageRequest.builder()
                                        .queueUrl(opts.getOutputQueueUrl())
                                        .messageBody(row.toString())
                                        .build()))
                .apply("Write to SQS", SqsIO.write());
    }

    static SqsIO.WriteBatches.Result writeBatches(ProducerOpts opts, PCollection<Long> df) {
        return df.apply(
                "Write to SQS",
                SqsIO.<Long>writeBatches((b, row) -> b.messageBody(row.toString()))
                        .to(opts.getOutputQueueUrl()));
    }
}
