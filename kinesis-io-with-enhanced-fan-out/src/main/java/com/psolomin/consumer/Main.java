package com.psolomin.consumer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisIO;
import org.apache.beam.sdk.io.aws2.kinesis.KinesisRecord;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import software.amazon.kinesis.common.InitialPositionInStream;

public class Main {
    public interface ConsumerOpts extends PipelineOptions, AwsOptions {
        @Validation.Required
        @Description("Kinesis stream to read from")
        String getInputStream();

        void setInputStream(String value);

        @Validation.Required
        @Description("Kinesis consumer to use")
        String getConsumerArn();

        void setConsumerArn(String value);

        @Validation.Required
        @Description("File sink location path")
        String getSinkLocation();

        void setSinkLocation(String value);
    }

    public static void main(String[] args) {
        ConsumerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ConsumerOpts.class);
        PipelineOptionsValidator.validate(ConsumerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);

        PCollection<KinesisRecord> windowedRecords = p.apply(
                        "Source",
                        KinesisIO.read()
                                .withStreamName(opts.getInputStream())
                                // .withConsumerArn(opts.getConsumerArn())
                                .withInitialPositionInStream(InitialPositionInStream.LATEST)
                                .withProcessingTimeWatermarkPolicy())
                .apply(
                        "Fixed windows",
                        Window.<KinesisRecord>into(FixedWindows.of(Duration.standardSeconds(60)))
                                .withAllowedLateness(Duration.ZERO)
                                .discardingFiredPanes()
                                .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardSeconds(60)))));

        KinesisToFilePipeline.write(windowedRecords, opts.getSinkLocation());
        p.run().waitUntilFinish();
    }
}
