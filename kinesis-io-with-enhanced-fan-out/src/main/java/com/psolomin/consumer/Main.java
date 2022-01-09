package com.psolomin.consumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;

public class Main {
    public interface ConsumerOpts extends PipelineOptions, AwsOptions {
        @Validation.Required
        @Description("Kinesis stream to read from")
        String getInputStream();

        void setInputStream(String value);
    }

    public static void main(String[] args) {
        ConsumerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ConsumerOpts.class);
        PipelineOptionsValidator.validate(ConsumerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);

        p.apply("Source", KinesisIO.read()
            .withStreamName(opts.getInputStream())
            .withInitialPositionInStream(InitialPositionInStream.LATEST)
            .withAWSClientsProvider(
                    DefaultAWSCredentialsProviderChain.getInstance(),
                    Regions.fromName(opts.getAwsRegion())
            ))
            .apply("Print", ParDo.of(new LoggerParDo()));

        p.run().waitUntilFinish();
    }
}
