package com.psolomin.producer;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.options.*;

public class Main {
    public interface ProducerOpts extends PipelineOptions, AwsOptions {
        @Validation.Required
        @Description("Kinesis stream for writing")
        String getOutputStream();

        void setOutputStream(String value);

        @Validation.Required
        @Description("Total N of messages to write")
        Integer getMsgsToWrite();

        void setMsgsToWrite(Integer value);

        @Validation.Required
        @Description("Rate of producing - N of msgs / sec")
        Integer getMsgsPerSec();

        void setMsgsPerSec(Integer value);
    }

    public static void main(String[] args) {
        ProducerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ProducerOpts.class);
        PipelineOptionsValidator.validate(ProducerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);
        p.run().waitUntilFinish();
    }
}
