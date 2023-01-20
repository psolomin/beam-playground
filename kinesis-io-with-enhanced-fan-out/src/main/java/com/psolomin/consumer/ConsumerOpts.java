package com.psolomin.consumer;

import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

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
