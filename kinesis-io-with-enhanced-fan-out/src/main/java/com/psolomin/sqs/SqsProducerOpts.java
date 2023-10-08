package com.psolomin.sqs;

import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface SqsProducerOpts extends PipelineOptions, AwsOptions {
    @Validation.Required
    @Description("Queue URL for writing.")
    String getOutputQueueUrl();

    void setOutputQueueUrl(String value);

    @Validation.Required
    @Description("Total N of messages to write.")
    Integer getMsgsToWrite();

    void setMsgsToWrite(Integer value);

    @Default.Boolean(false)
    @Description("Enable or disable batched writes. Disabled by default.")
    Boolean getEnableBatchedWrites();

    void setEnableBatchedWrites(Boolean value);
}
