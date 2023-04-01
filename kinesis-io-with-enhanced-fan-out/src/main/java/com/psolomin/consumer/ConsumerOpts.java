package com.psolomin.consumer;

import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ConsumerOpts extends PipelineOptions, AwsOptions {
    @Validation.Required
    @Description("Kinesis stream to read from")
    String getInputStream();

    void setInputStream(String value);

    @Default.String("LATEST")
    @Description("Start mode for Kinesis consumer - LATEST, etc."
            + "When restoring from a savepoint, this config will be ignored.")
    String getStartMode();

    void setStartMode(String value);

    @Description("Start timestamp, in the format of 2007-12-03T10:15:30.00Z. Used only if startMode=AT_TIMESTAMP")
    String getStartTs();

    void setStartTs(String value);

    @Validation.Required
    @Description("File sink location path")
    String getSinkLocation();

    void setSinkLocation(String value);

    @Default.Long(0L)
    @Description("Artificial 'processing' time to simulate back-pressure etc")
    Long getProcessTimePerRecord();

    void setProcessTimePerRecord(Long value);

    @Default.Integer(0)
    @Description("The pipeline will fail after consuming every N records. 0 means never fail")
    Integer getFailAfterRecordsSeenCnt();

    void setFailAfterRecordsSeenCnt(Integer value);
}
