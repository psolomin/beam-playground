package com.kfk.producer;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ProducerOpts extends PipelineOptions {
    @Validation.Required
    @Description("Kafka bootstrap servers")
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Validation.Required
    @Description("Kafka topic to write to")
    String getOutputTopic();

    void setOutputTopic(String value);

    @Validation.Required
    @Description("Total N of messages to write")
    Integer getMsgsToWrite();

    void setMsgsToWrite(Integer value);

    @Validation.Required
    @Description("Rate of producing - N of msgs / sec")
    Integer getMsgsPerSec();

    void setMsgsPerSec(Integer value);
}
