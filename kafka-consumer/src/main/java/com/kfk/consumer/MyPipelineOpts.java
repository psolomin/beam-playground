package com.kfk.consumer;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface MyPipelineOpts extends PipelineOptions {
    @Validation.Required
    @Description("Kafka bootstrap servers")
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Validation.Required
    @Description("Kafka topics to read from - comma-separated")
    String getInputTopics();

    void setInputTopics(String value);
}
