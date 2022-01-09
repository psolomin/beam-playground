package com.psolomin.producer;

import static com.psolomin.producer.Producer.buildProducerP;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.io.kinesis.KinesisPartitioner;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.options.Validation;

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

    private static final class RandomPartitioner implements KinesisPartitioner {
        @Override
        public String getPartitionKey(byte[] value) {
            return String.valueOf(Arrays.hashCode(value));
        }

        @Override
        public String getExplicitHashKey(byte[] value) {
            return null;
        }
    }

    public static void main(String[] args) {
        ProducerOpts opts = PipelineOptionsFactory.fromArgs(args).as(ProducerOpts.class);
        PipelineOptionsValidator.validate(ProducerOpts.class, opts);
        Pipeline p = Pipeline.create(opts);

        buildProducerP(p, opts).apply(KinesisIO.write()
            .withStreamName(opts.getOutputStream())
            .withPartitioner(new RandomPartitioner())
            .withAWSClientsProvider(
                    DefaultAWSCredentialsProviderChain.getInstance(),
                    Regions.fromName(opts.getAwsRegion())
            ));

        p.run().waitUntilFinish();
    }
}
