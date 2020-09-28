package com.application;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public class Main {


    public interface PrecisionOptions extends PipelineOptions{
        @Default.String("100000")
        @Description("Give me a precision")
        Long getInputPrecision();
    }


    public static void main(String[] args) {

        PrecisionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PrecisionOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(GenerateSequence.from(0).to(options.getInputPrecision()))
                .apply(Combine.globally(new PIGeneratorFn())) ; //globally e window, adica intoarce Pcollection cu un singur element
        pipeline.run().waitUntilFinish();

    }
}
