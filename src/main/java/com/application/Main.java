package com.application;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.logging.Logger;

public class Main {

    private static final Logger __logger = Logger.getLogger("com.application.ApproxPIBeam");

    public interface PrecisionOptions extends PipelineOptions{
        @Default.Long(100000)
        @Description("Give me a precision")
        Long getInputPrecision();
        void setInputPrecision(Long value);
    }


    public static void PiWithCombine(PrecisionOptions options){

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(GenerateSequence.from(0).to(options.getInputPrecision()))
                .apply(Combine.globally(new PIGeneratorFn())) ;

        pipeline.run().waitUntilFinish();
    }

    public static void PiwithGroupBy(PrecisionOptions options){
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(GenerateSequence.from(0).to(options.getInputPrecision()))
                .apply(Combine.globally(new PIGeneratorFn())) ;

        pipeline.run().waitUntilFinish();
    }

    public static Boolean getHit(){

        double x = -1 + 2 * Math.random();                         // o pCollection de boolean, cu group numeram cate au nimerit tinta,
        double y = -1 + 2 * Math.random();
        if (x * x + y * y <= 1.0) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        PrecisionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PrecisionOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(GenerateSequence.from(0).to(options.getInputPrecision()))
                .apply(MapElements.into(TypeDescriptors.booleans())
                .via(element -> getHit()))
                .apply(Count.perElement());
            /*
                .apply(ParDo.of(new DoFn<KV<Boolean, Long>, Object>() {

                    @ProcessElement
                    public void processElement(@Element KV<Boolean,Long> input, OutputReceiver<Object> outputReceiver){
                        __logger.info(String.format("Combine tn %s %f", input.getKey(),input.getValue()));

                    }
                }));
        */
        pipeline.run().waitUntilFinish();


    }
}
