package com.application;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.*;

import java.util.logging.Logger;

import static com.application.PiCalculator.calculatePI;

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
                .apply(MapElements.into(TypeDescriptors.booleans())
                        .via(element -> getHit()))
                .apply(Count.perElement())
                .apply(new GetPI());

        //   .apply(MapElements.into(TypeDescriptor.of(KVWrapper.class))
            //     .via(element -> new KVWrapper(KV.of(element.getKey(),element.getValue()))))
     //    .apply(Combine.globally(new GetResultFn()));

              //  .apply(Combine.globally((KV<Boolean,Long> x, KV<Boolean,Long> y) ->
                        
                //    4.0 * x.getValue() / (y.getValue() * 1.0);
                //   Double  relError =  Math.abs(approxPI - Math.PI) / Math.PI;
                  //   String.format("^^^^^^^Approx PI is %f, Math lib PI is %f with  error  %f%%", approxPI, Math.PI,100 * relError);
              //      KV.of( 4.0 * x.getValue() / (y.getValue() * 1.0),Math.abs(4.0 * x.getValue() / (y.getValue() * 1.0) - Math.PI) / Math.PI)

            //    ));
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



    public static void PiSimpleWay(PrecisionOptions options){
        Pipeline pipeline = Pipeline.create(options);

        final PCollectionView<Long> totalHitsView = pipeline.apply(Create.of(options.getInputPrecision()))
                                                        .apply(View.asSingleton());

        pipeline
                .apply(GenerateSequence.from(0).to(options.getInputPrecision()))
                .apply(MapElements.into(TypeDescriptors.booleans())
                        .via(element -> getHit()))
                .apply(Count.perElement())
                .apply(Filter.by(KV::getKey))
                .apply(MapElements.into(TypeDescriptor.of(KVWrapper.class))
                        .via(element -> new KVWrapper(KV.of(element.getKey(),element.getValue()))))
               .apply(ParDo.of(new DoFn<KVWrapper, String>(){
                   @ProcessElement
                   public void processElement(@Element KVWrapper input, OutputReceiver<String> outputReceiver, ProcessContext ctx){
                       Double approxPI = 4.0 * input.value.getValue() / (ctx.sideInput(totalHitsView) * 1.0);
                       Double  relError =  Math.abs(approxPI - Math.PI) / Math.PI;
                       String result = String.format("^^^^^^^Approx PI is %f, Math lib PI is %f with  error  %f%%", approxPI, Math.PI,100 * relError);
                       __logger.info(result);
                       outputReceiver.output(result);


                   }
               }).withSideInput("Total hits", totalHitsView));


        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        PrecisionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PrecisionOptions.class);

        //PiWithCombine(options);
        //PiwithGroupBy(options);
        PiSimpleWay(options);


    }
}
