package com.application;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.logging.Logger;

public class GetPI extends PTransform<PCollection<KV<Boolean, Long>>, PCollection<String>> {

    private static final Logger __logger = Logger.getLogger("com.application.ApproxPIBeam");

    static PCollection<String> myTransform(PCollection<KV<Boolean,Long>> input) {

        PCollection<KV<Integer,KVWrapper>> trueCollection = input
                .apply(ParDo.of(new DoFn<KV<Boolean, Long>, KV<Integer,KVWrapper>>() {

                    @ProcessElement
                    public void proccessElement(@Element KV<Boolean, Long> input, OutputReceiver<KV<Integer,KVWrapper>> outputReceiver ){
                        if (input.getKey()){
                            outputReceiver.output(KV.of(0,new KVWrapper(KV.of(input.getKey(), input.getValue()))));
                        }
                    }

                }));

        PCollection<KV<Integer,KVWrapper>> falseCollection = input
                .apply(ParDo.of(new DoFn<KV<Boolean, Long>, KV<Integer,KVWrapper>>() {

                    @ProcessElement
                    public void proccessElement(@Element KV<Boolean, Long> input, OutputReceiver<KV<Integer,KVWrapper>> outputReceiver ){
                        if (!input.getKey()){
                            outputReceiver.output(KV.of(0,new KVWrapper(KV.of(input.getKey(), input.getValue()))));
                        }
                    }

                }));



        TupleTag<KVWrapper> trueTag = new TupleTag<>();
        TupleTag<KVWrapper> falseTag = new TupleTag<>();

        return  KeyedPCollectionTuple
                .of(trueTag,trueCollection)
                .and(falseTag,falseCollection)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(new DoFn<KV<Integer, CoGbkResult>,String>(){

                    @ProcessElement
                    public void processElement(@Element KV<Integer,CoGbkResult> proccessElement,OutputReceiver<String> out){

                        CoGbkResult coGbkResult = proccessElement.getValue();

                        KVWrapper trueValues = coGbkResult.getOnly(trueTag);
                        KVWrapper falseValues = coGbkResult.getOnly(falseTag);

                        System.out.println(falseValues.value.getValue());

                        Double approxPI = 4.0 * trueValues.value.getValue() / ((falseValues.value.getValue() +trueValues.value.getValue()) * 1.0);
                        Double relError =  Math.abs(approxPI - Math.PI) / Math.PI;
                        String result = String.format("^^^^^^^Approx PI is %f, Math lib PI is %f with  error  %f%%", approxPI, Math.PI,100 * relError);
                        __logger.info(result);

                        out.output(result);

                    }


                }));



    }

    @Override
    public PCollection<String> expand(PCollection<KV<Boolean, Long>> input) {
        return myTransform(input);
    }
}