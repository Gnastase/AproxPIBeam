package com.application;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

import java.util.logging.Logger;

 class GetResultFn implements SerializableFunction<Iterable<KVWrapper>, String> {

    private static final Logger __logger = Logger.getLogger("com.application.ApproxPIBeam");

     @Override
     public String apply(Iterable<KVWrapper> input) {
         Long insideCount = 0L;
         Long totalCount = 0L;
         Double approxPI = 0d;
         Double relError = 0d;

         for(KVWrapper step : input){

             if(step.value.getKey() == true){
                 insideCount =  step.value.getValue();
             } else {
                 totalCount = step.value.getValue();
             }

         }
         approxPI = 4.0 * insideCount / (totalCount * 1.0);
         relError =  Math.abs(approxPI - Math.PI) / Math.PI;
         String result = String.format("^^^^^^^Approx PI is %f, Math lib PI is %f with  error  %f%%", approxPI, Math.PI,100 * relError);
         __logger.info(result);

         return result;
     }
}

