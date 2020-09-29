package com.application;

import org.apache.beam.sdk.transforms.Combine;

import java.io.Serializable;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

class PIGeneratorFn extends Combine.CombineFn<Long,PIGeneratorFn.Accum,String>{

    private static final Logger __logger = Logger.getLogger("com.application.ApproxPIBeam");

    public static class Accum implements Serializable {
        private final int insideCount;
        private final int totalCount;

        private Accum(int insideCount, int totalCount){
            this.insideCount = insideCount;
            this.totalCount = totalCount;
        }

        @Override
        public int hashCode() {
            return Objects.hash(insideCount,totalCount);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj){
                return true;
            }

            if(obj == null || getClass() != obj.getClass()){
                return false;
            }

            Accum accum = (Accum) obj;
            return this.insideCount == accum.insideCount && this.totalCount == accum.totalCount;
        }

        Accum newFromInsideEvent() {
            return new Accum(insideCount + 1, totalCount + 1);
        }

        Accum newFromOutsideEvent() {
            return new Accum(insideCount, totalCount + 1);
        }

        Accum combineWith(Accum other) {
            if (__logger.isLoggable(Level.INFO))
                __logger.info(String.format("Combine tn %s %f", Thread.currentThread().getName() ,other.approxPI()));
            return new Accum(insideCount + other.insideCount, totalCount + other.totalCount);
        }

        double approxPI() {
            return 4.0 * insideCount / (totalCount * 1.0);  // de aici iese val lui pi ( insideCount = numar de True, totalCount numar de false)
        }

        double relError() {
            return Math.abs(approxPI() - Math.PI) / Math.PI;
        }

    }


    @Override
    public Accum createAccumulator() {
        return new Accum(1,1);
    }

    @Override
    public Accum addInput(Accum mutableAccumulator, Long input) { // fara COMBINEFN, fara ACCUm, KV cu <boolean>
        double x = -1 + 2 * Math.random();                         // o pCollection de boolean, cu group numeram cate au nimerit tinta,
        double y = -1 + 2 * Math.random();
        if (x * x + y * y <= 1.0) {
            return mutableAccumulator.newFromInsideEvent();
        }
        return mutableAccumulator.newFromOutsideEvent();
    }

    @Override
    public Accum mergeAccumulators(Iterable<Accum> accumulators) {

        Accum accum = createAccumulator();
        for(Accum accum1: accumulators){
            accum = accum.combineWith(accum1);
        }
        return accum;
    }

    @Override
    public String extractOutput(Accum accumulator) {

        String result = String.format("^^^^^^^Approx PI is %f, Math lib PI is %f with  error  %f%%", accumulator.approxPI(), Math.PI,100 * accumulator.relError());
        __logger.info(result);
        return result;
    }
}