
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class ApproxPIBeam {

    private static final Logger __logger = Logger.getLogger("ApproxPIBeam");

    private static final int ITER_COUNT = 1_000_000;

    static class PIGeneratorFn extends Combine.CombineFn<Integer,PIGeneratorFn.Accum,String>{

        public static class Accum implements Serializable{
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
                return 4.0 * insideCount / (totalCount * 1.0);
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
        public Accum addInput(Accum mutableAccumulator, Integer input) {
            double x = -1 + 2 * Math.random();
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

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        ArrayList<Integer> list = new ArrayList<>();

        for(int i = 0; i<ITER_COUNT; i++){
            list.add(i);
        }

        pipeline
                .apply(Create.of(list))
                .apply(Combine.globally(new PIGeneratorFn())) ;
        pipeline.run().waitUntilFinish();




    }
}
