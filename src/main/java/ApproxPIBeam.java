
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

    private static final int ITER_COUNT = 1_000_000;

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
