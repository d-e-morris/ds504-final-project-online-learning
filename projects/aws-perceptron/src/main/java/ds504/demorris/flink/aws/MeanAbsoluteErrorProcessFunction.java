package ds504.demorris.flink.aws;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class MeanAbsoluteErrorProcessFunction  extends ProcessFunction<Tuple2<Double[],Double[]>, Double> {
    
    private long nbExampleSeen;
    private double absError;
    private int updateSize;

    public MeanAbsoluteErrorProcessFunction(int updateSize){
        this.updateSize = updateSize;
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        nbExampleSeen = 0;
        absError = 0.0;
    }

    @Override
    public void processElement(Tuple2<Double[],Double[]> input, ProcessFunction<Tuple2<Double[],Double[]>,Double>.Context arg1, Collector<Double> collector) throws Exception{
        nbExampleSeen++;
        for (int i = 0; i < input.f0.length; i++){
            absError = absError + Math.abs(input.f1[i] - input.f0[i]);
        }

        if(nbExampleSeen % updateSize == 0){
            System.out.println(nbExampleSeen + ": " + absError/nbExampleSeen);
            collector.collect(absError/nbExampleSeen);
        }
    }
}
