package ds504.demorris.flink.methane;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.neuroph.core.data.DataSet;
import org.neuroph.core.data.DataSetRow;

public class CreateWindowedInstance extends ProcessAllWindowFunction<Tuple3<Double[], Double[], Long>, Tuple1<DataSet>, TimeWindow> {

    private int windowSizeSeconds;
    private int numAttributesPerInstance;
    private int numTargets;
    
    public CreateWindowedInstance(int windowSizeSeconds, int attributesPerInstance, int numTargets){
        this.windowSizeSeconds = windowSizeSeconds;
        this.numAttributesPerInstance = attributesPerInstance;
        this.numTargets = numTargets;
    }

    public void process(ProcessAllWindowFunction<Tuple3<Double[],Double[],Long>,Tuple1<DataSet>,TimeWindow>.Context context, 
        Iterable<Tuple3<Double[],Double[],Long>> inputs, Collector<Tuple1<DataSet>> collector) throws Exception
    {
        int recordsCount = 0;
        double[] features = new double[windowSizeSeconds*numAttributesPerInstance];
        double[] targets = new double[numTargets];

        for(Tuple3<Double[],Double[],Long> input: inputs){
            for(int i=0; i < numAttributesPerInstance; i++){
                features[recordsCount*numAttributesPerInstance+i] = input.f0[i]/2020.0;
            }
            recordsCount++;
            if (recordsCount == windowSizeSeconds){
                for (int i=0; i < numTargets; i++){
                    targets[i] = input.f1[i];
                }
                DataSet trainingData = new DataSet(windowSizeSeconds*numAttributesPerInstance,numTargets);
                trainingData.add(new DataSetRow(features,targets));
                collector.collect(new Tuple1<>(trainingData));
            }
        }

        if (recordsCount < windowSizeSeconds){
            System.out.println(context.window().getEnd() + " - Skipping window. Only " + recordsCount + " instances in " + windowSizeSeconds + " seconds window.");
        }

    }
}
