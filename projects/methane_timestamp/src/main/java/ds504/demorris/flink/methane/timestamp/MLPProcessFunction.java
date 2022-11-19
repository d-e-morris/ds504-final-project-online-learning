package ds504.demorris.flink.methane.timestamp;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import org.neuroph.core.data.DataSet;
import org.neuroph.core.data.DataSetRow;
import org.neuroph.nnet.MultiLayerPerceptron;
import org.neuroph.nnet.learning.BackPropagation;

public class MLPProcessFunction extends ProcessFunction<DataSet,Tuple2<Double[],Double[]>> {
    
    protected MultiLayerPerceptron model;
    protected BackPropagation learningRule;
    protected Double learningRate;
    protected int features;
    protected int outputs;
    protected Double[] predictions;
    protected Double[] absError;
    protected long nbExampleSeen;

    public MLPProcessFunction(MultiLayerPerceptron model, BackPropagation learningRule){
        this.model=model;
        this.learningRule=learningRule;
        this.features=model.getInputsCount();
        this.outputs=model.getOutputsCount();
        this.predictions = new Double[this.outputs];
        this.absError = new Double[this.outputs];
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        for (int i=0; i<outputs; i++){
            predictions[i] = 0.0;
            absError[i] = 0.0;
        }
        this.nbExampleSeen=0;

        model.setLearningRule(learningRule);        
    }

    @Override
    public void processElement(DataSet instances, ProcessFunction<DataSet,Tuple2<Double[],Double[]>>.Context arg1, Collector<Tuple2<Double[],Double[]>> collector) throws Exception{


        for(DataSetRow instance: instances.getRows()){

            nbExampleSeen++;

            Double[] error = new Double[model.getOutputsCount()];
            Double[] actuals = ArrayUtils.toObject(instance.getDesiredOutput());

            // Predict
            model.setInput(instance.getInput());
            model.calculate();
            predictions = ArrayUtils.toObject(model.getOutput());
            collector.collect(
                new Tuple2<>(
                    actuals,
                    predictions
                )
            );
            for (int i = 0; i < error.length; i++){
                error[i] = predictions[i] - actuals[i];
                absError[i] += Math.abs(error[i]);
            }

            if (nbExampleSeen % 10_000 == 0){
                System.out.println("Examples seen: " + nbExampleSeen);
                for (int i=0; i<error.length; i++){
                    System.out.println("Output " + i + " - Mean Absolute Error: " + absError[i]/nbExampleSeen);
                }
            }
        }

        model.learn(instances);
        
    }
}
