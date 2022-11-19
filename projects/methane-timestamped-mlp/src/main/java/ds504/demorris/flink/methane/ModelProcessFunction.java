package ds504.demorris.flink.methane;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import org.neuroph.core.data.DataSet;
import org.neuroph.core.data.DataSetRow;
import org.neuroph.nnet.MultiLayerPerceptron;
//import org.neuroph.nnet.AutoencoderNetwork;
import org.neuroph.nnet.learning.BackPropagation;
// import org.neuroph.util.TransferFunctionType;
// import org.neuroph.core.Layer;
// import org.neuroph.core.Neuron;

public class ModelProcessFunction extends ProcessFunction<Tuple1<DataSet>, Tuple2<Double[],Double[]>> {

    private MultiLayerPerceptron model;
    private BackPropagation learningRule;
    
    //private int features;
    private int outputs;
    private Double[] absError;
    private long nbExampleSeen;

    public ModelProcessFunction(MultiLayerPerceptron model, BackPropagation learningRule){
        this.model = model;
        this.learningRule = learningRule;
        //this.features = model.getInputsCount();
        this.outputs = model.getOutputsCount();
        this.absError = new Double[this.outputs];
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        for(int i=0; i < outputs; i++){
            absError[i] = 0.0;
        }

        nbExampleSeen=0;

        model.setLearningRule(learningRule);
    }

    @Override
    public void processElement(Tuple1<DataSet> instances, ProcessFunction<Tuple1<DataSet>, Tuple2<Double[], Double[]>>.Context context, Collector<Tuple2<Double[],Double[]>> collector) throws Exception{
        
        boolean trainingNecessary = false;

        // Predict
        for(DataSetRow instance: instances.f0){
            nbExampleSeen++;

            model.setInput(instance.getInput());
            model.calculate();
            double[] predictions = model.getOutput();

            double[] actuals = instance.getDesiredOutput();

            collector.collect(new Tuple2<>(
                ArrayUtils.toObject(actuals),
                ArrayUtils.toObject(predictions)
            ));

            for (int i=0; i < outputs; i++){
                absError[i] += Math.abs(predictions[i] - actuals[i]);
                if (Math.abs(predictions[i] - actuals[i]) > 0.1){
                    trainingNecessary = true;
                }
            }

            if (nbExampleSeen % 100_000 == 0){
                System.out.println("Examples seen: " + nbExampleSeen);
                for (int i=0; i < absError.length; i++){
                    System.out.println("Output " + i + " - Mean Absolute Error: " + absError[i]/nbExampleSeen);
                }
            }

        }

        if(trainingNecessary){
            // train
            //learningRule.doOneLearningIteration(instances.f0);
            model.learn(instances.f0);
        }
    }
    
}
