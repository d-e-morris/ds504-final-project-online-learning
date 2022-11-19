package ds504.demorris.flink.methane.mlp.autoencoded;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

import org.neuroph.core.data.DataSet;
import org.neuroph.core.data.DataSetRow;
import org.neuroph.nnet.MultiLayerPerceptron;
import org.neuroph.nnet.AutoencoderNetwork;
import org.neuroph.nnet.learning.BackPropagation;
import org.neuroph.util.TransferFunctionType;
import org.neuroph.core.Layer;
import org.neuroph.core.Neuron;

public class MLPProcessFunction extends ProcessFunction<DataSet,Tuple2<Double[],Double[]>> {
    
    protected MultiLayerPerceptron model;
    protected MultiLayerPerceptron autoencoder;
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
        
        autoencoder = new MultiLayerPerceptron(TransferFunctionType.RECTIFIED,28,16,8,4,8,16,28);

        BackPropagation autoLearningRule = new BackPropagation();
        autoLearningRule.setLearningRate(0.01);
        autoLearningRule.setMaxIterations(1);
        autoencoder.setLearningRule(autoLearningRule);
    }

    @Override
    public void processElement(DataSet instances, ProcessFunction<DataSet,Tuple2<Double[],Double[]>>.Context arg1, Collector<Tuple2<Double[],Double[]>> collector) throws Exception{

        DataSet autoInstances = new DataSet(28,28);
        DataSet modelInstances = new DataSet(4,3);

        for(DataSetRow instance: instances.getRows()){

            nbExampleSeen++;

            Double[] error = new Double[model.getOutputsCount()];
            Double[] actuals = ArrayUtils.toObject(instance.getDesiredOutput());

            autoInstances.add(new DataSetRow(instance.getInput(), instance.getInput()));

            // Predict
            autoencoder.setInput(instance.getInput());
            autoencoder.calculate();
            Layer outputLayer = autoencoder.getLayerAt(3);
            double[] autoOutput = new double[outputLayer.getNeuronsCount()-1];
            for (int i=0; i<autoOutput.length; i++){
                autoOutput[i] = outputLayer.getNeuronAt(i).getOutput();
            }

            modelInstances.add(new DataSetRow(autoOutput,instance.getDesiredOutput()));
            model.setInput(autoOutput);
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

            if (nbExampleSeen % 100_000 == 0){
                System.out.println("Examples seen: " + nbExampleSeen);
                for (int i=0; i<error.length; i++){
                    System.out.println("Output " + i + " - Mean Absolute Error: " + absError[i]/nbExampleSeen*100);
                }
            }
        }

        autoencoder.learn(autoInstances);
        model.learn(modelInstances);
        
    }
}
