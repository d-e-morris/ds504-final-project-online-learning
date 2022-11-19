package ds504.demorris.flink.aws;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

import com.yahoo.labs.samoa.instances.Instance;

import moa.classifiers.Classifier;
import moa.core.InstanceExample;
import moa.options.ClassOption;

import java.util.Random;
import java.lang.Math;

public class PerceptronProcessFunction extends ProcessFunction<Tuple2<Double[],Double[]>, Tuple2<Double[],Double[]>> {

    protected int features;
    protected int outputs;
    protected double[][] weights;
    protected Double[] predictions;
    protected Double learningRate;
    protected Double[] absError;
    protected int randMax;

    protected long nbExampleSeen;

    public PerceptronProcessFunction(int features, int outputs, double learningRate){
        this.features=features;
        this.outputs=outputs;
        this.weights = new double[outputs][features+1];
        this.predictions = new Double[outputs];
        this.absError = new Double[outputs];
        this.learningRate=learningRate;
    }
    
    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {

        // instantiate the weights with a value between 0 and randMax
        Random randGenerator = new Random();
        for (int i=0; i<outputs; i++){
            for (int j=0; j<features + 1; j++){
                //weights[i][j] = randGenerator.nextDouble();
                weights[i][j] = 0.1;
            }
            predictions[i] = 0.0;
            absError[i] = 0.0;
        }
        this.nbExampleSeen = 0;

        
    }

    @Override
    public void processElement(Tuple2<Double[],Double[]> record, ProcessFunction<Tuple2<Double[],Double[]>, Tuple2<Double[],Double[]>>.Context arg1, Collector<Tuple2<Double[],Double[]>> collector) throws Exception{
        nbExampleSeen++;

        Double[] error = new Double[outputs];
        for(int i=0; i < outputs; i++){
            error[i] = 0.0;
        }

        //predict
        for (int i=0; i < outputs; i++){
            double prediction = weights[i][features];
            for (int j=0; j<features; j++){
                prediction = prediction + record.f0[j] * weights[i][j];
            }
            predictions[i] = prediction;
            error[i] = record.f1[i] - prediction;
            absError[i] = absError[i] + Math.abs(record.f1[i]-prediction);
        }
        collector.collect(new Tuple2(record.f1,predictions));

        //train
        for (int i=0; i < outputs; i++){
            weights[i][features] = weights[i][features] + learningRate*error[i];
            for(int j=0; j < features; j++){
                weights[i][j] = weights[i][j] + learningRate * error[i] * record.f0[j];
            }
        }

        
        // if (nbExampleSeen % 100_000 == 0){
        //     //System.out.println("Examples seen: " + nbExampleSeen);
        //     for (int i=0; i<outputs; i++){
        //         System.out.println("Output " + i + "- Mean Absolute Error: " + absError[i]/nbExampleSeen);
        //     }
        // }
        
    }


}