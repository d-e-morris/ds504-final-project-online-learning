package ds504.demorris;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import moa.core.InstanceExample;
import moa.classifiers.Classifier;
import moa.classifiers.functions.MajorityClass;

import com.yahoo.labs.samoa.instances.MultiLabelPrediction;


public class ModelStacker implements CoFlatMapFunction<Tuple3<InstanceExample,Boolean[],Double>, Classifier, Tuple3<InstanceExample,Boolean[],Double>>{

    private int outputSize;
    private Classifier classifier;

    public ModelStacker(int outputSize){
        this.outputSize=outputSize;
        this.classifier = new MajorityClass();
        this.classifier.prepareForUse();
    }

    @Override
    public void flatMap1(Tuple3<InstanceExample, Boolean[],Double> input, Collector<Tuple3<InstanceExample,Boolean[],Double>> collector) throws Exception{
        Boolean[] outPred = new Boolean[outputSize];
        for (int i=0; i < outputSize-1; i++){
            outPred[i] = input.f1[i];
        }
        if (classifier.trainingHasStarted()){
            //outPred[outputSize-1] = classifier.correctlyClassifies(input.f0.getData());
            double[] prediction = classifier.getPredictionForInstance(input.f0.getData()).getVotes();
            if (prediction.length < 2){
                System.out.println("Not enough outputs");
            }
            if(prediction[0] >= prediction[1]){
                outPred[outputSize-1] = true;
            } else {
                outPred[outputSize-1] = false;
            }
            collector.collect(new Tuple3<>(input.f0,outPred,input.f2));
        }      
    }

    public void flatMap2(Classifier classifier, Collector<Tuple3<InstanceExample,Boolean[],Double>> collector) throws Exception{
        this.classifier=classifier;
    }
    
}
