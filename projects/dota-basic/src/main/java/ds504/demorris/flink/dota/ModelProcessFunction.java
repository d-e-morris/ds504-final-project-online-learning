package ds504.demorris.flink.dota;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import moa.classifiers.Classifier;
import moa.classifiers.trees.HoeffdingTree;
import moa.core.InstanceExample;
import moa.options.ClassOption;
import moa.core.Utils;

public class ModelProcessFunction extends ProcessFunction<InstanceExample, Tuple2<Double,Double>> {

    private Classifier classifier;
    private int updateSize;
    private long nbExampleSeen;
    private boolean outputString;
    private long correct;
    private String name;

    public ModelProcessFunction(String cliString, int updateSize, boolean outputString, String name){
        initModel(cliString);
        this.updateSize = updateSize;
        this.outputString = outputString;
        this.name = name;
    }

    private void initModel(String cliString){
        try{
            this.classifier = (Classifier)ClassOption.cliStringToObject(cliString, Classifier.class, null);
        } catch(Exception c){
            System.out.println("ERROR - CANNOT CREATE CLASSIFIER FROM STRING: " + cliString + " --- falling back to hoeffding default");
            this.classifier = new HoeffdingTree();
        }
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        nbExampleSeen=0;
        correct=0;
        classifier.prepareForUse();
    }

    @Override
    public void processElement(InstanceExample input, ProcessFunction<InstanceExample, Tuple2<Double,Double>>.Context arg1, Collector<Tuple2<Double,Double>> collector) throws Exception{
        nbExampleSeen++;

        if (classifier.correctlyClassifies(input.getData())){
            correct++;
        }

        //Predict
        Tuple2<Double,Double> output = new Tuple2<>(
            Double.valueOf(Utils.maxIndex(classifier.getVotesForInstance(input.getData()))),
            input.getData().classValue());

        collector.collect(output);

        //Train
        classifier.trainOnInstance(input);

        if((nbExampleSeen % updateSize==0) && outputString){
            System.out.println(this.name + " -- Examples Seen: " + nbExampleSeen + " Accuracy: " + ((double)correct)/nbExampleSeen);
        }
    }
    
}

