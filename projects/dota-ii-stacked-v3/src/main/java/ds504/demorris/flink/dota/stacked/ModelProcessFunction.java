package ds504.demorris.flink.dota.stacked;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import moa.classifiers.Classifier;
import moa.classifiers.trees.HoeffdingTree;
import moa.core.InstanceExample;
import moa.options.ClassOption;
import moa.core.Utils;

public class ModelProcessFunction extends ProcessFunction<Tuple2<InstanceExample,Long>,Tuple4<Double,Double,Integer,Long>> {

    private Classifier classifier;
    private int updateSize;
    private long nbExampleSeen;
    private boolean outputString;
    private long correct;
    private String name;
    private int stackedIndex;

    public ModelProcessFunction(String cliString, int updateSize, boolean outputString, String name, int stackedIndex){
        initModel(cliString);
        this.updateSize = updateSize;
        this.outputString = outputString;
        this.name = name;
        this.stackedIndex = stackedIndex;
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
    public void processElement(Tuple2<InstanceExample,Long> input, ProcessFunction<Tuple2<InstanceExample,Long>,Tuple4<Double,Double,Integer,Long>>.Context arg1, Collector<Tuple4<Double,Double,Integer,Long>> collector) throws Exception{
        nbExampleSeen++;

        if (classifier.correctlyClassifies(input.f0.getData())){
            correct++;
        }

        //Predict
        Tuple4<Double,Double,Integer,Long> output = new Tuple4<>(
            Double.valueOf(Utils.maxIndex(classifier.getVotesForInstance(input.f0.getData()))),
            input.f0.getData().classValue(),
            stackedIndex,
            input.f1);

        collector.collect(output);

        //Train
        classifier.trainOnInstance(input.f0);

        if((nbExampleSeen % updateSize==0) && outputString){
            System.out.println(this.name + " -- Examples Seen: " + nbExampleSeen + " Accuracy: " + ((double)correct)/nbExampleSeen);
        }
    }
    
}
