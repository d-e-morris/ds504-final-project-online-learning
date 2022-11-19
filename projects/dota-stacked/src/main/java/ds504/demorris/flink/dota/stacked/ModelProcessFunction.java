package ds504.demorris.flink.dota.stacked;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import moa.classifiers.Classifier;
import moa.classifiers.trees.HoeffdingTree;
import moa.core.InstanceExample;
import moa.options.ClassOption;

public class ModelProcessFunction extends ProcessFunction <InstanceExample, Tuple3<Integer,Classifier,Integer>> {

    private Classifier classifier;
    private int updateSize;
    private long nbExampleSeen;
    private boolean outputString;
    private long correct;
    private String name;
    private int classifierIndex;
    private int versionNumber;

    public ModelProcessFunction(String cliString, int UpdateSize, int classifierIndex){
        initModel(cliString);
        this.updateSize = updateSize;
        this.name = cliString;
        this.outputString = false;
        this.classifierIndex = classifierIndex;
    }

    public ModelProcessFunction(String cliString, int updateSize, int classifierIndex, String name){
        initModel(cliString);
        this.updateSize = updateSize;
        this.name=name;
        this.outputString = false;
        this.classifierIndex = classifierIndex;
    }
    
    public ModelProcessFunction(String cliString, int updateSize, int classifierIndex, boolean outputString){
        initModel(cliString);
        this.updateSize = updateSize;
        this.outputString = outputString;
        this.name = cliString;
        this.classifierIndex = classifierIndex;
    }

    public ModelProcessFunction(String cliString, int updateSize, int classifierIndex, boolean outputString, String name){
        initModel(cliString);
        this.updateSize = updateSize;
        this.outputString = outputString;
        this.name = name;
        this.classifierIndex = classifierIndex;
    }

    private void initModel(String cliString){
        try{
            this.classifier = (Classifier)ClassOption.cliStringToObject(cliString, Classifier.class, null);
        } catch(Exception e){
            System.out.println("ERROR - CANNOT CREATE CLASSIFIER FROM STRING: " + cliString + " --- falling back to hoeffding default");
            this.classifier = new HoeffdingTree();
        }
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        nbExampleSeen=0;
        correct=0;
        versionNumber=0;
        classifier.prepareForUse();
    }

    @Override
    public void processElement(InstanceExample record, ProcessFunction<InstanceExample, Tuple3<Integer, Classifier,Integer>>.Context arg1, Collector<Tuple3<Integer,Classifier, Integer>> collector) throws Exception{
        nbExampleSeen++;
        if (classifier.correctlyClassifies(record.getData())){
            correct++;
        }

        classifier.trainOnInstance(record);
        if(nbExampleSeen % updateSize == 0){
            versionNumber++;
            collector.collect(new Tuple3<>(this.classifierIndex,classifier,versionNumber));

            if (outputString){
                System.out.println(this.name + " -- Examples Seen: " + nbExampleSeen + " Accuracy: " + ((double)correct)/nbExampleSeen);
            }
        }

    }
    
}
