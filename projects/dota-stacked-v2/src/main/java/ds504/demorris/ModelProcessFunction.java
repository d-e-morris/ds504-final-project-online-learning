package ds504.demorris;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import moa.classifiers.Classifier;
import moa.classifiers.trees.HoeffdingTree;
import moa.core.InstanceExample;
import moa.options.ClassOption;
import moa.classifiers.functions.Perceptron;

public class ModelProcessFunction extends ProcessFunction <Tuple3<InstanceExample,Boolean[],Double>, Classifier> {

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
        } catch(Exception e){
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
    public void processElement(Tuple3<InstanceExample,Boolean[],Double> recordFull, ProcessFunction<Tuple3<InstanceExample,Boolean[],Double>, Classifier>.Context arg1, Collector<Classifier> collector) throws Exception{
        nbExampleSeen++;
        InstanceExample record = recordFull.f0;
        
        if (classifier.correctlyClassifies(record.getData())){
            correct++;
        }

        classifier.trainOnInstance(record);
        if(nbExampleSeen % updateSize == 0){
            collector.collect(classifier);

            if (outputString){
                System.out.println(this.name + " -- Examples Seen: " + nbExampleSeen + " Accuracy: " + ((double)correct)/nbExampleSeen);
            }
        }

        // if (name.equals("OutputLayer")){
        //     System.out.println(name + " - " + recordFull + " -- Seen: " + nbExampleSeen + " Correct: " + correct);
        // }

    }

    @Override
    public void close() throws Exception{
        if (name.equals("OutputLayer")){
            System.out.println("rest");
        }
    }
    
}
