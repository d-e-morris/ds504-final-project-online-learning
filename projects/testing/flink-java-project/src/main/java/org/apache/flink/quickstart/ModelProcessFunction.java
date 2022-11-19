package org.apache.flink.quickstart;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import com.yahoo.labs.samoa.instances.Instance;

import moa.classifiers.Classifier;
import moa.core.InstanceExample;
import moa.options.ClassOption;
import moa.classifiers.trees.HoeffdingTree;

public class ModelProcessFunction extends ProcessFunction<InstanceExample, Classifier> {
    
    private static final long serialVersionUID = 1L;
    private Class<? extends Classifier> model;
    private Classifier classifier;
    private int updateSize;
    private long nbExampleSeen;
    private long correct;

    public ModelProcessFunction(Class<? extends Classifier> model, int updateSize){
        this.model=model;
        this.updateSize=updateSize;
        this.nbExampleSeen=0;
        this.correct=0;
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        try{
            classifier = (Classifier)ClassOption.cliStringToObject("HoeffdingTree",this.model,null);
        } catch(Exception e) {
            classifier = new HoeffdingTree();
        }
        classifier.prepareForUse();
        System.out.println(classifier.getCLICreationString(model));   
    }

    @Override
    public void processElement(InstanceExample record, ProcessFunction<InstanceExample, Classifier>.Context arg1, Collector<Classifier> collector) throws Exception{
        nbExampleSeen++;
        if (classifier.correctlyClassifies(record.getData())){
            correct++;
        }
        classifier.trainOnInstance(record);
        if(nbExampleSeen % updateSize == 0){
            collector.collect(classifier);
        }
        if (nbExampleSeen % 10_000 == 0){
            System.out.println("Examples Seen: " + nbExampleSeen + " Accuracy: " + ((double)correct)/nbExampleSeen);
        }
    }
}
