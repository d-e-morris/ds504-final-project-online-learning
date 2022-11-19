package ds504.demorris.airline.hoeffding;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

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
    private int correct;
    private double accuracy;
    private double alpha;
    private double running_alpha;
    private String cliString;

    public ModelProcessFunction(String cliString, Class<? extends Classifier> model, int updateSize, double alpha){
        this.cliString=cliString;
        this.model=model;
        this.updateSize=updateSize;
        this.nbExampleSeen=0;
        this.correct=0;
        this.accuracy = 0;
        this.alpha = alpha;
        this.running_alpha = alpha;
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        try{
            classifier = (Classifier)ClassOption.cliStringToObject(this.cliString,this.model,null);
        } catch(Exception e) {
            classifier = new HoeffdingTree();
        }
        classifier.prepareForUse();
        System.out.println(classifier.getCLICreationString(model));   
    }

    @Override
    public void processElement(InstanceExample record, ProcessFunction<InstanceExample, Classifier>.Context arg1, Collector<Classifier> collector) throws Exception{
        nbExampleSeen++;
        correct = 0;
        if (classifier.correctlyClassifies(record.getData())){
            correct = 1;
        }
        accuracy = alpha * accuracy + (1-alpha) * correct;
        double corrected_accuracy = accuracy / (1 - running_alpha);
        running_alpha *= running_alpha;
        classifier.trainOnInstance(record);
        if(nbExampleSeen % updateSize == 0){
            collector.collect(classifier);
        }
        if (nbExampleSeen % 10_000 == 0){
            System.out.println("Examples Seen: " + nbExampleSeen + " Accuracy: " + corrected_accuracy);
        }
    }
}
