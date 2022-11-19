package ds504.demorris.flink.dota.stacked;

import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

import moa.classifiers.Classifier;
import moa.classifiers.functions.Perceptron;
import moa.core.InstanceExample;
import moa.options.ClassOption;
import moa.core.FastVector;
import moa.core.Utils;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;

public class StackedModelProcessFunction extends ProcessAllWindowFunction<Tuple4<Double,Double,Integer,Long>, Tuple2<Double,Double>, TimeWindow> {

    protected InstancesHeader streamHeader;

    private Classifier classifier;
    private int numClassifiers;
    private int updateSize;
    private long nbExampleSeen;
    private boolean outputString;
    private long correct;
    private String name;

    public StackedModelProcessFunction(String cliString, int numClassifiers, boolean outputString, String name, int updateSize){
        initModel(cliString);
        this.numClassifiers = numClassifiers;
        this.outputString = outputString;
        this.name = name;
        this.updateSize=updateSize;
        generateHeader();
    }

    private void initModel(String cliString){
        try{
            this.classifier = (Classifier)ClassOption.cliStringToObject(cliString, Classifier.class, null);
        } catch(Exception c){
            System.out.println("ERROR - CANNOT CREATE CLASSIFIER FROM STRING: " + cliString + " --- falling back to hoeffding default");
            this.classifier = new Perceptron();
        }
    }

    protected void generateHeader(){
        FastVector attributes = new FastVector<>();
        for(int i=0; i < this.numClassifiers; i++){
            attributes.addElement(new Attribute("att"+(i+1)));
        }

        FastVector classLabels = new FastVector<>();
        classLabels.addElement("Win");
        classLabels.addElement("Lose");
        attributes.addElement(new Attribute("class",classLabels));

        this.streamHeader = new InstancesHeader(new Instances("DOTA",attributes,0));
        this.streamHeader.setClassIndex(this.streamHeader.numAttributes()-1);
    }

    public InstancesHeader getHeader(){
        return this.streamHeader;
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        nbExampleSeen = 0;
        correct = 0;
        classifier.prepareForUse();
    }

    @Override
    public void process(ProcessAllWindowFunction<Tuple4<Double,Double,Integer,Long>, Tuple2<Double,Double>,TimeWindow>.Context arg1, Iterable<Tuple4<Double,Double,Integer,Long>> inputs, Collector<Tuple2<Double,Double>> collector) throws Exception {
        nbExampleSeen++;

        ///////////////////////////////////
        //Convert grouped items to instance
        ///////////////////////////////////
        // Start by creating an empty double array
        double[] features = new double[numClassifiers+1];
        double target = 0.0;
        int count = 0;
        for(Tuple4<Double,Double,Integer,Long> input: inputs){
            features[input.f2] = input.f0;
            target = input.f1;
            count++;
        }

        // Do a quick error check
        if(count < numClassifiers){
            System.out.println("ERROR - NOT ENOUGH INSTANCES IN STREAM");
        }

        Instance inst = new DenseInstance(1.0, features);
        inst.setDataset(getHeader());
        inst.setClassValue(target);

        //////////////////////////////////////
        //Predict
        //////////////////////////////////////
        collector.collect(
            new Tuple2<>(
                target,
                Double.valueOf(
                    Utils.maxIndex(
                        classifier.getVotesForInstance(
                            inst
                        )
                    )
                )
            )
        );

        if (classifier.correctlyClassifies(inst)){
            correct++;
        }

        //////////////////////////////////////
        //Train
        //////////////////////////////////////
        classifier.trainOnInstance(inst);

        if((nbExampleSeen % updateSize==0) && outputString){
            System.out.println(this.name + " -- Examples Seen: " + nbExampleSeen + " Accuracy: " + ((double)correct)/nbExampleSeen);
        }


    }
}
