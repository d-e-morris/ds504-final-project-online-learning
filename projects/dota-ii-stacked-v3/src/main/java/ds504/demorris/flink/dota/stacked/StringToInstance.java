package ds504.demorris.flink.dota.stacked;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;

import moa.core.FastVector;
import moa.core.InstanceExample;

public class StringToInstance implements MapFunction<String, Tuple2<InstanceExample,Long>> {
    
    protected InstancesHeader streamHeader;

    protected int numAttributes;
    protected int numClasses;
    protected Long examplesSeen;

    public StringToInstance(int numAttributes, int numClasses){
        this.numAttributes = numAttributes;
        this.numClasses = numClasses;
        this.examplesSeen=0L;
        generateHeader();
    }

    @Override
    public Tuple2<InstanceExample, Long> map(String inputString) throws Exception{
        String[] splitText = inputString.split(",");

        double target = 1;
        if (Integer.parseInt(splitText[0]) == 1){
            target = 0;
        }

        double[] features = new double[numAttributes+1];
        for (int i=1; i < numAttributes+1; i++){
            features[i-1] = Double.parseDouble(splitText[i]);
        }

        Instance inst = new DenseInstance(1.0,features);
        inst.setDataset(getHeader());
        inst.setClassValue(target);
        examplesSeen += 1000;
        return new Tuple2<>(new InstanceExample(inst),examplesSeen);
    }

    protected void generateHeader(){
        FastVector attributes = new FastVector<>();
        for(int i=0; i < this.numAttributes; i++){
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
}
