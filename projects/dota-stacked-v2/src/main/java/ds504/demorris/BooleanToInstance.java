package ds504.demorris;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;

import moa.core.FastVector;
import moa.core.InstanceExample;

public class BooleanToInstance implements MapFunction<Tuple3<InstanceExample,Boolean[],Double>, Tuple3<InstanceExample, Boolean[],Double>> {
    
    protected InstancesHeader streamHeader;

    protected int numAttributes;
    protected int numClasses;

    public BooleanToInstance(int numAttributes, int numClasses){
        this.numAttributes = numAttributes;
        this.numClasses = numClasses;
        generateHeader();
    }

    @Override
    public Tuple3<InstanceExample,Boolean[],Double> map(Tuple3<InstanceExample,Boolean[],Double> inputData ) throws Exception{

        double target = inputData.f0.getData().classValue();
        if (target != inputData.f2){
            System.out.println("Target Mismatch");
        }

        double[] features = new double[numAttributes+1];
        for (int i=0; i < numAttributes; i++){
            features[i] = inputData.f1[i]?1.0:0.0;
        }

        Instance inst = new DenseInstance(1.0,features);
        inst.setDataset(getHeader());
        inst.setClassValue(target);
        return new Tuple3(new InstanceExample(inst), new Boolean[1], inputData.f2);
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
