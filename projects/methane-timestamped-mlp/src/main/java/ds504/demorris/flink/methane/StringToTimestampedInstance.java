package ds504.demorris.flink.methane;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;


public class StringToTimestampedInstance implements MapFunction<String, Tuple3<Double[], Double[], Long>> {

    private int numAttributes;
    private int[] targetIndexes;
    private int numTargets;

    private double[] norms = {
        2014.0,
        12.0,
        31.0,
        24.0,
        60.0,
        60.0,
        5.0,
        5.0,
        6.0,
        28.0,
        100.0,
        1200.0,
        32.0,
        100.0,
        1200.0,
        30.0,
        30.0,
        30.0,
        30.0,
        40,
        30.0,
        30.0,
        70.0,
        260.0,
        440.0,
        41.0,
        10.0,
        1000.0,
        1100.0,
        220.0,
        200.0,
        150.0,
        2,
        100.0
    };

    public StringToTimestampedInstance(int numAttributes, int... targetIndexes){
        this.numAttributes = numAttributes;
        this.numTargets = targetIndexes.length;
        this.targetIndexes = targetIndexes;
    }
    
    @Override
    public Tuple3<Double[], Double[], Long> map(String inputText) throws Exception{
        //Split the text
        String[] splitText = inputText.split(",");

        //Extract the targets
        Double[] targets = new Double[numTargets];
        for (int i=0; i < numTargets; i++){
            targets[i] = Double.parseDouble(splitText[targetIndexes[i]].replace("'",""));
        }

        //Extract the features
        Double[] features = new Double[numAttributes];
        int offset = 0;
        for(int i=0; (i + offset) < numAttributes; i++){
            if (i==32){
                features[i] = 0.0;
                features[i+1] = 0.0;
                features[i+2] = 0.0;
                offset = 2;
                int oneHot = (int)Double.parseDouble(splitText[i].replace(",",""));
                features[i+oneHot] = 1.0;
            } else {
                features[i+offset] = Double.parseDouble(splitText[i].replace(",",""))/norms[i];
            }
        }

        //Create the timestamp
        Long timestamp = ZonedDateTime.of(LocalDateTime.of(
            (int)Double.parseDouble(splitText[0]),
            (int)Double.parseDouble(splitText[1]),
            (int)Double.parseDouble(splitText[2]),
            (int)Double.parseDouble(splitText[3]),
            (int)Double.parseDouble(splitText[4]),
            (int)Double.parseDouble(splitText[5])), ZoneId.systemDefault()).toInstant().toEpochMilli();
        
        
        return new Tuple3<>(features, targets, timestamp);


    }

}
