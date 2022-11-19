package ds504.demorris.flink.methane.timestamp;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;

public class CountsAggregate extends ProcessFunction<Tuple2<Double[],Double[]>,Tuple2<Integer,Integer>> {

    protected double warning;
    protected double alarm;

    protected Integer[][] counts = new Integer[3][3];

    public CountsAggregate(double warning, double alarm){
        this.warning=warning;
        this.alarm=alarm;
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        for (int i=0; i < 3; i++){
            for (int j=0; j < 3; j++){
                counts[i][j] = 0;
            }
        }
    }

    @Override
    public void processElement(Tuple2<Double[],Double[]> instances, ProcessFunction<Tuple2<Double[],Double[]>,Tuple2<Integer,Integer>>.Context arg1, Collector<Tuple2<Integer,Integer>> collector) throws Exception{
        Double[] actual = instances.f0;
        Double[] predicted = instances.f1;

        for (int i=0; i < actual.length; i++){
            Integer actClass = 0;
            Integer predClass = 0;

            if(actual[i] >= alarm){
                actClass = 2;
            } else if(actual[i] >= warning){
                actClass = 1;
            }

            if(predicted[i] >= alarm){
                predClass = 2;
            } else if(predicted[i] >= warning){
                predClass = 1;
            }

            collector.collect(new Tuple2<>(actClass,predClass));

            counts[actClass][predClass]++;
        }
    }

    @Override
    public void close() throws Exception{
        System.out.println("----------------Predicted--------------------");
        System.out.println("Actual 0: " + counts[0][0] +" - " + counts[0][1] + " - " + counts[0][2]);
        System.out.println("Actual 1: " + counts[1][0] +" - " + counts[1][1] + " - " + counts[1][2]);
        System.out.println("Actual 2: " + counts[2][0] +" - " + counts[2][1] + " - " + counts[2][2]);
    }

    
}
