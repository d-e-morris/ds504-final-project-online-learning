package ds504.demorris.testing.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import moa.classifiers.Classifier;
import moa.core.InstanceExample;
import moa.options.ClassOption;
import moa.classifiers.trees.HoeffdingTree;
import moa.MOAObject;

public class customProcess extends ProcessFunction<Long, Long>{

    private HoeffdingTree myTree;
    
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        try{}
        HoeffdingTree myTree2 = (HoeffdingTree)ClassOption.cliStringToObject("HoeffdingTree -c 0.01", null, null);

    }

    public void processElement(Long in, ProcessFunction<Long, Long>.Context arg1, Collector<Long> out) throws Exception {

    }
}
