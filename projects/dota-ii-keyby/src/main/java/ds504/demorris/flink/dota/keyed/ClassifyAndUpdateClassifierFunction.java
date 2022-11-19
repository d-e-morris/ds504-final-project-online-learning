package ds504.demorris.flink.dota.keyed;

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;


import moa.classifiers.Classifier;
import moa.classifiers.functions.NoChange;
import moa.core.InstanceExample;

public class ClassifyAndUpdateClassifierFunction implements CoFlatMapFunction<InstanceExample, Classifier, Boolean> {
    
    private static final long serialVersionUID = 1L;
    private Classifier classifier = new NoChange();

    @Override
    public void flatMap1(InstanceExample value, Collector<Boolean> out) throws Exception {
        out.collect(classifier.correctlyClassifies(value.getData()));
    }

    @Override
    public void flatMap2(Classifier classifier, Collector<Boolean> out) throws Exception {
        this.classifier = classifier;
    }
}
