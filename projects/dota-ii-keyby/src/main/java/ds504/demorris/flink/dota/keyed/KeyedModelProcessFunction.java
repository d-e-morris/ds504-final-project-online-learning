package ds504.demorris.flink.dota.keyed;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;

import moa.classifiers.Classifier;
import moa.core.InstanceExample;

import com.yahoo.labs.samoa.instances.Prediction;

public class KeyedModelProcessFunction extends KeyedProcessFunction<Double, InstanceExample, Prediction> {
    
    private static final long serialVersionUID = 1L;

    final OutputTag<Tuple2<Double,StringBuilder>> modelInfoTag = new OutputTag<Tuple2<Double,StringBuilder>>("model-info"){};

    private transient ValueState<Classifier> keyedClassifier;
    private Class<? extends Classifier> model;

    //private Classifier classifier;
    private int updateSize;
    private int warmup;
    private transient ValueState<Tuple2<Long, Long>> summaryStats;

    public KeyedModelProcessFunction(Class<? extends Classifier> model, int updateSize, int warmup){
        this.model=model;
        this.updateSize=updateSize;
        this.warmup=warmup;
    }

    @Override
    public void open(Configuration parameters) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        ValueStateDescriptor<Classifier> descriptor = new ValueStateDescriptor<>("classifier", Classifier.class);
        keyedClassifier = getRuntimeContext().getState(descriptor);

        ValueStateDescriptor<Tuple2<Long, Long>> summaryStatDescriptor = new ValueStateDescriptor<>(
            "summaryStats",
            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));
        summaryStats = getRuntimeContext().getState(summaryStatDescriptor);
    }

    @Override
    public void processElement(InstanceExample inst, KeyedProcessFunction<Double, InstanceExample, Prediction>.Context arg1, Collector<Prediction> collector) throws Exception{
        Classifier classifier = keyedClassifier.value();
        if (classifier == null){
            classifier = model.newInstance();
            classifier.prepareForUse();
        }

        Tuple2<Long, Long> stats = summaryStats.value();
        if (stats == null) {
            stats = new Tuple2<>(0L, 0L);
        }

        stats.f0++;

        if (stats.f0 >= warmup){
            if (classifier.correctlyClassifies(inst.getData())){
                stats.f1++;
            }

            if (stats.f0 % updateSize == 0){
                StringBuilder info = new StringBuilder();
                classifier.getDescription(info,1);
                arg1.output(modelInfoTag, new Tuple2<Double, StringBuilder>(arg1.getCurrentKey(),info));
            }

            if (stats.f0 % 10_000 == 0){
                System.out.println("KEY_" + arg1.getCurrentKey()+" - Examples Seen: " + stats.f0 + " Accuracy: " + ((double)stats.f1)/(stats.f0-warmup+1));
            }
            collector.collect(classifier.getPredictionForInstance(inst.getData()));
        } 
        classifier.trainOnInstance(inst);

        keyedClassifier.update(classifier);
        summaryStats.update(stats);
    }
}
