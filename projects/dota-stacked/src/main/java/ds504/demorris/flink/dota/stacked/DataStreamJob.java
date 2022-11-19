/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ds504.demorris.flink.dota.stacked;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.*;

import moa.core.InstanceExample;
import moa.core.FastVector;
import moa.classifiers.Classifier;
import moa.classifiers.functions.NoChange;

import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

        public static void main(String[] args) throws Exception {
                // Sets up the execution environment, which is the main entry point
                // to building Flink applications.
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

                final OutputTag<InstanceExample> stackedHoeffTag_1 = new OutputTag<InstanceExample>("Hoeffding-1"){};
				final OutputTag<InstanceExample> stackedHoeffTag_2 = new OutputTag<InstanceExample>("Hoeffding-2"){};

                SingleOutputStreamOperator<InstanceExample> data = env.readTextFile("files:///root/data/dota_ii/dota2Train.csv")
                        .setParallelism(1)
                        .process(new convertToInstance())
                        .setParallelism(1);

                DataStream<InstanceExample> stackedHoeffding_1 = data.getSideOutput(stackedHoeffTag_1);
				DataStream<InstanceExample> stackedHoeffding_2 = data.getSideOutput(stackedHoeffTag_2);

				SingleOutputStreamOperator<Tuple3<Integer,Classifier,Integer>> stackedClassifier_1 = stackedHoeffding_1.process(new ModelProcessFunction("HoeffdingTree",10_000, 0, "Hoeffding_1"));
				SingleOutputStreamOperator<Tuple3<Integer,Classifier,Integer>> stackedClassifier_2 = stackedHoeffding_2.process(new ModelProcessFunction("Hoeffding",10_000, 0, "Hoeffinding_2"));

				data.connect(stackedClassifier_1).flatMap(new CoFlatMapFunction<InstanceExample,Tuple3<Integer,Classifier,Integer>,Tuple2<InstanceExample,Boolean[]>>() {
					private Classifier classifier = new NoChange();


					@Override
					public void flatMap1(InstanceExample value, Collector<Tuple2<InstanceExample,Boolean[]>> out) throws Exception{
						Boolean[] outPred = new Boolean[1];
						outPred[0] = classifier.correctlyClassifies(value.getData());
						out.collect(new Tuple2<>(value,outPred));
					}

					@Override 
					public void flatMap2(Tuple3<Integer,Classifier,Integer> inputTuple, Collector<Tuple2<InstanceExample,Boolean[]>> out) throws Exception{
						this.classifier = inputTuple.f1;

					}
					
				}).connect(stackedClassifier_2).flatMap(new CoFlatMapFunction<Tuple2<InstanceExample, Boolean[]>, Tuple3<Integer,Classifier,Integer>,Tuple2<InstanceExample,Boolean[]>>(){
					private Classifier classifier = new NoChange();

					@Override
					public void flatMap1(Tuple2<InstanceExample,Boolean[]> value, Collector<Tuple2<InstanceExample, Boolean[]>> out) throws Exception{
						Boolean[] outPred = new Boolean[2];
						outPred[0] = value.f1[0];
						outPred[1] = classifier.correctlyClassifies(value.f0.getData());
						out.collect(new Tuple2<>(value.f0,outPred));
					}

					@Override
					public void flatMap2(Tuple3<Integer,Classifier,Integer> input, Collector<Tuple2<InstanceExample, Boolean[]>> out) throws Exception{
						this.classifier=input.f1;
					}
				}).process(new ModelProcessFunction("Hoeffding",10_000,0,true));
                
                // Execute program, beginning computation.
                env.execute("Stacked Hoeffding Tree");
        }
        
        public static class convertToInstance extends ProcessFunction<String, InstanceExample> {

			protected InstancesHeader streamHeader;

			protected int numAttributes = 116;
			protected int numClasses = 2;

			protected OutputTag<InstanceExample> stackedHoeffTag_1 = new OutputTag<InstanceExample>("Hoeffding-1");
			protected OutputTag<InstanceExample> stackedHoeffTag_2 = new OutputTag<InstanceExample>("Hoeffding-2");


			public convertToInstance(){
				generateHeader();
			}

			@Override
			public void processElement(String inputText, 
							ProcessFunction<String, InstanceExample>.Context ctx, 
							Collector<InstanceExample> collector) throws Exception
					{

				String[] splitText = inputText.split(",");

				double target = 1;
				if (Integer.parseInt(splitText[0]) == 1) {
					target = 0;
				}

				double[] features = new double[splitText.length];
				for (int i = 1; i < splitText.length; i++){
					features[i-1] = Double.parseDouble(splitText[i]);
				}
				
				Instance inst = new DenseInstance(1.0,features);
				inst.setDataset(getHeader());
				inst.setClassValue(target);
				collector.collect(new InstanceExample(inst));

				ctx.output(stackedHoeffTag_1, new InstanceExample(inst));
				ctx.output(stackedHoeffTag_2, new InstanceExample(inst));
			}

			protected void generateHeader(){
				FastVector attributes = new FastVector();
				for (int i=0; i < this.numAttributes; i++){
					attributes.addElement(new Attribute("att"+(i+1)));
				}

				FastVector classLabels = new FastVector();
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

}

