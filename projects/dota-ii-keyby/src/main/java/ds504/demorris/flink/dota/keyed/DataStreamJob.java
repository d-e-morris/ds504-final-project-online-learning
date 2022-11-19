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

package ds504.demorris.flink.dota.keyed;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;


//MOA packages
import moa.classifiers.trees.HoeffdingTree;
import moa.core.InstanceExample;
import moa.core.FastVector;

//SAMOA packages
import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Prediction;

/**
 * This Job will split the DOTA II stream based on a selected field and will build
 * a classifier for each of the split streams. In addition, the classifiers will use
 * an interleaved test-then-train performane metric with a warm-up period to allow
 * the model to develop some sophistication.
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {

                int keyIndex = 2; // 0 - Cluster ID, 1 - Game Mode, 2 - Game Type
                int warmup = 10_000; // number of instances to wait before testing

		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final OutputTag<Tuple2<Double,StringBuilder>> modelInfoTag = new OutputTag<Tuple2<Double,StringBuilder>>("model-info"){};

		// The first step is to read in the file as a DataStream Object
		DataStream<InstanceExample> train = env
			.readTextFile("file:///root/projects/testing/flink-java-project/data/dota2Train.csv")
			.map(new convertToInstance());

		SingleOutputStreamOperator<Prediction> predictions = train.keyBy(n -> n.getData().value(keyIndex))
				.process(new KeyedModelProcessFunction(HoeffdingTree.class, 30000, warmup));
		
		DataStream<Tuple2<Double,StringBuilder>> modelInfoStream = predictions.getSideOutput(modelInfoTag);
		modelInfoStream.map(new MapFunction<Tuple2<Double,StringBuilder>,String>(){
				@Override
				public String map(Tuple2<Double,StringBuilder> modelInfo) throws Exception {
						return "KEY_"+modelInfo.f0+" - " +modelInfo.f1.toString();
				}
		});

		// Execute program, beginning computation.
		env.execute("Keyed Hoeffding");
	}

	public static class convertToInstance implements MapFunction<String, InstanceExample> {

		protected InstancesHeader streamHeader;

		protected int numAttributes = 116;
		protected int numClasses = 2;

		public convertToInstance(){
			generateHeader();
		}

		@Override
		public InstanceExample map(String inputText) throws Exception{
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
			return new InstanceExample(inst);
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
