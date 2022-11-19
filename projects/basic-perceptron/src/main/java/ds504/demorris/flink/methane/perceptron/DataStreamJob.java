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

package ds504.demorris.flink.methane.perceptron;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.api.java.tuple.*;

//MOA packages
import moa.core.InstanceExample;
import moa.core.FastVector;
import moa.classifiers.Classifier;
import moa.classifiers.trees.HoeffdingTree;

//SAMOA packages
import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;

//JAVA packages
import java.time.LocalDateTime;
import java.util.Map;

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

				Double learningRate = 0.0002;

                // Sets up the execution environment, which is the main entry point
                // to building Flink applications.
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

                

                DataStream<Tuple2<Double[],Double[]>> train = env.readTextFile("file:///root/data/methane/methane_combined.csv")
					.setParallelism(1)
					.map(new convertToInstance(34,3))
					.setParallelism(1);

				SingleOutputStreamOperator<Tuple2<Double[],Double[]>> results = train.process(new PerceptronProcessFunction(34,3,learningRate));

				results.flatMap(new FlatMapFunction<Tuple2<Double[],Double[]>, Tuple3<Integer,Integer,String>>(){
					@Override
					public void flatMap(Tuple2<Double[],Double[]> inputStream, Collector<Tuple3<Integer,Integer,String>> collector){
						for (int i=0; i < inputStream.f0.length; i++){
							Double actualVal = inputStream.f0[i] * 100.0;
							Double predVal = inputStream.f1[i] * 100;

							int actualClass = 0;
							if (actualVal >= 1.5){
								actualClass = 2;
							} else if (actualVal >= 1.0){
								actualClass = 1;
							}

							int predClass = 0;
							if (predVal >= 1.5){
								predClass = 2;
							} else if (predVal >= 1.0){
								predClass = 1;
							}

							collector.collect(new Tuple3(actualClass,predClass, Integer.toString(i)+"-"+Integer.toString(actualClass)));
						}
					}
				}).writeAsCsv("file:///root/projects/basic-perceptron/output"+java.time.LocalDateTime.now().toString()).setParallelism(1);
				
                // Execute program, beginning computation.
                env.execute("Flink Java API Skeleton");
        }

		public static class convertToInstance implements MapFunction<String, Tuple2<Double[],Double[]>> {

			protected int numFeatures;
			protected int numTargets;
			protected Double[] norms = {
				2014.0,
				6.0,
				31.0,
				23.0,
				59.0,
				59.0,
				5.0,
				2.4,
				5.3,
				27.9,
				71.0,
				1131.7,
				31.2,
				86.0,
				1130.9,
				30.0,
				30.0,
				30.0,
				30.0,
				40.0,
				30.0,
				30.0,
				67.7,
				258.0,
				435.4,
				40.5,
				6.39,
				988.0,
				1009.0,
				216.0,
				198.0,
				121.0,
				3.0,
				100.0,
			};

			public convertToInstance(int numFeatures, int numTargets){
				this.numFeatures=numFeatures;
				this.numTargets=numTargets;
			}

			@Override
			public Tuple2<Double[],Double[]> map(String inputText) throws Exception{
				String[] splitText = inputText.split(",");

				Double[] features = new Double[numFeatures];
				for (int i=0; i<numFeatures; i++){
					features[i] = Double.parseDouble(splitText[i])/norms[i];
				}
				Double[] targets = new Double[numTargets];
				for (int i=0; i<numTargets; i++){
					targets[i] = Double.parseDouble(splitText[numFeatures+i])/100.0;
				}

				return new Tuple2(features,targets);
			}

		}

}

