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

package org.apache.flink.quickstart;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


//These Flink packages are to import MOA components for online learning
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

//MOA packages
import moa.classifiers.Classifier;
import moa.classifiers.functions.NoChange;
import moa.classifiers.trees.HoeffdingTree;
import moa.core.InstanceExample;
import moa.core.FastVector;
import moa.streams.InstanceStream;
import moa.options.ClassOption;

//SAMOA packages
import com.yahoo.labs.samoa.instances.InstancesHeader;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.Instances;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;

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

		System.out.println(System.getProperty("java.version"));
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// The first step is to read in the file as a DataStream Object
		DataStream<InstanceExample> train = env
			.readTextFile("file:///root/projects/testing/flink-java-project/data/dota2Train.csv")
			.map(new convertToInstance());

		DataStream<InstanceExample> test = env
			.readTextFile("file:///root/projects/testing/flink-java-project/data/dota2Train.csv")
			.map(new convertToInstance());

		SingleOutputStreamOperator<Classifier> classifier = train.process(new ModelProcessFunction(HoeffdingTree.class, 1000));

		test.connect(classifier)
			.flatMap(new ClassifyAndUpdateClassifierFunction())
			.countWindowAll(1000).aggregate(new PerformanceFunction())
			.print();
		
		// Execute program, beginning computation.
		env.execute("Training");
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
