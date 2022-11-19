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

package ds504.demorris.flink.dota;

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

				int numAttributes = 116;
				int numClasses = 2;
				int targetIndex = 0;
				String filePath = "file:///root/data/dota_ii/dota2Train.csv";
				String cliString = "trees.HoeffdingTree";

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
                env.setParallelism(1);

                DataStream<InstanceExample> train = env.readTextFile(filePath)
                 .map(new convertToInstance(numAttributes,numClasses,targetIndex));

                train.process(new ModelProcessFunction(cliString,1_000,true,"HoeffdingTree"));
                
                // Execute program, beginning computation.
                env.execute("Flink Java API Skeleton");
        }

        public static class convertToInstance implements MapFunction<String, InstanceExample> {

		protected InstancesHeader streamHeader;

		protected int numAttributes;
		protected int numClasses;
		protected int targetIndex;

		public convertToInstance(int numAttributes, int numClasses, int targetIndex){
			this.numAttributes = numAttributes;
			this.numClasses = numClasses;
			this.targetIndex = targetIndex;
			generateHeader();
		}

		@Override
		public InstanceExample map(String inputText) throws Exception{
			String[] splitText = inputText.split(",");

			double target = 0;
			double[] features = new double[splitText.length];
			for (int i = 0; i < splitText.length; i++){
				double value = Double.parseDouble(splitText[i]);
				if (i == targetIndex){
					if (value == 1){
						target = 0;
					} else{
						target = 1;
					}
				} else if (i < targetIndex){
					features[i] = value;
				} else {
					features[i-1] = value;
				}
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
			for (int i=0; i < numClasses; i++){
				classLabels.addElement("Class-"+(i+1));
			}
			attributes.addElement(new Attribute("class",classLabels));

			this.streamHeader = new InstancesHeader(new Instances("DOTA",attributes,0));
			this.streamHeader.setClassIndex(this.streamHeader.numAttributes()-1);
		}

		public InstancesHeader getHeader(){
			return this.streamHeader;
		}
	}
}

