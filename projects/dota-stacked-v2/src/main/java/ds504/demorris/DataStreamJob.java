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

package ds504.demorris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import moa.core.InstanceExample;
import moa.classifiers.Classifier;

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

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);

                //Load the data file and convert it to an instance example class for training
                DataStream<Tuple3<InstanceExample,Boolean[],Double>> data = env.readTextFile("file:///root/data/dota_ii/dota2Train.csv")
                        .setParallelism(1).map(new StringToInstance(116,2)).setParallelism(1);

                //Run the instance example through the various stacked classifiers
                SingleOutputStreamOperator<Classifier> stackedClassifier_1 = data.process(new ModelProcessFunction("bayes.NaiveBayes", 1_000, true, "NaiveBayes"));
                SingleOutputStreamOperator<Classifier> stackedClassifier_2 = data.process(new ModelProcessFunction("trees.HoeffdingTree", 1_000, true, "Standard Hoeffding"));
                SingleOutputStreamOperator<Classifier> stackedClassifier_3 = data.process(new ModelProcessFunction("trees.HoeffdingTree -b -r -l MC", 1_000, true, "Special Hoeffding"));

                //Connect the classifier back to the original data stream to use them in an ensemble
                SingleOutputStreamOperator<Tuple3<InstanceExample,Boolean[],Double>> stackedOutputs = data
                        .connect(stackedClassifier_1).flatMap(new ModelStacker(1))
                        .connect(stackedClassifier_2).flatMap(new ModelStacker(2))
                        .connect(stackedClassifier_3).flatMap(new ModelStacker(3));

                //Convert the output of the classifiers into its own instance example for the perceptron
                SingleOutputStreamOperator<Tuple3<InstanceExample,Boolean[],Double>> stackedInstance = stackedOutputs.map(new BooleanToInstance(3,2))
                        .setParallelism(1);

                // stackedInstance.map(new MapFunction<Tuple3<InstanceExample,Boolean[],Double>,String>() {
                //         @Override
                //         public String map(Tuple3<InstanceExample,Boolean[],Double> inputValue) throws Exception{
                //                 String outValue = "";
                //                 for (int i=0; i < 3; i++){
                //                         outValue += inputValue.f0.getData().value(i) + ",";
                //                 }
                //                 outValue += inputValue.f2;
                //                 return outValue;
                //         }
                // }).writeAsText("file:///root/projects/dota-stacked-v2/output/file.csv");

                //Predict
                SingleOutputStreamOperator<Classifier> outputModel = stackedInstance.process(new ModelProcessFunction("functions.Perceptron",1_000,true,"OutputLayer"));
                
                // Execute program, beginning computation.
                env.execute("Dota II Stacked Classifier");
        }
}

