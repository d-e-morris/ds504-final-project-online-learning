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

package ds504.demorris.flink.aws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;

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

                int numAttributes = 135;
                double learningRate = 0.01;
                String filePath = "file:///root/data/aws/one_hot_aws_month.csv";

                final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

                DataStream<Tuple2<Double[],Double[]>> data = env
                        .readTextFile(filePath).setParallelism(1)
                        .map(new MapFunction<String,Tuple2<Double[],Double[]>>(){

                                @Override
                                public Tuple2<Double[],Double[]> map(String inputText) throws Exception{
                                        int numAttributes = 135;
                                        int targetIndex = 2;

                                        String[] splitText = inputText.split(",");
                                        
                                        Double[] target = {0.0};
                                        Double[] features = new Double[numAttributes];
                                        for(int i = 0; i < splitText.length-1; i++){
                                                double value = Double.parseDouble(splitText[i]);
                                                if (i == targetIndex){
                                                        target[0] = value;
                                                } else if(i < targetIndex){
                                                        features[i] = value/60;
                                                } else {
                                                        features[i-1] = value/60;
                                                }
                                        }

                                        return new Tuple2<>(features,target);
                                }
                        }).setParallelism(1);
                        
                data.process(new PerceptronProcessFunction(numAttributes,1,learningRate))
                .process(new MeanAbsoluteErrorProcessFunction(100_000)).setParallelism(1);
 
                // Execute program, beginning computation.
                env.execute("Flink Java API Skeleton");
        }
}

