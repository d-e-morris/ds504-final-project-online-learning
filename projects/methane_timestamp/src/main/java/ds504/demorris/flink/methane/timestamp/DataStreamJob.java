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

package ds504.demorris.flink.methane.timestamp;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import moa.core.InstanceExample;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import org.neuroph.core.NeuralNetwork;
import org.neuroph.nnet.MultiLayerPerceptron;
import org.neuroph.core.data.DataSet;
import org.neuroph.core.data.DataSetRow;
import org.neuroph.util.TransferFunctionType;
import org.neuroph.nnet.learning.BackPropagation;

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

                BackPropagation learningRule = new BackPropagation();
                learningRule.setLearningRate(0.01);
                learningRule.setMaxIterations(1);

                MultiLayerPerceptron model = new MultiLayerPerceptron(TransferFunctionType.RECTIFIED,28,16,16,8,8,4,3);

                DataStream<DataSet> train = env.readTextFile("file:///root/data/methane/methane_combined.csv")
                        .setParallelism(1)
                        .map(new MapFunction<String,DataSet>() {
                                @Override
                                public DataSet map(String inputString) throws Exception{
                                        String[] splitText = inputString.split(",");

                                        double[] targets = new double[3];
                                        for (int i=0; i < 3; i++){
                                                targets[i] = Double.parseDouble(splitText[splitText.length-3+i])/100;
                                        }
                                        double[] features = new double[splitText.length-9];
                                        for (int i=0; i < splitText.length-9; i++){
                                                features[i] = Double.parseDouble(splitText[i+6])/1000;
                                        }

                                        DataSet trainingSet = new DataSet(splitText.length-3-6,3);
                                        trainingSet.add(new DataSetRow(features,targets));
                                        return trainingSet;
                                }                                
                        }).setParallelism(1);

                train.process(new MLPProcessFunction(model, learningRule)).setParallelism(1)
                .process(new CountsAggregate(1.0/100,1.5/100)).setParallelism(1)
                .writeAsCsv("file:///root/projects/methane_timestamp/output/output-"+
                java.time.LocalDateTime.now().format(DateTimeFormatter.ofPattern("YYYY-MM-DD_HH-mm-ss")));

                
                
                // Execute program, beginning computation.
                env.execute("Flink Java API Skeleton");
        }
}

