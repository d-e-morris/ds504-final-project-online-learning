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

package ds504.demorris.flink.methane;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.neuroph.core.data.DataSet;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.neuroph.nnet.MultiLayerPerceptron;
import org.neuroph.nnet.learning.BackPropagation;
import org.neuroph.util.TransferFunctionType;

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

                final int numAttributes = 36;
                final int[] targetIndexes = {34, 35, 36};
                final int numTargets = targetIndexes.length;
                final int windowSizeSeconds = 15;
                final int windowSlideSeconds = 1;

                final double learningRate = 0.001;
                final int maxIterations = 1;

                final MultiLayerPerceptron model = new MultiLayerPerceptron(
                        TransferFunctionType.RECTIFIED,numAttributes*windowSizeSeconds,
                        32, 16, 4, numTargets
                );

                final BackPropagation learningRule = new BackPropagation();
                learningRule.setLearningRate(learningRate);
                learningRule.setMaxIterations(maxIterations);


                // Sets up the execution environment, which is the main entry point
                // to building Flink applications.
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                
                DataStream<Tuple3<Double[], Double[], Long>> data = env
                        .readTextFile("file:///root/data/methane/methane_combined.csv").setParallelism(1)
                        .map(new StringToTimestampedInstance(numAttributes, targetIndexes)).setParallelism(1);

                DataStream<Tuple3<Double[], Double[], Long>> timestampedData = data
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<Tuple3<Double[], Double[], Long>>forMonotonousTimestamps()
                                .withTimestampAssigner((input,timestamp) -> input.f2));

                SingleOutputStreamOperator<Tuple1<DataSet>> windowedData =  timestampedData
                        .windowAll( SlidingEventTimeWindows.of( Time.seconds(windowSizeSeconds),Time.seconds(windowSlideSeconds)))
                        .process(new CreateWindowedInstance(windowSizeSeconds,numAttributes,numTargets));

                windowedData.process(new ModelProcessFunction(model, learningRule));
                //.process(new CountsAggregate(1.0, 1.5));

                // Execute program, beginning computation.
                env.execute("Methane MLP Multiple Samples");
        }
}

