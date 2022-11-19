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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;



import moa.core.InstanceExample;

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

                //Open file and convert to instance example with timestamp
                DataStream<Tuple2<InstanceExample,Long>> data = env
                        .readTextFile("file:///root/data/dota_ii/dota2Train.csv").setParallelism(1)
                        .map(new StringToInstance(116,2)).setParallelism(1);

                //Predict and Train on each Example
                SingleOutputStreamOperator<Tuple4<Double,Double,Integer,Long>> stackedClassifier_1 = data
                        .process(new ModelProcessFunction("bayes.NaiveBayes", 1_000, false, "NaiveBayes",0))
                        .setParallelism(1);
                SingleOutputStreamOperator<Tuple4<Double,Double,Integer,Long>> stackedClassifier_2 = data
                        .process(new ModelProcessFunction("trees.HoeffdingTree", 1_000, false, "Standard Hoeffding",1))
                        .setParallelism(1);
                SingleOutputStreamOperator<Tuple4<Double,Double,Integer,Long>> stackedClassifier_3 = data
                        .process(new ModelProcessFunction("trees.HoeffdingTree -b -r -l MC", 1_000, false, "Special Hoeffding",2))
                        .setParallelism(1);

                //Join each example into a single output
                SingleOutputStreamOperator<Tuple4<Double,Double,Integer,Long>> combinedStream = stackedClassifier_1.union(stackedClassifier_2,stackedClassifier_3)
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<Tuple4<Double,Double,Integer,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(30000))
                                .withTimestampAssigner((input,timestamp)->input.f3));

                //Predict and Train on each example
                SingleOutputStreamOperator<Tuple2<Double,Double>> stackedStream = combinedStream
                        .windowAll(SlidingEventTimeWindows.of(Time.seconds(1),Time.seconds(1)))
                        .process(new StackedModelProcessFunction("functions.Perceptron",3,true,"OutputLayer",1_000)).setParallelism(1);
                
                // Execute program, beginning computation.
                env.execute("Stacked Dota Model V3");
        }
}

