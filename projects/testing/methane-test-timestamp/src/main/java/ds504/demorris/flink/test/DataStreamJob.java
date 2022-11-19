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

package ds504.demorris.flink.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

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

                DataStream<Double[]> data = env.readTextFile("file:///root/data/methane/methane_data.csv")
                        .map(new MapFunction<String,Double[]>() {
                                @Override
                                public Double[] map(String inputRecord) throws Exception{
                                        String[] splitString = inputRecord.split(",");
                                        Double[] features = new Double[splitString.length];
                                        for (int i=0; i < features.length; i++){
                                                features[i] = Double.parseDouble(splitString[i].replace("'",""));
                                        }
                                        return features;
                                }     
                        })
                        .filter(new FilterFunction<Double[]>() {
                                @Override
                                public boolean filter(Double[] input) throws Exception{
                                        return (input[1] < 4) && (input[2] < 5);
                                }
                        });

                DataStream<Double[]> timeStampedData = data.assignTimestampsAndWatermarks(WatermarkStrategy.<Double[]>forMonotonousTimestamps()
                .withTimestampAssigner((input, timestamp) -> ZonedDateTime.of(LocalDateTime.of(
                        input[0].intValue(),
                        input[1].intValue(),
                        input[2].intValue(),
                        input[3].intValue(),
                        input[4].intValue(),
                        input[5].intValue()),ZoneId.systemDefault()).toInstant().toEpochMilli()));
                
                timeStampedData.windowAll(SlidingEventTimeWindows.of(Time.seconds(30),Time.seconds(1)))
                .process(new ProcessAllWindowFunction<Double[],Double[],TimeWindow>() {
                        @Override
                        public void process(ProcessAllWindowFunction<Double[],Double[],TimeWindow>.Context context, Iterable<Double[]> inputs, Collector<Double[]> output) throws Exception{
                                System.out.println("Winow: "+ context.window());
                                for (Double[] input: inputs){
                                        System.out.println(LocalDateTime.of(
                                                input[0].intValue(),
                                                input[1].intValue(),
                                                input[2].intValue(),
                                                input[3].intValue(),
                                                input[4].intValue(),
                                                input[5].intValue()));
                                }
                        }
                        
                });
                
                // Execute program, beginning computation.
                env.execute("Flink Java API Skeleton");
        }

        private static class AverageAggregate implements AggregateFunction<Double[],Tuple2<Double[],Long>,Double[]>{
                @Override
                public Tuple2<Double[],Long> createAccumulator() {
                        Double[] sum = new Double[34];
                        for (int i=0; i < sum.length; i++){
                                sum[i] = 0.0;
                        }
                  return new Tuple2<>(sum, 0L);
                }
              
                @Override
                public Tuple2<Double[], Long> add(Double[] value, Tuple2<Double[],Long> accumulator) {
                        Double[] sum = new Double[value.length];
                        for (int i=0; i<value.length; i++){
                                sum[i] = accumulator.f0[i] + value[i];
                        }
                  return new Tuple2<>(sum, accumulator.f1 + 1L);
                }
              
                @Override
                public Double[] getResult(Tuple2<Double[], Long> accumulator) {
                        Double[] output = new Double[accumulator.f0.length];
                        for (int i = 0; i < output.length; i++){
                                output[i] = accumulator.f0[i]/accumulator.f1;
                        }
                  return output;
                }
              
                @Override
                public Tuple2<Double[], Long> merge(Tuple2<Double[], Long> a, Tuple2<Double[], Long> b) {
                        Double[] output = new Double[a.f0.length];
                        for (int i=0; i < output.length; i++){
                                output[i] = a.f0[i] + b.f0[i];
                        }
                  return new Tuple2<>(output, a.f1 + b.f1);
                }
              
        }
}

