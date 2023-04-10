package com.sampleFlinkProject.util;
import java.util.Properties;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//TODO Fix Kafka version
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class SetEnv {

    public void setEnv(String[] args) throws Exception {
        if (args.length != 3) {
            System.err/*  w w w.j a  v  a 2 s.c  o  m*/
                    .println("USAGE: Main <topic> <checkpointing> <checkpointing time (ms)>");
            System.err.println("\t <checkpointing>: [0|1]");
            return;
        }

        //FLINK CONFIGURATION
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setParallelism(8);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        if (Integer.valueOf(args[1]) == 1) {
            env.enableCheckpointing(Integer.valueOf(args[2]));
            env.getCheckpointConfig().setCheckpointingMode(
                    CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            env.setStateBackend(new FsStateBackend(
                    "file:///home/fran/nfs/nfs/checkpoints/flink"));
        }

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.155:9092");
        //FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(
        //        args[0], new SimpleStringSchema(), properties);

        //KAFKA PRODUCER
        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers",
                "192.168.0.155:9092");
        producerConfig.setProperty("acks", "all");
        producerConfig.setProperty("linger.ms", "0");

        //MAIN PROGRAM
        //Read from Kafka
        //TODO 
        //DataStream<String> line = env.addSource(myConsumer); //.addSource(myConsumer);

        /*
         * This part is just to consume CPU as all the changes results in nothing in the end
         */
        //DataStream<String> lineSum = line.map(new WhileSumAllNumbers());

        //DataStream<String> line2 = lineSum.map(new RemoveSumAllNumbers());

        //Add 1 to each line
        //DataStream<Tuple2<String, Integer>> line_Num = line2
        //        .map(new NumberAdder());

        //Filter Odd numbers
        //DataStream<Tuple2<String, Integer>> line_Num_Odd = line_Num
        //        .filter(new FilterOdd());
        //DataStream<Tuple3<String, String, Integer>> line_Num_Odd_2 = line_Num_Odd
        //        .map(new OddAdder());

        //Filter Even numbers
        //DataStream<Tuple2<String, Integer>> line_Num_Even = line_Num
        //        .filter(new FilterEven());
        //DataStream<Tuple3<String, String, Integer>> line_Num_Even_2 = line_Num_Even
        //        .map(new EvenAdder());

        //Join Even and Odd
        //DataStream<Tuple3<String, String, Integer>> line_Num_U = line_Num_Odd_2
        //        .union(line_Num_Even_2);

        //Tumbling windows every 2 seconds
        //WindowedStream<Tuple3<String, String, Integer>, Tuple, TimeWindow> windowedLine_Num_U_K = line_Num_U
        //        .keyBy(1).window(
        //                TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //Reduce to one line with the sum
        //DataStream<Tuple3<String, String, Integer>> wL_Num_U_Reduced = windowedLine_Num_U_K
        //        .reduce(new Reducer());

        //Calculate the average of the elements summed
        //DataStream<String> wL_Average = wL_Num_U_Reduced
        //        .map(new AverageCalculator());

        //Add timestamp and calculate the difference with the average
        //DataStream<String> averageTS = wL_Average.map(new TimestampAdder());

        //Send the result to Kafka
		/*
		 * FlinkKafkaProducer010Configuration<String> myProducerConfig =
		 * (FlinkKafkaProducer010Configuration<String>) FlinkKafkaProducer010
		 * .writeToKafkaWithTimestamps(averageTS, "testRes", new SimpleStringSchema(),
		 * producerConfig);
		 
        myProducerConfig.setWriteTimestampToKafka(true);
*/
        env.execute("Main method");

    }

    public static class WhileSumAllNumbers implements
            MapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        public String map(String line) {
            int sumNumbers = 0;
            for (int i = 1; i <= line.length(); i++) {
                if (line.substring(i - 1, i).matches("[-+]?\\d*\\.?\\d+")) {
                    sumNumbers += Integer.valueOf(line.substring(i - 1, i));
                }
            }
            String newLine = line.concat(" " + String.valueOf(sumNumbers));
            return newLine;
        }
    };

    public static class RemoveSumAllNumbers implements
            MapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        public String map(String line) {
            String newLine = line.split(" ")[0] + " " + line.split(" ")[1];
            return newLine;
        }
    };

    public static class OddAdder
            implements
            MapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
        private static final long serialVersionUID = 1L;

        public Tuple3<String, String, Integer> map(
                Tuple2<String, Integer> line) throws Exception {
            Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(
                    line.f0, "odd0000", line.f1);
            return newLine;
        }
    };

    public static class EvenAdder
            implements
            MapFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>> {
        private static final long serialVersionUID = 1L;

        public Tuple3<String, String, Integer> map(
                Tuple2<String, Integer> line) throws Exception {
            Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(
                    line.f0, "even1111", line.f1);
            return newLine;
        }
    };

    public static class FilterOdd implements
            FilterFunction<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public boolean filter(Tuple2<String, Integer> line)
                throws Exception {
            Boolean isOdd = (Long.valueOf(line.f0.split(" ")[0]) % 2) != 0;
            return isOdd;
        }
    };

    public static class FilterEven implements
            FilterFunction<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public boolean filter(Tuple2<String, Integer> line)
                throws Exception {
            Boolean isEven = (Long.valueOf(line.f0.split(" ")[0]) % 2) == 0;
            return isEven;
        }
    };

    public static class NumberAdder implements
            MapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        public Tuple2<String, Integer> map(String line) {
            Tuple2<String, Integer> newLine = new Tuple2<String, Integer>(
                    line, 1);
            return newLine;
        }
    };

    public static class Reducer implements
            ReduceFunction<Tuple3<String, String, Integer>> {
        private static final long serialVersionUID = 1L;

        public Tuple3<String, String, Integer> reduce(
                Tuple3<String, String, Integer> line1,
                Tuple3<String, String, Integer> line2) throws Exception {
            Long sum = Long.valueOf(line1.f0.split(" ")[0])
                    + Long.valueOf(line2.f0.split(" ")[0]);
            Long sumTS = Long.valueOf(line1.f0.split(" ")[1])
                    + Long.valueOf(line2.f0.split(" ")[1]);
            Tuple3<String, String, Integer> newLine = new Tuple3<String, String, Integer>(
                    String.valueOf(sum) + " " + String.valueOf(sumTS),
                    line1.f1, line1.f2 + line2.f2);
            return newLine;
        }
    };

    public static class AverageCalculator implements
            MapFunction<Tuple3<String, String, Integer>, String> {
        private static final long serialVersionUID = 1L;

        public String map(Tuple3<String, String, Integer> line)
                throws Exception {
            Long average = Long.valueOf(line.f0.split(" ")[1]) / line.f2;
            String result = String.valueOf(line.f2) + " "
                    + String.valueOf(average);
            return result;
        }
    };

    public static final class TimestampAdder implements
            MapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        public String map(String line) throws Exception {
            Long currentTime = System.currentTimeMillis();
            String totalTime = String.valueOf(currentTime
                    - Long.valueOf(line.split(" ")[1]));
            String newLine = line.concat(" " + String.valueOf(currentTime)
                    + " " + totalTime);

            return newLine;
        }
    };
}