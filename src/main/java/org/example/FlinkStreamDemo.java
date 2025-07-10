package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 7001);

        DataStream<Tuple2<String, Integer>> wordCounts = text
                .map(new Tokenizer())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        wordCounts.print();

        env.execute("Flink Stream Demo");
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String value) {
            System.out.println("Tokenizer.map");
            String[] words = value.toLowerCase().split(" ");
            return new Tuple2<>(words[0], 1);
        }
    }
}
