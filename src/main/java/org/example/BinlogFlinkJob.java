package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class BinlogFlinkJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // Kafka broker address
        properties.setProperty("group.id", "flink-consumer-group");  // Consumer group ID

        // Create a Kafka consumer to consume from the `dbserver` topic (binlog messages)
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "dbserver.testdb.order", // Kafka topic
                new SimpleStringSchema(), // Deserialization schema
                properties // Kafka properties
        );

        // Get the stream from Kafka
        DataStream<String> stream = env.addSource(kafkaConsumer);

        // Perform some computation (for example, just print the messages)
        stream.print();

        // Execute the Flink job
        env.execute("Flink Binlog Processing Job");
    }
}
