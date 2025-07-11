package org.example;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class BinlogFlinkJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                10, // 重启次数
                Time.of(3, TimeUnit.SECONDS) // 间隔
        ));

        // Configure Kafka consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:29092"); // Kafka broker address
        properties.setProperty("group.id", "flink-consumer-group");  // Consumer group ID
        properties.setProperty("client.id", "flink-consumer-group-" + UUID.randomUUID()); // Set a unique client ID for the consumer

        // Create a Kafka consumer to consume from the `dbserver` topic (binlog messages)
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "dbserver.testdb.trade_core_order", // Kafka topic
                new SafeStringSchema(), // Deserialization schema
                properties // Kafka properties
        );

        // kafkaConsumer.setStartFromEarliest(); // 每次都重头消费
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        // Get the stream from Kafka
        DataStream<String> stream = env.addSource(kafkaConsumer);

        // 消息体前缀加上"Received Binlog Message: "
        stream.print("Received Binlog Message: ");

        // 仅打印消息体
        // stream.print();

        // Execute the Flink job
        env.execute("Flink Binlog Processing Job");
    }
}
