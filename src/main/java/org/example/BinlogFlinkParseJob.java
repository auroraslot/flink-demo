package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Flink作业，消费Kafka中的binlog消息并解析，然后打印出操作类型和实体信息
 */
@Slf4j
public class BinlogFlinkParseJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 1. 设置 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. FlinkKafkaConsumer的配置属性
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:29092");
        props.setProperty("group.id", "flink-consumer-group");

        // 3. 创建 FlinkKafkaConsumer，监听 Kafka 主题 dbserver.testdb.trade_core_order
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "dbserver.testdb.trade_core_order",
                new SafeStringSchema(),
                props
        );
        consumer.setCommitOffsetsOnCheckpoints(true);

        // 4. 消费者作为数据源，添加到 Flink 执行环境
        DataStream<String> raw = env.addSource(consumer);

        // 5. 获取到来自Kafka的数据流，做map操作，该操作称之为“算子”
        raw.map(new DebeziumParser())
                .name("ParseDebeziumEvent");

        // 6. 除了可以根据数据流进行map操作，还可以进行其他算子操作，比如过滤、聚合等
        // raw.addSink() // 可以添加一个Sink操作，将处理后的数据发送到其他系统，比如数据库、文件系统
        // raw.process()

        // 6. 执行这个Job
        env.execute("Flink Binlog Parsing Job");
    }

    /**
     * MapFunction 把 JSON 字符串反序列化，然后提取 op、实体，并用日志框架打印。
     */
    public static class DebeziumParser implements MapFunction<String, Void> {
        @Override
        public Void map(String json) throws Exception {
            JsonNode root = MAPPER.readTree(json);
            JsonNode payload = root.get("payload");
            if (payload == null || payload.isNull()) {
                log.warn("Empty payload, skipping");
                return null;
            }

            String op = payload.get("op").asText(); // c,u,d
            String dml;
            JsonNode entityNode;
            switch (op) {
                case "c":
                    dml = "INSERT";
                    entityNode = payload.get("after");
                    break;
                case "u":
                    dml = "UPDATE";
                    entityNode = payload.get("after");
                    break;
                case "d":
                    dml = "DELETE";
                    entityNode = payload.get("before");
                    break;
                default:
                    dml = "UNKNOWN";
                    entityNode = null;
            }

            // 构造一个简易的实体展示，例如打印 id、order_no、user_id、status
            if (entityNode != null && !entityNode.isNull()) {
                int id = entityNode.get("id").asInt();
                String orderNo = entityNode.get("order_no").asText();
                int userId = entityNode.get("user_id").asInt();
                String status = entityNode.get("status").asText();

                log.info("DML=[{}]  latestEntity=[id={}, orderNo={}, userId={}, status={}]",
                        dml, id, orderNo, userId, status);
            } else {
                log.info("DML=[{}]  no entity data available", dml);
            }
            return null;
        }
    }

}
