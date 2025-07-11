package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@Slf4j
public class TradeCoreOrderRequestAlertJob {

    // 用于 JSON 解析
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // 1. 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        // 2. Kafka 配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:29092");
        props.setProperty("group.id", "flink-consumer-group");

        // 3. 支持多 Topic 订阅
        List<String> topics = Arrays.asList(
                "dbserver.testdb.trade_core_order",
                "dbserver.testdb.trade_request"
        );

        // 我们需要拿到 topic 信息，所以下面用 KafkaDeserializationSchema
        FlinkKafkaConsumer<RobustEvent> consumer =
                new FlinkKafkaConsumer<>(
                        topics,
                        new KafkaDeserializationSchema<RobustEvent>() {
                            @Override
                            public boolean isEndOfStream(RobustEvent nextElement) {
                                return false;
                            }

                            @Override
                            public RobustEvent deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                                String topic = record.topic();
                                byte[] value = record.value();
                                if (value == null) {
                                    return null;
                                }
                                String json = new String(value, StandardCharsets.UTF_8);
                                return new RobustEvent(topic, json);
                            }

                            @Override
                            public TypeInformation<RobustEvent> getProducedType() {
                                return TypeInformation.of(RobustEvent.class);
                            }
                        },
                        props
                );
        consumer.setCommitOffsetsOnCheckpoints(true);

        // 4. 添加数据源
        DataStream<RobustEvent> raw = env.addSource(consumer).name("Kafka Source")
                .filter(Objects::nonNull).name("Kafka Sink");

        // 5. 过滤出需要计算的事件
        SingleOutputStreamOperator<ParsedEvent> withTs = raw.map(robustEvent -> {
                    // 先解析成JSON对象
                    JsonNode payload = MAPPER.readTree(robustEvent.json).get("payload");
                    if (payload == null) {
                        return null; // 跳过空事件
                    }
                    // 获取操作类型
                    String op = payload.get("op").asText();
                    if (!StringUtils.equals(op, "c")) {
                        return null;
                    }

                    // 如果是 INSERT
                    return new ParsedEvent(
                            robustEvent.topic,
                            payload.get("ts_ms").asLong());
                })
                .filter(Objects::nonNull)
                // 标记水位线，这里容忍5秒的乱序，时间窗口调大5s也能解决问题，但是业务含义会有歧义
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ParsedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.ts)
                );

        // 按topic分组
        KeyedStream<ParsedEvent, String> keyWithTs = withTs.keyBy(parsedEvent -> parsedEvent.topic);

        // 定义CEP模式
        Pattern<ParsedEvent, ?> pattern = Pattern.<ParsedEvent>begin("core")
                .where(new SimpleCondition<ParsedEvent>() {
                    @Override
                    public boolean filter(ParsedEvent value) throws Exception {
                        return false;
                    }
                })
                .followedBy("request")
                .where(new SimpleCondition<ParsedEvent>() {
                    @Override
                    public boolean filter(ParsedEvent value) throws Exception {
                        return false;
                    }
                })
                .within(Time.minutes(1)); // 1分钟内

        PatternStream<ParsedEvent> ps = CEP.pattern(keyWithTs, pattern);
        SingleOutputStreamOperator<Either<Object, Object>> result = ps.select(
                new PatternTimeoutFunction<ParsedEvent, Object>() {
                    @Override
                    public Object timeout(Map<String, List<ParsedEvent>> pattern, long timeoutTimestamp) throws Exception {
                        ParsedEvent core = pattern.get("core").get(0);
                        return String.format(
                            "超过1分钟未匹配到 request，报警！core topic: %s, timeout at: %d",
                            core.topic, timeoutTimestamp
                        );

                    }
                },
                new PatternSelectFunction<ParsedEvent, Object>() {
                    @Override
                    public Object select(Map<String, List<ParsedEvent>> pattern) throws Exception {
                        // 成功匹配的可以忽略或记录
                        ParsedEvent core = pattern.get("core").get(0);
                        ParsedEvent request = pattern.get("request").get(0);
                        return String.format(
                            "成功匹配 core 和 request，core topic: %s, request topic: %s",
                            core.topic, request.topic
                        );
                    }
                }
        );

        // 6. 输出结果到控制台
        result.addSink(new PrintSinkFunction<>())
                .name("Print Sink")
                .setParallelism(1);

        // 7. 执行作业
        env.execute("Trade Core Order Request Alert Job");
    }
}
