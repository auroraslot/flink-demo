package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Flink 作业，订阅多topic，按keyBy(topic)分组，按topic统计每分钟的 INSERT 事件数量，并汇总比较
 */
public class BinlogFlinkCompareJob {

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
        DataStream<RobustEvent> raw = env.addSource(consumer)
            .filter(Objects::nonNull);

        // 5. 解析、过滤出 INSERT 的事件
        DataStream<ParsedEvent> inserts = raw
            .map(evt -> {
                JsonNode payload = MAPPER.readTree(evt.json).get("payload");
                if (payload == null || payload.isNull()) {
                    return null;
                }
                String op = payload.get("op").asText();
                // 只保留 c → INSERT
                if (!"c".equals(op)) {
                    return null;
                }
                // 构造 ParsedEvent，保留 topic 以便后面分组
                return new ParsedEvent(evt.topic, /* timestamp */ payload.get("ts_ms").asLong());
            })
            .filter(Objects::nonNull)
            // 用事件时间做 Window，如果你想用处理时间可以改成 ProcessingTimeWindow
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                  .<ParsedEvent>forMonotonousTimestamps()
                  .withTimestampAssigner((e, ts) -> e.ts)
            );

        // 6. 按 topic 做 1 分钟 Tumbling Window 计数
        DataStream<Tuple2<String, Long>> topicCounts = inserts
            .map(e -> Tuple2.of(e.topic, 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .sum(1);

        // 7. 在同一个 1 分钟窗口结束时，收集两个 topic 的 count 并比较
        topicCounts
            .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
            .process(new ProcessAllWindowFunction<
                                Tuple2<String,Long>, String, TimeWindow>() {
                @Override
                public void process(
                    Context ctx,
                    Iterable<Tuple2<String,Long>> elements,
                    Collector<String> out) {

                    long countOrder  = 0;
                    long countRequest = 0;
                    for (Tuple2<String,Long> t : elements) {
                        if (t.f0.equals("dbserver.testdb.trade_core_order")) {
                            countOrder = t.f1;
                        } else if (t.f0.equals("dbserver.testdb.trade_request")) {
                            countRequest = t.f1;
                        }
                    }
                    if (countOrder == countRequest) {
                        out.collect(String.format(
                          "Window [%s - %s]: 两个 topic INSERT 数量相同 = %d",
                          Instant.ofEpochMilli(ctx.window().getStart()),
                          Instant.ofEpochMilli(ctx.window().getEnd()),
                          countOrder
                        ));
                    } else {
                        out.collect(String.format(
                          "Window [%s - %s]: 数量不等！order=%d, request=%d",
                          Instant.ofEpochMilli(ctx.window().getStart()),
                          Instant.ofEpochMilli(ctx.window().getEnd()),
                          countOrder, countRequest
                        ));
                    }
                }
            })
            .print();

        // 8. 执行 Job
        env.execute("Binlog INSERT Count Compare");
    }

    /** 原始事件，带 topic + JSON 字符串 */
    public static class RobustEvent {
        public String topic;
        public String json;
        public RobustEvent() {}
        public RobustEvent(String topic, String json) {
            this.topic = topic;
            this.json = json;
        }
    }

    /** 解析后只保留 topic + 时间戳 */
    public static class ParsedEvent {
        public String topic;
        public long   ts;
        public ParsedEvent() {}
        public ParsedEvent(String topic, long ts) {
            this.topic = topic;
            this.ts    = ts;
        }
    }
}
