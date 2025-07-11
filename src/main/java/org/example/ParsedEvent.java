package org.example;

/**
 * 解析后只保留 topic + 时间戳
 */
public class ParsedEvent {
    public String topic;
    public long ts;

    public ParsedEvent() {
    }

    public ParsedEvent(String topic, long ts) {
        this.topic = topic;
        this.ts = ts;
    }
}