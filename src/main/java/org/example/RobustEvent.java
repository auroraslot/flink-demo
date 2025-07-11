package org.example;

/**
 * 原始事件，带 topic + JSON 字符串
 */
public class RobustEvent {
    public String topic;
    public String json;

    public RobustEvent() {
    }

    public RobustEvent(String topic, String json) {
        this.topic = topic;
        this.json = json;
    }
}