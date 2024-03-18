package com.practice.kafka.event;

public class MessageEvent {

    private final String key;

    private final String value;

    public MessageEvent(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

}
