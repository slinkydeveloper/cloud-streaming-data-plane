package com.slinkydeveloper.cloud.streaming.engine.messaging;

import io.cloudevents.CloudEvent;

import java.time.ZonedDateTime;

public class MockMessage implements Message {

    String stream;
    String key;
    long partition;
    ZonedDateTime timestamp;
    CloudEvent event;

    public MockMessage(String stream, String key, long partition, ZonedDateTime timestamp, CloudEvent event) {
        this.stream = stream;
        this.key = key;
        this.partition = partition;
        this.timestamp = timestamp;
        this.event = event;
    }

    @Override
    public String stream() {
        return stream;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public long partition() {
        return partition;
    }

    @Override
    public ZonedDateTime timestamp() {
        return timestamp;
    }

    @Override
    public CloudEvent<?, ?> toEvent() {
        return event;
    }
}
