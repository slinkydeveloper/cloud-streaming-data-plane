package com.slinkydeveloper.cloud.streaming.engine.aggregation;

//TODO rename to Aggregation

import io.cloudevents.CloudEvent;
import io.vertx.core.buffer.Buffer;

import java.util.Map;

public class Aggregation {

    private Buffer aggregationKey;
    private Map<String, CloudEvent> input;
    private CloudEvent state;

    public Aggregation(Buffer aggregationKey, Map<String, CloudEvent> input, CloudEvent state) {
        this.aggregationKey = aggregationKey;
        this.input = input;
        this.state = state;
    }

    public Buffer getAggregationKey() {
        return aggregationKey;
    }

    public Map<String, CloudEvent> getInput() {
        return input;
    }

    public CloudEvent getState() {
        return state;
    }
}
