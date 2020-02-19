package com.slinkydeveloper.cloud.streaming.engine.aggregation;

import io.cloudevents.CloudEvent;

import java.util.Map;

//TODO rename to Aggregation
public class Aggregation {

    private String aggregationKey;
    private Map<String, CloudEvent> input;
    private CloudEvent state;

    public Aggregation(String aggregationKey, Map<String, CloudEvent> input, CloudEvent state) {
        this.aggregationKey = aggregationKey;
        this.input = input;
        this.state = state;
    }

    public String getAggregationKey() {
        return aggregationKey;
    }

    public Map<String, CloudEvent> getInput() {
        return input;
    }

    public CloudEvent getState() {
        return state;
    }
}
