package com.slinkydeveloper.cloud.streaming.engine;

import io.cloudevents.CloudEvent;

import java.util.Map;

public class FunctionInvocation {

    private String key;
    private Map<String, CloudEvent> input;
    private CloudEvent state;

    public FunctionInvocation(String key, Map<String, CloudEvent> input, CloudEvent state) {
        this.key = key;
        this.input = input;
        this.state = state;
    }

    public String getKey() {
        return key;
    }

    public Map<String, CloudEvent> getInput() {
        return input;
    }

    public CloudEvent getState() {
        return state;
    }
}
