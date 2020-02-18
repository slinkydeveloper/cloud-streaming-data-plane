package com.slinkydeveloper.cloud.streaming.engine.aggregation;

//TODO rename to Aggregation

import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;

import java.util.Map;

public class RunningAggregation {

    private String aggregationKey;
    private Map<String, Message> aggregationInputMap;

    public RunningAggregation(String aggregationKey, Map<String, Message> aggregationInputMap) {
        this.aggregationKey = aggregationKey;
        this.aggregationInputMap = aggregationInputMap;
    }
}
