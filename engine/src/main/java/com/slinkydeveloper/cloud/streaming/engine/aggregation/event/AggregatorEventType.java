package com.slinkydeveloper.cloud.streaming.engine.aggregation.event;

public enum AggregatorEventType {
    NEW_MESSAGE,
    EXPIRED_MESSAGE,
    FUNCTION_INVOCATION_START,
    FUNCTION_INVOCATION_ENDED,
    FUNCTION_INVOCATION_FAILED,
    SEND_FAILED,
}
