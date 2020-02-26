package com.slinkydeveloper.cloud.streaming.engine.event;

public enum ProcessorNodeEventType {
    NEW_MESSAGE,
    EXPIRED_MESSAGE,
    FUNCTION_INVOCATION_START,
    FUNCTION_INVOCATION_END,
    FUNCTION_INVOCATION_FAIL,
    SEND_FAILED,
}
