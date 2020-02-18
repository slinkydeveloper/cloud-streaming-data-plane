package com.slinkydeveloper.cloud.streaming.engine.api;

public enum TimeoutStrategy {
    DROP,
    SEND_IN_DLQ,
    INVOKE_FN,
}
