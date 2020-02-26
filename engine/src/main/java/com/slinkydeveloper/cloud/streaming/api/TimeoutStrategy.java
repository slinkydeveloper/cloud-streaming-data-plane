package com.slinkydeveloper.cloud.streaming.api;

public enum TimeoutStrategy {
    DROP,
    SEND_IN_DLQ,
    INVOKE_FN,
}
