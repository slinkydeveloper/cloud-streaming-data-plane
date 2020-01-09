package com.slinkydeveloper.cloud.streaming.runtime.api;

public enum TimeoutStrategy {
    DROP,
    SEND_IN_DLQ,
    INVOKE_FN,
}
