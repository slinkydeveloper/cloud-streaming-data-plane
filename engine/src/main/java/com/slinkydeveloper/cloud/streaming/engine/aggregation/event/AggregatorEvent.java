package com.slinkydeveloper.cloud.streaming.engine.aggregation.event;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.RunningAggregation;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface AggregatorEvent {

    AggregatorEventType type();

    void onNewMessage(Consumer<Message> handler);

    void onExpiredMessage(Consumer<Message> handler);

    void onFunctionInvocationStart(Consumer<RunningAggregation> handler);

    void onFunctionInvocationEnded(BiConsumer<RunningAggregation, Map<String, Message>> handler);

    void onFunctionInvocationFailed(BiConsumer<RunningAggregation, Exception> handler);

    void onSendFailed(BiConsumer<RunningAggregation, Exception> handler);

    static AggregatorEvent createNewMessageEvent(Message message) {
        return new AggregatorEventImpl(AggregatorEventType.NEW_MESSAGE, null, message);
    }

    static AggregatorEvent createExpiredMessageEvent(Message message) {
        return new AggregatorEventImpl(AggregatorEventType.EXPIRED_MESSAGE, null, message);
    }

    static AggregatorEvent createFunctionInvocationStartEvent(RunningAggregation aggregation) {
        return new AggregatorEventImpl(AggregatorEventType.FUNCTION_INVOCATION_START, aggregation, null);
    }

    static AggregatorEvent createFunctionInvocationEndedEvent(RunningAggregation aggregation, Map<String, Message> result) {
        return new AggregatorEventImpl(AggregatorEventType.FUNCTION_INVOCATION_ENDED, aggregation, result);
    }

    static AggregatorEvent createFunctionInvocationFailedEvent(RunningAggregation aggregation, Exception exception) {
        return new AggregatorEventImpl(AggregatorEventType.FUNCTION_INVOCATION_FAILED, aggregation, exception);
    }

    static AggregatorEvent createSendFailedEvent(RunningAggregation aggregation, Exception exception) {
        return new AggregatorEventImpl(AggregatorEventType.SEND_FAILED, aggregation, exception);
    }

}
