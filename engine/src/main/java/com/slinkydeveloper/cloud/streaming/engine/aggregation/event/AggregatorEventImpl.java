package com.slinkydeveloper.cloud.streaming.engine.aggregation.event;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.RunningAggregation;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class AggregatorEventImpl implements AggregatorEvent {

    private AggregatorEventType eventType;
    private RunningAggregation runningAggregation;
    private Object extraPayload;

    protected AggregatorEventImpl(AggregatorEventType eventType, RunningAggregation runningAggregation, Object extraPayload) {
        this.eventType = eventType;
        this.extraPayload = extraPayload;
        this.runningAggregation = runningAggregation;
    }

    @Override
    public AggregatorEventType type() {
        return eventType;
    }

    @Override
    public void onNewMessage(Consumer<Message> handler) {
        if (eventType == AggregatorEventType.NEW_MESSAGE) {
            handler.accept((Message) extraPayload);
        }
    }

    @Override
    public void onExpiredMessage(Consumer<Message> handler) {
        if (eventType == AggregatorEventType.EXPIRED_MESSAGE) {
            handler.accept((Message) extraPayload);
        }
    }

    @Override
    public void onFunctionInvocationStart(Consumer<RunningAggregation> handler) {
        if (eventType == AggregatorEventType.FUNCTION_INVOCATION_START) {
            handler.accept(runningAggregation);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onFunctionInvocationEnded(BiConsumer<RunningAggregation, Map<String, Message>> handler) {
        if (eventType == AggregatorEventType.FUNCTION_INVOCATION_ENDED) {
            handler.accept(runningAggregation, (Map<String, Message>) extraPayload);
        }
    }

    @Override
    public void onFunctionInvocationFailed(BiConsumer<RunningAggregation, Exception> handler) {
        if (eventType == AggregatorEventType.FUNCTION_INVOCATION_FAILED) {
            handler.accept(runningAggregation, (Exception) extraPayload);
        }
    }

    @Override
    public void onSendFailed(BiConsumer<RunningAggregation, Exception> handler) {
        if (eventType == AggregatorEventType.SEND_FAILED) {
            handler.accept(runningAggregation, (Exception) extraPayload);
        }
    }
}
