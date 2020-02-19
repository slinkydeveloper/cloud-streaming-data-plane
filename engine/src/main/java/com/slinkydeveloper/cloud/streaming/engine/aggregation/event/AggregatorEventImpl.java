package com.slinkydeveloper.cloud.streaming.engine.aggregation.event;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.Aggregation;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import com.slinkydeveloper.cloud.streaming.engine.utils.TriConsumer;
import io.cloudevents.CloudEvent;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class AggregatorEventImpl implements AggregatorEvent {

    private AggregatorEventType eventType;
    private Object t0;
    private Object t1;
    private Object t2;

    public AggregatorEventImpl(AggregatorEventType eventType, Object t0, Object t1, Object t2) {
        this.eventType = eventType;
        this.t0 = t0;
        this.t1 = t1;
        this.t2 = t2;
    }

    @Override
    public AggregatorEventType type() {
        return eventType;
    }

    @Override
    public void onNewMessage(Consumer<Message> handler) {
        if (eventType == AggregatorEventType.NEW_MESSAGE) {
            handler.accept((Message) t0);
        }
    }

    @Override
    public void onExpiredMessage(TriConsumer<String, String, CloudEvent> handler) {
        if (eventType == AggregatorEventType.EXPIRED_MESSAGE) {
            handler.accept((String) t0, (String) t1, (CloudEvent) t2);
        }
    }

    @Override
    public void onFunctionInvocationStart(Consumer<Aggregation> handler) {
        if (eventType == AggregatorEventType.FUNCTION_INVOCATION_START) {
            handler.accept((Aggregation) t0);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onFunctionInvocationEnded(BiConsumer<Aggregation, Map<String, CloudEvent>> handler) {
        if (eventType == AggregatorEventType.FUNCTION_INVOCATION_ENDED) {
            handler.accept((Aggregation) t0, (Map<String, CloudEvent>) t1);
        }
    }

    @Override
    public void onFunctionInvocationFailed(BiConsumer<Aggregation, Throwable> handler) {
        if (eventType == AggregatorEventType.FUNCTION_INVOCATION_FAILED) {
            handler.accept((Aggregation) t0, (Throwable) t1);
        }
    }

    @Override
    public void onSendFailed(BiConsumer<Aggregation, Throwable> handler) {
        if (eventType == AggregatorEventType.SEND_FAILED) {
            handler.accept((Aggregation) t0, (Throwable) t1);
        }
    }
}
