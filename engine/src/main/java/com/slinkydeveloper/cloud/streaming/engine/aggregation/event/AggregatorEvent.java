package com.slinkydeveloper.cloud.streaming.engine.aggregation.event;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.Aggregation;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import com.slinkydeveloper.cloud.streaming.engine.utils.TriConsumer;
import io.cloudevents.CloudEvent;
import io.vertx.core.buffer.Buffer;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface AggregatorEvent {

    AggregatorEventType type();

    void onNewMessage(Consumer<Message> handler);

    void onExpiredMessage(TriConsumer<Buffer, String, CloudEvent> handler);

    void onFunctionInvocationStart(Consumer<Aggregation> handler);

    void onFunctionInvocationEnded(BiConsumer<Aggregation, Map<String, CloudEvent>> handler);

    void onFunctionInvocationFailed(BiConsumer<Aggregation, Throwable> handler);

    void onSendFailed(BiConsumer<Aggregation, Throwable> handler);

    static AggregatorEvent createNewMessageEvent(Message message) {
        return new AggregatorEventImpl(AggregatorEventType.NEW_MESSAGE, message, null, null);
    }

    static AggregatorEvent createExpiredMessageEvent(Buffer key, String stream, CloudEvent event) {
        return new AggregatorEventImpl(AggregatorEventType.EXPIRED_MESSAGE, key, stream, event);
    }

    static AggregatorEvent createFunctionInvocationStartEvent(Aggregation aggregation) {
        return new AggregatorEventImpl(AggregatorEventType.FUNCTION_INVOCATION_START, aggregation, null, null);
    }

    static AggregatorEvent createFunctionInvocationEndedEvent(Aggregation aggregation, Map<String, CloudEvent> result) {
        return new AggregatorEventImpl(AggregatorEventType.FUNCTION_INVOCATION_ENDED, aggregation, result, null);
    }

    static AggregatorEvent createFunctionInvocationFailedEvent(Aggregation aggregation, Throwable exception) {
        return new AggregatorEventImpl(AggregatorEventType.FUNCTION_INVOCATION_FAILED, aggregation, exception, null);
    }

    static AggregatorEvent createSendFailedEvent(Aggregation aggregation, Throwable exception) {
        return new AggregatorEventImpl(AggregatorEventType.SEND_FAILED, aggregation, exception, null);
    }

}
