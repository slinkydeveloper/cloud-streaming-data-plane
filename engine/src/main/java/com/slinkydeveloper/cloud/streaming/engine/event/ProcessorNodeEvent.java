package com.slinkydeveloper.cloud.streaming.engine.event;

import com.slinkydeveloper.cloud.streaming.engine.FunctionInvocation;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import com.slinkydeveloper.cloud.streaming.engine.utils.TriConsumer;
import io.cloudevents.CloudEvent;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface ProcessorNodeEvent {

    ProcessorNodeEventType type();

    void onNewMessage(Consumer<Message> handler);

    void onExpiredMessage(TriConsumer<String, String, CloudEvent> handler);

    void onFunctionInvocationStart(Consumer<FunctionInvocation> handler);

    void onFunctionInvocationEnded(BiConsumer<FunctionInvocation, Map<String, CloudEvent>> handler);

    void onFunctionInvocationFailed(BiConsumer<FunctionInvocation, Throwable> handler);

    void onSendFailed(BiConsumer<FunctionInvocation, Throwable> handler);

    static ProcessorNodeEvent createNewMessageEvent(Message message) {
        return new ProcessorNodeEventImpl(ProcessorNodeEventType.NEW_MESSAGE, message, null, null);
    }

    static ProcessorNodeEvent createExpiredMessageEvent(String key, String stream, CloudEvent event) {
        return new ProcessorNodeEventImpl(ProcessorNodeEventType.EXPIRED_MESSAGE, key, stream, event);
    }

    static ProcessorNodeEvent createFunctionInvocationStartEvent(FunctionInvocation functionInvocation) {
        return new ProcessorNodeEventImpl(ProcessorNodeEventType.FUNCTION_INVOCATION_START, functionInvocation, null, null);
    }

    static ProcessorNodeEvent createFunctionInvocationEndEvent(FunctionInvocation functionInvocation, Map<String, CloudEvent> result) {
        return new ProcessorNodeEventImpl(ProcessorNodeEventType.FUNCTION_INVOCATION_END, functionInvocation, result, null);
    }

    static ProcessorNodeEvent createFunctionInvocationFailEvent(FunctionInvocation functionInvocation, Throwable exception) {
        return new ProcessorNodeEventImpl(ProcessorNodeEventType.FUNCTION_INVOCATION_FAIL, functionInvocation, exception, null);
    }

    static ProcessorNodeEvent createSendFailedEvent(FunctionInvocation functionInvocation, Throwable exception) {
        return new ProcessorNodeEventImpl(ProcessorNodeEventType.SEND_FAILED, functionInvocation, exception, null);
    }

}
