package com.slinkydeveloper.cloud.streaming.engine.event;

import com.slinkydeveloper.cloud.streaming.engine.FunctionInvocation;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import com.slinkydeveloper.cloud.streaming.engine.utils.TriConsumer;
import io.cloudevents.CloudEvent;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ProcessorNodeEventImpl implements ProcessorNodeEvent {

    private ProcessorNodeEventType eventType;
    private Object t0;
    private Object t1;
    private Object t2;

    public ProcessorNodeEventImpl(ProcessorNodeEventType eventType, Object t0, Object t1, Object t2) {
        this.eventType = eventType;
        this.t0 = t0;
        this.t1 = t1;
        this.t2 = t2;
    }

    @Override
    public ProcessorNodeEventType type() {
        return eventType;
    }

    @Override
    public void onNewMessage(Consumer<Message> handler) {
        if (eventType == ProcessorNodeEventType.NEW_MESSAGE) {
            handler.accept((Message) t0);
        }
    }

    @Override
    public void onExpiredMessage(TriConsumer<String, String, CloudEvent> handler) {
        if (eventType == ProcessorNodeEventType.EXPIRED_MESSAGE) {
            handler.accept((String) t0, (String) t1, (CloudEvent) t2);
        }
    }

    @Override
    public void onFunctionInvocationStart(Consumer<FunctionInvocation> handler) {
        if (eventType == ProcessorNodeEventType.FUNCTION_INVOCATION_START) {
            handler.accept((FunctionInvocation) t0);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onFunctionInvocationEnded(BiConsumer<FunctionInvocation, Map<String, CloudEvent>> handler) {
        if (eventType == ProcessorNodeEventType.FUNCTION_INVOCATION_END) {
            handler.accept((FunctionInvocation) t0, (Map<String, CloudEvent>) t1);
        }
    }

    @Override
    public void onFunctionInvocationFailed(BiConsumer<FunctionInvocation, Throwable> handler) {
        if (eventType == ProcessorNodeEventType.FUNCTION_INVOCATION_FAIL) {
            handler.accept((FunctionInvocation) t0, (Throwable) t1);
        }
    }

    @Override
    public void onSendFailed(BiConsumer<FunctionInvocation, Throwable> handler) {
        if (eventType == ProcessorNodeEventType.SEND_FAILED) {
            handler.accept((FunctionInvocation) t0, (Throwable) t1);
        }
    }

    @Override
    public String toString() {
        return "ProcessorNodeEvent " + eventType;
    }
}
