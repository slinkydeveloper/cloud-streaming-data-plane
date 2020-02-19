package com.slinkydeveloper.cloud.streaming.engine.function;

import com.slinkydeveloper.cloud.streaming.engine.function.impl.FunctionInvokerImpl;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.Map;

@FunctionalInterface
public interface FunctionInvoker {

    Future<Map<String, CloudEvent>> call(Map<String, CloudEvent> in);

    static FunctionInvoker create(Vertx vertx) {
        return new FunctionInvokerImpl(vertx);
    }

}
