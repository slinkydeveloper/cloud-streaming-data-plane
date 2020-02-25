package com.slinkydeveloper.cloud.streaming.engine.function.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.slinkydeveloper.cloud.streaming.engine.function.FunctionInvoker;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.CloudEventImpl;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.client.WebClient;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FunctionInvokerImpl implements FunctionInvoker {

    public static SocketAddress UDS_FUNCTION_ADDRESS = SocketAddress.domainSocketAddress("/data/function");
    public static String BUNDLE_CONTENT_TYPE = "application/cloudevents-bundle+json";

    private WebClient client;

    public FunctionInvokerImpl(Vertx vertx) {
        this.client = WebClient.create(vertx);
    }

    // SUPER INEFFICIENT but it's a demo
    // In next iterations the invocation could be done preparing multipart envelopes
    @Override
    public Future<Map<String, CloudEvent>> call(Map<String, CloudEvent> in) {
        return client
            .request(HttpMethod.POST, UDS_FUNCTION_ADDRESS, "/")
            .putHeader("content-type", BUNDLE_CONTENT_TYPE)
            .sendBuffer(createRequestBody(in))
            .compose(response -> {
                if (response.statusCode() >= 400) {
                    return Future.failedFuture("Error happened in user function invocation. Status code: " + response.statusCode() + ", Body: " + response.bodyAsString());
                } else {
                    return Future.succeededFuture(response.bodyAsJsonObject());
                }
            })
            .map(this::parseResponse);
    }

    private Buffer createRequestBody(Map<String, CloudEvent> in) {
        return in
            .entrySet()
            .stream()
            .map(e -> new JsonObject().put(e.getKey(), toJson(e.getValue())))
            .reduce(new JsonObject(), JsonObject::mergeIn)
            .toBuffer();
    }

    private Map<String, CloudEvent> parseResponse(JsonObject response) {
        if (response == null) return new HashMap<>();
        return response
            .stream()
            .map(e -> new AbstractMap.SimpleImmutableEntry<>(
                e.getKey(),
                toEvent((JsonObject) e.getValue())
            ))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }


    //TODO highly inefficient, this should be improved in sdk-java
    private JsonObject toJson(CloudEvent event) {
        return new JsonObject(io.cloudevents.json.Json.encode(event));
    }

    private CloudEvent toEvent(JsonObject obj) {
        return io.cloudevents.json.Json.decodeValue(obj.toString(), new TypeReference<CloudEventImpl>() {
        });
    }
}
