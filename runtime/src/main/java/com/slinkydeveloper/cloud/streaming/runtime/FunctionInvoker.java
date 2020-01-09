package com.slinkydeveloper.cloud.streaming.runtime;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FunctionInvoker {

    public static SocketAddress UDS_FUNCTION_ADDRESS = SocketAddress.domainSocketAddress("/data/function");

    private WebClient client;

    public FunctionInvoker(Vertx vertx) {
        this.client = WebClient.create(vertx);
    }

    // SUPER INEFFICIENT but it's a demo
    // In next iterations the invocation could be done preparing multipart envelopes
    public Future<Map<String, Buffer>> call(Map<String, KafkaConsumerRecord<Buffer, Buffer>> in) {
        return client
            .request(HttpMethod.POST, UDS_FUNCTION_ADDRESS, "/")
            .sendJson(createRequestBody(in))
            .compose(response -> {
                if (response.statusCode() >= 400) {
                    return Future.failedFuture("Error happened in user function invocation. Status code: " + response.statusCode());
                } else {
                    return Future.succeededFuture(response.bodyAsJsonObject());
                }
            })
            .map(this::parseResponse);
    }

    private JsonObject createRequestBody(Map<String, KafkaConsumerRecord<Buffer, Buffer>> in) {
        return in
            .values()
            .stream()
            .map(e -> new JsonObject().put(e.topic(), new JsonObject(e.value())))
            .reduce(new JsonObject(), JsonObject::mergeIn);
    }

    private Map<String, Buffer> parseResponse(JsonObject response) {
        if (response == null) return new HashMap<>();
        return response
            .stream()
            .map(e -> new AbstractMap.SimpleImmutableEntry<>(
                e.getKey(),
                ((JsonObject)e.getValue()).toBuffer()
            ))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
