package com.slinkydeveloper.cloud.streaming.engine.kafka;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.AggregationOrchestrator;
import com.slinkydeveloper.cloud.streaming.engine.api.InputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.OutputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.StreamProcessor;
import com.slinkydeveloper.cloud.streaming.engine.function.FunctionInvoker;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaReceiverVerticle extends AbstractVerticle {

    private String bootstrapServers;
    private String appId;
    private StreamProcessor model;

    public KafkaReceiverVerticle(String bootstrapServers, String appId, StreamProcessor model) {
        this.bootstrapServers = bootstrapServers;
        this.appId = appId;
        this.model = model;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapServers);
        config.put("key.serializer", "io.vertx.kafka.client.serialization.BufferSerializer");
        config.put("value.serializer", "io.vertx.kafka.client.serialization.BufferSerializer");
        config.put("key.deserializer", "io.vertx.kafka.client.serialization.BufferDeserializer");
        config.put("value.deserializer", "io.vertx.kafka.client.serialization.BufferDeserializer");
        config.put("group.id", appId);
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true"); // Fuck yeah it's a prototype
        config.put("acks", "1");

        Set<String> inputTopics = model.getInputStreams().stream().map(InputStream::getName).collect(Collectors.toSet());

        AggregationOrchestrator orchestrator = new AggregationOrchestrator(
            vertx,
            KafkaProducer.createShared(vertx, "aggregator-producer-" + appId, config),
            new FunctionInvoker(vertx),
            inputTopics,
            model.getOutputStreams().stream().map(OutputStream::getName).collect(Collectors.toSet()),
            model.getTimeout()
        );

        KafkaConsumer<Buffer, Buffer> consumer = KafkaConsumer.create(vertx, config);
        consumer.handler(orchestrator::onNewMessage);
        consumer
            .subscribe(inputTopics)
            .setHandler(startPromise);
    }
}
