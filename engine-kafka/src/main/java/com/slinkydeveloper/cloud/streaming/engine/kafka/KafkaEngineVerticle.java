package com.slinkydeveloper.cloud.streaming.engine.kafka;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.event.AggregatorEvent;
import com.slinkydeveloper.cloud.streaming.engine.aggregation.orchestrator.AggregationOrchestrator;
import com.slinkydeveloper.cloud.streaming.engine.api.InputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.OutputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.StreamProcessor;
import com.slinkydeveloper.cloud.streaming.engine.function.FunctionInvoker;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import com.slinkydeveloper.cloud.streaming.engine.utils.ApiEnvReader;
import io.cloudevents.json.Json;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaEngineVerticle extends AbstractVerticle {

    private String bootstrapServers;
    private String appId;
    private StreamProcessor model;

    public KafkaEngineVerticle(String bootstrapServers, String appId, StreamProcessor model) {
        this.bootstrapServers = bootstrapServers;
        this.appId = appId;
        this.model = model;
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapServers);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", appId);
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "true"); // Fuck yeah it's a prototype
        config.put("acks", "1");

        Set<String> inputTopics = model.getInputStreams().stream().map(InputStream::getName).collect(Collectors.toSet());

        KafkaProducer<String, byte[]> producer = KafkaProducer.createShared(vertx, "aggregator-" + appId, config);

        AggregationOrchestrator orchestrator = new AggregationOrchestrator(
            vertx,
            FunctionInvoker.create(vertx),
            inputTopics,
            model.getOutputStreams().stream().map(OutputStream::getName).collect(Collectors.toSet()),
            model.getStateStream(),
            model.getTimeout(),
            (stream, key, cloudEvent) -> producer.send(KafkaProducerRecord.create(stream, key, Json.binaryEncode(cloudEvent))).mapEmpty()
        );

        KafkaConsumer<String, byte[]> consumer = KafkaConsumer.create(vertx, config);
        consumer.handler(stringKafkaConsumerRecord -> {
            Message message = new KafkaMessage(stringKafkaConsumerRecord);
            orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(message));
        });
        consumer
            .subscribe(inputTopics)
            .setHandler(startPromise);
    }

    public static void main(String[] args) {
        String bootstrapServers = ApiEnvReader.getEnv("KAFKA_BROKERS").get();
        String appId = ApiEnvReader.getEnv("APP_ID").get();
        StreamProcessor streamProcessorModel = ApiEnvReader.readStreamProcessorFromEnv();

        Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
        vertx.deployVerticle(new KafkaEngineVerticle(
            bootstrapServers,
            appId,
            streamProcessorModel
        )).setHandler(ar -> {
            if (ar.failed()) {
                System.out.println("Failed deploy: " + ar.cause());
            } else {
                System.out.println("Succeeded deploy of AggregatorVerticle");
            }
        });
    }

}
