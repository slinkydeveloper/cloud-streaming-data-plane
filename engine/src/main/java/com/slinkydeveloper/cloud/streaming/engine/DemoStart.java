package com.slinkydeveloper.cloud.streaming.engine;

import com.slinkydeveloper.cloud.streaming.engine.api.InputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.OutputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.StreamProcessor;
import com.slinkydeveloper.cloud.streaming.engine.kafka.KafkaReceiverVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DemoStart {

    public static void main(String[] args) {
        String bootstrapServers = getEnv("KAFKA_BROKERS").get();
        String appId = getEnv("APP_ID").get();
        StreamProcessor model = new StreamProcessor(
            Arrays
                .stream(getEnv("INPUT_STREAMS", s -> s.split(Pattern.quote(","))).get())
                .map(InputStream::new)
                .collect(Collectors.toSet()),
            Arrays
                .stream(getEnv("OUTPUT_STREAMS", s -> s.split(Pattern.quote(","))).get())
                .map(OutputStream::new)
                .collect(Collectors.toSet()),
            getEnv("TIMEOUT", Duration::parse).orElse(null),
            null,
            stateStream);

        Vertx vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
        vertx.deployVerticle(new KafkaReceiverVerticle(
            bootstrapServers,
            appId,
            model
        )).setHandler(ar -> {
            if (ar.failed()) {
                System.out.println("Failed deploy: " + ar.cause());
            } else {
                System.out.println("Succeeded deploy of AggregatorVerticle");
            }
        });
    }

    public static Optional<String> getEnv(String key) {
        return getEnv(key, Function.identity());
    }

    public static <T> Optional<T> getEnv(String key, Function<String, T> mapper) {
        return Optional.ofNullable(System.getenv(key)).map(mapper);
    }

}
