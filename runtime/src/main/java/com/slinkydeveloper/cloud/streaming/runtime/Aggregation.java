package com.slinkydeveloper.cloud.streaming.runtime;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class Aggregation {

    private static final Logger logger = LoggerFactory.getLogger(Aggregation.class);

    /**
     * Vertx instance
     */
    private Vertx vertx;

    /**
     * Key for this aggregation
     */
    private Buffer aggregationKey;

    /**
     * Required input streams to run aggregation
     */
    private Set<String> requiredStreams;

    /**
     * Output Streams
     */
    private Set<String> outputStreams;

    /**
     * Output Producer
     */
    private KafkaProducer<Buffer, Buffer> producer;

    /**
     * Received records
     */
    private Map<String, KafkaConsumerRecord<Buffer, Buffer>> receivedRecords;

    /**
     * On end listener
     */
    private BiConsumer<Buffer, Aggregation> onEnd;

    /**
     * On failure listener
     */
    private BiConsumer<Buffer, Aggregation> onFailure;

    /**
     * On timeout listener
     */
    private BiConsumer<Buffer, Aggregation> onTimeout;

    /**
     * Timeout
     */
    private Duration waitEventsTimeout;

    /**
     * Function invoker to actually call the function that performs the aggregation
     */
    private FunctionInvoker functionInvoker;

    /**
     * Flag to check if the aggregation function is executing
     */
    private boolean executing;

    /**
     * Timer id to cancel when aggregation is completed
     */
    private Long timerId;

    private Aggregation(Vertx vertx, Buffer aggregationKey, Set<String> requiredStreams, Set<String> outputStreams, KafkaProducer<Buffer, Buffer> producer, BiConsumer<Buffer, Aggregation> onEnd, BiConsumer<Buffer, Aggregation> onFailure, BiConsumer<Buffer, Aggregation> onTimeout, Duration waitEventsTimeout, FunctionInvoker functionInvoker) {
        this.vertx = vertx;
        this.aggregationKey = aggregationKey;
        this.requiredStreams = requiredStreams;
        this.outputStreams = outputStreams;
        this.producer = producer;
        this.onFailure = onFailure;
        this.waitEventsTimeout = waitEventsTimeout;
        this.functionInvoker = functionInvoker;
        this.receivedRecords = new HashMap<>();
        this.onEnd = onEnd;
        this.onTimeout = onTimeout;
        this.executing = false;
    }

    public void addNewMessage(String stream, KafkaConsumerRecord<Buffer, Buffer> record) {
        handleNewMessage(stream, record);
    }

    public void startExecution() {
        handleInvokeUserFunction();
    }

    public boolean hasRecordFromStream(String stream) {
        return receivedRecords.containsKey(stream);
    }

    public boolean isExecuting() {
        return executing;
    }

    public boolean isReady() {
        return receivedRecords.keySet().equals(requiredStreams);
    }

    // Lifecycle

    private void handleNewMessage(String stream, KafkaConsumerRecord<Buffer, Buffer> record) {
        if (this.receivedRecords.size() == 0) { // Need to setup timer
            startTimer();
        }

        KafkaConsumerRecord<Buffer, Buffer> evicted = this.receivedRecords.put(stream, record);
        if (evicted != null) {
            logger.debug("Evicted message from topic {} with offset {} and key {}", evicted.topic(), evicted.offset(), evicted.key().toString());
        }

        if (this.receivedRecords.size() == requiredStreams.size()) { // All events received
            stopTimer();
        }
    }

    private void handleTimeout() {
        logger.debug("Expired aggregation for key {}", this.aggregationKey.toString());
        this.onTimeout.accept(this.aggregationKey, this);
    }

    private void handleInvokeUserFunction() {
        if (!isReady()) {
            throw new IllegalStateException("Cannot invoke user function if aggregation is not ready to be processed");
        }

        this.executing = true;
        functionInvoker.call(this.receivedRecords).setHandler(ar -> {
            this.executing = false;
            if (ar.failed()) {
                handleFailure(ar.cause());
            } else {
                handleReplyUserFunctionSuccess(ar.result());
            }
        });
    }

    private void handleReplyUserFunctionSuccess(Map<String, Buffer> response) {
        CompositeFuture.all(
            response
                .entrySet()
                .stream()
                .filter(e -> this.outputStreams.contains(e.getKey()))
                .map(e -> producer.send(
                    KafkaProducerRecord.create(e.getKey(), this.aggregationKey, e.getValue())
                ))
                .collect(Collectors.toList())
        ).setHandler(ar -> {
            if (ar.failed()) {
                handleFailure(ar.cause());
            } else {
                handleEnd();
            }
        });
    }

    private void handleFailure(Throwable exception) {
        logger.error("Failure in aggregation for key " + aggregationKey.toString(), exception);
        this.onFailure.accept(this.aggregationKey, this);
    }

    private void handleEnd() {
        logger.debug("Ended aggregation for key {}", this.aggregationKey.toString());
        this.onEnd.accept(this.aggregationKey, this);
    }

    // Helpers

    private void startTimer() {
        if (this.waitEventsTimeout != null) {
            timerId = vertx.setTimer(waitEventsTimeout.toMillis(), h -> {
                if (h != null) handleTimeout();
            });
        }
    }

    private void stopTimer() {
        if (this.timerId != null) {
            vertx.cancelTimer(this.timerId);
        }
    }

    // Builder

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        private Vertx vertx;
        private Buffer aggregationKey;
        private Set<String> requiredStreams;
        private Set<String> outputStreams;
        private BiConsumer<Buffer, Aggregation> onEnd;
        private BiConsumer<Buffer, Aggregation> onFailure;
        private BiConsumer<Buffer, Aggregation> onTimeout;
        private Duration timeout;
        private FunctionInvoker functionInvoker;
        private KafkaProducer<Buffer, Buffer> producer;

        public Aggregation.Builder setVertx(Vertx vertx) {
            this.vertx = vertx;
            return this;
        }

        public Aggregation.Builder setAggregationKey(Buffer aggregationKey) {
            this.aggregationKey = aggregationKey;
            return this;
        }

        public Aggregation.Builder setRequiredStreams(Set<String> requiredStreams) {
            this.requiredStreams = requiredStreams;
            return this;
        }

        public Aggregation.Builder setOutputStreams(Set<String> outputStreams) {
            this.outputStreams = outputStreams;
            return this;
        }

        public Aggregation.Builder setOnEnd(BiConsumer<Buffer, Aggregation> onEnd) {
            this.onEnd = onEnd;
            return this;
        }

        public Aggregation.Builder setOnFailure(BiConsumer<Buffer, Aggregation> onFailure) {
            this.onFailure = onFailure;
            return this;
        }

        public Aggregation.Builder setOnTimeout(BiConsumer<Buffer, Aggregation> onTimeout) {
            this.onTimeout = onTimeout;
            return this;
        }

        public Aggregation.Builder setTimeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        public Aggregation.Builder setFunctionInvoker(FunctionInvoker functionInvoker) {
            this.functionInvoker = functionInvoker;
            return this;
        }

        public Aggregation.Builder setProducer(KafkaProducer<Buffer, Buffer> producer) {
            this.producer = producer;
            return this;
        }

        public Aggregation build() {
            Objects.requireNonNull(vertx);
            Objects.requireNonNull(aggregationKey);
            Objects.requireNonNull(requiredStreams);
            Objects.requireNonNull(outputStreams);
            Objects.requireNonNull(onEnd);
            Objects.requireNonNull(onFailure);
            Objects.requireNonNull(onTimeout);
            Objects.requireNonNull(functionInvoker);
            Objects.requireNonNull(producer);
            return new Aggregation(vertx, aggregationKey, requiredStreams, outputStreams, producer, onEnd, onFailure, onTimeout, timeout, functionInvoker);
        }

    }
}
