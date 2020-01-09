package com.slinkydeveloper.cloud.streaming.runtime;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class AggregationOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(AggregationOrchestrator.class);

    private Vertx vertx;
    private Map<Buffer, Aggregation> joiningInstances;
    private Map<Buffer, ArrayDeque<Aggregation>> instances;

    private KafkaProducer<Buffer, Buffer> producer;

    private Set<String> requiredInputStreams;
    private Set<String> outputStreams;
    private Duration timeout;

    private FunctionInvoker functionInvoker;

    public AggregationOrchestrator(Vertx vertx, KafkaProducer<Buffer, Buffer> producer, FunctionInvoker functionInvoker, Set<String> requiredInputStreams, Set<String> outputStreams, Duration timeout) {
        this.vertx = vertx;
        this.outputStreams = outputStreams;
        this.producer = producer;
        this.requiredInputStreams = requiredInputStreams;
        this.timeout = timeout;
        this.functionInvoker = functionInvoker;
        this.joiningInstances = new HashMap<>();
        this.instances = new HashMap<>();
    }

    public void onNewMessage(KafkaConsumerRecord<Buffer, Buffer> record) {
        ArrayDeque<Aggregation> queue = instances.computeIfAbsent(record.key(), buf -> new ArrayDeque<>());

        Optional<Aggregation> result = queue
            .stream()
            .filter(ai -> !ai.hasRecordFromStream(record.topic()))
            .findFirst();

        // If another queue is waiting for this element, put there and done
        // Otherwise create a new aggregation instance
        if (result.isPresent()) {
            result.get().addNewMessage(record.topic(), record);
        } else {
            Aggregation aggregation = newAggregation(record.key());
            aggregation.addNewMessage(record.topic(), record);
            queue.push(aggregation);
        }

        triggerExecution();

        logActualState();
    }

    public void onAggregationInstanceTimeout(Buffer key, Aggregation aggregation) {
        instances.get(key).remove(aggregation);
        triggerExecution();
    }

    public void onAggregationInstanceEnd(Buffer key, Aggregation aggregation) {
        joiningInstances.remove(key);
        triggerExecution();
    }

    public void triggerExecution() {
        // Clean up empty queue
        instances.entrySet().removeIf(e -> e.getValue().isEmpty());

        // Start new executions
        instances.forEach((key, queue) -> {
            boolean alreadyAggregating = joiningInstances.containsKey(key);
            boolean readyToAggregate = queue.element().isReady();

            if (!alreadyAggregating && readyToAggregate) {
                Aggregation nextAggregation = queue.remove();
                nextAggregation.startExecution();
                joiningInstances.put(key, nextAggregation);
            }
        });
    }

    private Aggregation newAggregation(Buffer aggregationKey) {
      return Aggregation.build()
          .setVertx(vertx)
          .setAggregationKey(aggregationKey)
          .setOutputStreams(outputStreams)
          .setRequiredStreams(requiredInputStreams)
          .setTimeout(timeout)
          .setOnEnd(this::onAggregationInstanceEnd)
          .setOnTimeout(this::onAggregationInstanceTimeout)
          .setFunctionInvoker(functionInvoker)
          .setProducer(producer)
          .build();
    }

    private void logActualState() {
        logger.debug("Running aggregations: {}", joiningInstances.size());
        logger.debug("Waiting aggregations: {}", instances.values().stream().map(ArrayDeque::size).reduce(Integer::sum).orElse(0));
        logger.debug("Waiting keys: {}", instances.keySet().stream().map(Buffer::toString).collect(Collectors.toList()));
    }

}
