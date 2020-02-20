package com.slinkydeveloper.cloud.streaming.engine.aggregation.orchestrator;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.Aggregation;
import com.slinkydeveloper.cloud.streaming.engine.aggregation.event.AggregatorEvent;
import com.slinkydeveloper.cloud.streaming.engine.api.InputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.OutputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.StateStream;
import com.slinkydeveloper.cloud.streaming.engine.function.FunctionInvoker;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import com.slinkydeveloper.cloud.streaming.engine.utils.KeyExtractor;
import com.slinkydeveloper.cloud.streaming.engine.utils.TriFunction;
import io.cloudevents.CloudEvent;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//TODO this is per partition -> Aggregation Orchestrator : Partition
public class AggregationOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(AggregationOrchestrator.class);

    private Vertx vertx;
    private Map<String, HashMap<String, ArrayDeque<CloudEventQueueItem>>> waitingMessages;
    private Map<String, CloudEvent> lastState;
    private List<Aggregation> runningAggregations;

    private Map<String, InputStream> inputStreams;
    private Map<String, OutputStream> returnValuesNamesToOutputStreams;
    private StateStream stateStream;

    private Duration timeout;

    private FunctionInvoker functionInvokerImpl;

    private TriFunction<String, String, CloudEvent<?, ?>, Future<Void>> eventSender;

    public AggregationOrchestrator(Vertx vertx, FunctionInvoker functionInvokerImpl, Set<InputStream> inputStreams, Set<OutputStream> outputStreams, StateStream stateStream, Duration timeout, TriFunction<String, String, CloudEvent<?, ?>, Future<Void>> eventSender) {
        this.vertx = vertx;
        this.returnValuesNamesToOutputStreams = outputStreams
            .stream()
            .collect(Collectors.toMap(OutputStream::getFunctionReturnName, Function.identity()));
        this.inputStreams = inputStreams
            .stream()
            .collect(Collectors.toMap(InputStream::getName, Function.identity()));
        this.stateStream = stateStream;
        this.timeout = timeout;
        this.functionInvokerImpl = functionInvokerImpl;
        this.waitingMessages = new HashMap<>();
        this.runningAggregations = new ArrayList<>();
        this.eventSender = eventSender;
        this.lastState = new HashMap<>();
    }

    public void onEvent(AggregatorEvent event) {
        logger.debug("New event {}", event);
        logActualState();

        event.onNewMessage(message -> {
            enqueueMessage(message);
            triggerExecution();
        });

        event.onExpiredMessage((buffer, stream, cloudEvent) -> {
            //TODO implement expired strategy
            removeMessage(buffer, stream, cloudEvent);
        });

        event.onFunctionInvocationStart(aggregation -> {
            this.runningAggregations.add(aggregation);
            startAggregation(aggregation);
        });

        event.onFunctionInvocationEnded((aggregation, output) -> {
            forwardAggregationResponse(aggregation, output);
            this.runningAggregations.remove(aggregation);
        });

        event.onFunctionInvocationFailed((aggregation, throwable) -> {
            //TODO implement function invocation failure strategy
            handleFailure(throwable);
            this.runningAggregations.remove(aggregation);
        });

        event.onSendFailed((aggregation, throwable) -> {
            //TODO implement send failure strategy
            handleFailure(throwable);
        });

        logActualState();
    }

    private void triggerExecution() {
        this.waitingMessages.entrySet().stream().filter(e ->
            // Start new aggregation if there is at least one item in each stream event queue
            e.getValue()
                .entrySet()
                .stream()
                .filter(e1 -> e1.getValue().size() >= 1)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet())
                .equals(inputStreams.keySet())
        ).map(Map.Entry::getKey).collect(Collectors.toList()).forEach(key -> {
            // Dequeue messages
            Map<String, CloudEvent> aggregationInputMap = inputStreams
                .entrySet()
                .stream()
                .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getValue().getFunctionParameterName(), dequeueEvent(key, e.getKey())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            CloudEvent state = null;
            if (stateStream != null) {
                state = this.lastState.get(key);
            }

            Aggregation aggregation = new Aggregation(key, aggregationInputMap, state);

            // Trigger aggregation start
            onEvent(AggregatorEvent.createFunctionInvocationStartEvent(aggregation));
        });
    }

    private void enqueueMessage(Message message) {
        if (stateStream != null && message.stream().equals(stateStream)) {
            //TODO what should we do here? we need a comparison between the actual state and the new received one
            return;
        }

        InputStream stream = inputStreams.get(message.stream());
        if (stream == null) {
            handleFailure(new RuntimeException("Unknown stream " + message.stream()));
            return;
        }
        //TODO handle bad event
        CloudEvent event = message.toEvent();
        String key = KeyExtractor.extractKey(event, stream.getMetadataAsKey(), message.key());

        logger.debug("Received event in stream {} with key {} and id {}", stream.getName(), key, event.getAttributes().getId());

        HashMap<String, ArrayDeque<CloudEventQueueItem>> streamQueueMap = this.waitingMessages.computeIfAbsent(key, buf -> new HashMap<>());
        ArrayDeque<CloudEventQueueItem> waitingMessageForStream = streamQueueMap.computeIfAbsent(message.stream(), str -> new ArrayDeque<>());
        if (this.timeout != null) {
            long timerId = vertx.setTimer(this.timeout.toMillis(), h -> {
                if (h != null) {
                    onEvent(AggregatorEvent.createExpiredMessageEvent(message.key(), message.stream(), event));
                }
            });
            waitingMessageForStream.push(new CloudEventQueueItem(event, timerId));
        } else {
            waitingMessageForStream.push(new CloudEventQueueItem(event, null));
        }
    }

    private void startAggregation(Aggregation aggregation) {
        HashMap<String, CloudEvent> in = new HashMap<>(aggregation.getInput());
        if (aggregation.getState() != null) {
            in.put(stateStream.getFunctionReturnName(), aggregation.getState());
        }
        functionInvokerImpl.call(in).setHandler(ar -> {
            if (ar.failed()) {
                onEvent(AggregatorEvent.createFunctionInvocationFailedEvent(aggregation, ar.cause()));
            } else {
                onEvent(AggregatorEvent.createFunctionInvocationEndedEvent(aggregation, ar.result()));
            }
        });
    }

    private void forwardAggregationResponse(Aggregation aggregation, Map<String, CloudEvent> out) {
        logger.debug("Function invocation for key {} returned events {}", aggregation.getAggregationKey(), out.keySet());

        var outputEvents = out
            .entrySet()
            .stream()
            .filter(e -> this.returnValuesNamesToOutputStreams.containsKey(e.getKey()))
            .map(e -> new AbstractMap.SimpleImmutableEntry<>(this.returnValuesNamesToOutputStreams.get(e.getKey()), e.getValue()))
            .map(e -> new AbstractMap.SimpleImmutableEntry<>(
                new AbstractMap.SimpleImmutableEntry<>(
                    e.getKey().getName(),
                    KeyExtractor.extractKey(e.getValue(), e.getKey().getMetadataAsKey(), aggregation.getAggregationKey())
                ), e.getValue()
            ));

        if (stateStream != null && out.containsKey(stateStream.getFunctionReturnName())) {
            CloudEvent state = out.get(stateStream.getFunctionReturnName());
            String stateKey = KeyExtractor.extractKey(state, stateStream.getMetadataAsKey(), aggregation.getAggregationKey());
            updateState(stateKey, state);
            outputEvents =
                Stream.concat(
                    Stream.of(
                        new AbstractMap.SimpleImmutableEntry<>(
                            new AbstractMap.SimpleImmutableEntry<>(
                                stateStream.getName(),
                                stateKey
                            ),
                            state
                        )
                    ),
                    outputEvents
                );
        }

        CompositeFuture.all(
            outputEvents
                .map(e ->
                    this.eventSender
                        .accept(e.getKey().getKey(), e.getKey().getValue(), e.getValue())
                )
                .collect(Collectors.toList())
        ).setHandler(ar -> {
            if (ar.failed()) {
                handleFailure(ar.cause());
            }
        });
    }

    private CloudEvent dequeueEvent(String key, String stream) {
        HashMap<String, ArrayDeque<CloudEventQueueItem>> streamQueueMap = this.waitingMessages.get(key);
        if (streamQueueMap == null) {
            return null;
        }
        ArrayDeque<CloudEventQueueItem> waitingMessageForStream = streamQueueMap.get(stream);
        if (waitingMessageForStream == null) {
            return null;
        }
        CloudEventQueueItem item = waitingMessageForStream.remove();
        if (item.timerId != null) {
            vertx.cancelTimer(item.timerId);
        }

        if (waitingMessageForStream.isEmpty()) {
            // We don't need this queue anymore
            streamQueueMap.remove(stream);
        }
        if (streamQueueMap.isEmpty()) {
            // We don't need this map anymore
            this.waitingMessages.remove(key);
        }

        return item.event;
    }

    private void removeMessage(String key, String stream, CloudEvent event) {
        HashMap<String, ArrayDeque<CloudEventQueueItem>> streamQueueMap = this.waitingMessages.get(key);
        if (streamQueueMap == null) {
            return;
        }

        ArrayDeque<CloudEventQueueItem> waitingMessageForStream = streamQueueMap.get(stream);
        if (waitingMessageForStream != null) {
            waitingMessageForStream.removeIf(q -> {
                if (q.event.equals(event)) {
                    if (q.timerId != null) {
                        vertx.cancelTimer(q.timerId);
                    }
                    return true;
                }
                return false;
            });

            if (waitingMessageForStream.isEmpty()) {
                // We don't need this queue anymore
                streamQueueMap.remove(stream);
            }
            if (streamQueueMap.isEmpty()) {
                // We don't need this map anymore
                this.waitingMessages.remove(key);
            }
        }
    }

    private Map<String, Integer> waitingMessagesCountForKey(String key) {
        return Optional.ofNullable(
            waitingMessages.get(key)
        ).map(h -> h
            .entrySet()
            .stream()
            .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().size()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        ).orElse(new HashMap<>());
    }

    private void handleFailure(Throwable throwable) {
        logger.warn("Failure", throwable);
    }

    private void logActualState() {
        if (logger.isDebugEnabled()) {
            logger.debug("Waiting aggregation keys {}", this.waitingMessages.size());
            waitingMessages.keySet().forEach(k -> {
                logger.debug("Messages in queues for key {}: {}", k, waitingMessagesCountForKey(k));
            });
            logger.debug("Last state: {}", this.lastState.size());
            this.lastState.forEach((k, v) -> {
                logger.debug("State {}: {}", k, v.getAttributes().getId());
            });
        }
    }

    private void updateState(String key, CloudEvent state) {
        if (state == null) {
            this.lastState.remove(key);
            logger.debug("State removed");
            return;
        }
        this.lastState.put(key, state);
        logger.debug("State updated with new event id: {}", state.getAttributes().getId());
    }

    private class CloudEventQueueItem {

        private CloudEvent event;
        private Long timerId;

        private CloudEventQueueItem(CloudEvent event, Long timerId) {
            this.event = event;
            this.timerId = timerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CloudEventQueueItem that = (CloudEventQueueItem) o;
            return Objects.equals(event, that.event);
        }

        @Override
        public int hashCode() {
            return Objects.hash(event);
        }
    }

}
