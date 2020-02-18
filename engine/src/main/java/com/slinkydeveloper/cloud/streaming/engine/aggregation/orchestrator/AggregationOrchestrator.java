package com.slinkydeveloper.cloud.streaming.engine.aggregation.orchestrator;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.RunningAggregation;
import com.slinkydeveloper.cloud.streaming.engine.aggregation.event.AggregatorEvent;
import com.slinkydeveloper.cloud.streaming.engine.function.FunctionInvoker;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import io.cloudevents.CloudEvent;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

//TODO this is per partition -> Aggregation Orchestrator : Partition
public class AggregationOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(AggregationOrchestrator.class);

    private Vertx vertx;
    private Map<Buffer, HashMap<String, ArrayDeque<MessageQueueItem>>> waitingMessages;
    private List<RunningAggregation> runningAggregations;

    private Set<String> requiredInputStreams;
    private Set<String> outputStreams;
    private String stateStream;

    private Duration timeout;

    private FunctionInvoker functionInvoker;

    private BiFunction<String, CloudEvent<?, ?>, Future<Void>> eventSender;

    public AggregationOrchestrator(Vertx vertx, FunctionInvoker functionInvoker, Set<String> requiredInputStreams, Set<String> outputStreams, String stateStream, Duration timeout, BiFunction<String, CloudEvent<?, ?>, Future<Void>> eventSender) {
        this.vertx = vertx;
        this.outputStreams = outputStreams;
        this.requiredInputStreams = requiredInputStreams;
        this.stateStream = stateStream;
        this.timeout = timeout;
        this.functionInvoker = functionInvoker;
        this.waitingMessages = new HashMap<>();
        this.runningAggregations = new ArrayList<>();
        this.eventSender = eventSender;
    }

    public void onEvent(AggregatorEvent event) {
        event.onNewMessage(message -> {
            enqueueMessage(message);
            triggerExecution();
            logActualState();
        });

        event.onExpiredMessage(message -> {
            //TODO implement expired strategy
            removeMessage(message);
            logActualState();
        });

        event.onFunctionInvocationStart(runningAggregation -> {
            //TODO
        });

        event.onFunctionInvocationEnded((runningAggregation, stringMessageMap) -> {
            //TODO
        });

        event.onFunctionInvocationFailed((runningAggregation, e) -> {
            //TODO
        });

        event.onSendFailed((runningAggregation, e) -> {
            //TODO
        });

    }

    public void triggerExecution() {
        // Start new aggregation if there is at least one item in each stream event queue
        if (waitingMessages
            .entrySet()
            .stream()
            .filter(e -> e.getValue().size() >= 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet())
            .equals(requiredInputStreams)
        ) {
            // Dequeue messages
            Map<String, Message> aggregationInputMap = requiredInputStreams
                .stream()
                .map(stream -> new AbstractMap.SimpleImmutableEntry<>(stream, dequeueMessage(stream)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            RunningAggregation aggregation = new RunningAggregation(aggregationKey, aggregationInputMap);

            // Trigger aggregation start
            onEvent(AggregatorEvent.createFunctionInvocationStartEvent(aggregation));
        }
    }

    private void enqueueMessage(Message message) {
        HashMap<String, ArrayDeque<MessageQueueItem>> streamQueueMap = this.waitingMessages.computeIfAbsent(message.key(), buf -> new HashMap<>());
        ArrayDeque<MessageQueueItem> waitingMessageForStream = streamQueueMap.computeIfAbsent(message.stream(), str -> new ArrayDeque<>());
        long timerId = vertx.setTimer(this.timeout.toMillis(), h -> {
            if (h != null) {
                onEvent(AggregatorEvent.createExpiredMessageEvent(message));
            }
        });

        waitingMessageForStream.push(new MessageQueueItem(message, timerId));
    }

    private Message dequeueMessage(Buffer key, String stream) {
        HashMap<String, ArrayDeque<MessageQueueItem>> streamQueueMap = this.waitingMessages.get(key);
        if (streamQueueMap == null) {
            return null;
        }
        ArrayDeque<MessageQueueItem> waitingMessageForStream = streamQueueMap.get(stream);
        if (waitingMessageForStream == null) {
            return null;
        }
        MessageQueueItem item = waitingMessageForStream.remove();
        vertx.cancelTimer(item.timerId);
        return item.message;
    }

    private void removeMessage(Message message) {
        String stream = message.stream();
        HashMap<String, ArrayDeque<MessageQueueItem>> streamQueueMap = this.waitingMessages.get(message.key());
        if (streamQueueMap == null) {
            return;
        }
        ArrayDeque<MessageQueueItem> waitingMessageForStream = streamQueueMap.get(stream);
        if (waitingMessageForStream != null) {
            waitingMessageForStream.removeIf(q -> {
                if (q.message.equals(message)) {
                    vertx.cancelTimer(q.timerId);
                    return true;
                }
                return false;
            });
            if (waitingMessageForStream.isEmpty()) {
                // We don't need this queue anymore
                this.waitingMessages.remove(stream);
            }
        }
    }

    private Map<String, Integer> waitingMessagesCountForKey(Buffer key) {
        return Optional.ofNullable(
            waitingMessages.get(key)
        ).map(h -> h
            .entrySet()
            .stream()
            .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), e.getValue().size()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
        ).orElse(new HashMap<>());
    }

    private void logActualState() {
        if (logger.isDebugEnabled()) {
            logger.debug("Running aggregations: {}", runningAggregations.size());
            waitingMessages.keySet().forEach(k -> {
                logger.debug("Messages in queues for key {}: {}", k, waitingMessagesCountForKey(k));
            });
        }
    }

    private class MessageQueueItem {

        private Message message;
        private long timerId;

        public MessageQueueItem(Message message, long timerId) {
            this.message = message;
            this.timerId = timerId;
        }

        public Message getMessage() {
            return message;
        }

        public long getTimerId() {
            return timerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageQueueItem that = (MessageQueueItem) o;
            return Objects.equals(message, that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }
    }

}
