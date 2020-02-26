package com.slinkydeveloper.cloud.streaming.api;

import java.time.Duration;
import java.util.Set;

// API for a "stream processor" block
public class StreamProcessor {

    // Input Streams must be co-partitioned
    // N elements from N streams are joined when [0].Key == [1].Key == ... == [N-1].Key
    private Set<InputStream> inputStreams;

    // Possible output streams
    private Set<OutputStream> outputStreams;

    // TODO The state stream is a special stream that bla bla bla
    private StateStream stateStream;

    // Timeout for an aggregation instance, aka timeout for a key to join with other elements
    // The timer for this timeout starts when a new aggregation begin and ends when all messages
    // required for the aggregation are received
    private Duration timeout;

    // What to do when the timeout happens?
    private TimeoutStrategy timeoutStrategy;

    // What to do when a failure happens in join function?
    private FailureStrategy failureStrategy;

    public StreamProcessor(Set<InputStream> inputStreams, Set<OutputStream> outputStreams, Duration timeout, TimeoutStrategy timeoutStrategy, StateStream stateStream) {
        this.inputStreams = inputStreams;
        this.outputStreams = outputStreams;
        this.timeout = timeout;
        this.timeoutStrategy = timeoutStrategy;
        this.stateStream = stateStream;
    }

    public StreamProcessor() {
    }

    public Set<InputStream> getInputStreams() {
        return inputStreams;
    }

    public StreamProcessor setInputStreams(Set<InputStream> inputStreams) {
        this.inputStreams = inputStreams;
        return this;
    }

    public Set<OutputStream> getOutputStreams() {
        return outputStreams;
    }

    public StreamProcessor setOutputStreams(Set<OutputStream> outputStreams) {
        this.outputStreams = outputStreams;
        return this;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public StreamProcessor setTimeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public TimeoutStrategy getTimeoutStrategy() {
        return timeoutStrategy;
    }

    public StreamProcessor setTimeoutStrategy(TimeoutStrategy timeoutStrategy) {
        this.timeoutStrategy = timeoutStrategy;
        return this;
    }

    public FailureStrategy getFailureStrategy() {
        return failureStrategy;
    }

    public StreamProcessor setFailureStrategy(FailureStrategy failureStrategy) {
        this.failureStrategy = failureStrategy;
        return this;
    }

    public StateStream getStateStream() {
        return stateStream;
    }

    public StreamProcessor setStateStream(StateStream stateStream) {
        this.stateStream = stateStream;
        return this;
    }
}
