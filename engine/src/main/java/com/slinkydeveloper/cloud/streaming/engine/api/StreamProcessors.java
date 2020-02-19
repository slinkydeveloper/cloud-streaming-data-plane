package com.slinkydeveloper.cloud.streaming.engine.api;

import java.time.Duration;
import java.util.Set;

public class StreamProcessors {

    public static StreamProcessor createTupleJoiner(String topic1, String topic2, String successTopic, Duration timeout, TimeoutStrategy timeoutStrategy) {
        return new StreamProcessor()
            .setInputStreams(Set.of(new InputStream(topic1, null), new InputStream(topic2, null)))
            .setOutputStreams(Set.of(new OutputStream(successTopic, null)))
                .setTimeout(timeout)
                .setTimeoutStrategy(timeoutStrategy);
    }

    public static StreamProcessor createFold(String input, String output) {
        return new StreamProcessor()
            .setInputStreams(Set.of(new InputStream(input, null)))
            .setStateStream(output);
    }

    public static StreamProcessor createStatefulMapper(String input, String output) {
        String stateTopic = "state-" + input + "-" + output;
        return new StreamProcessor()
            .setInputStreams(Set.of(new InputStream(input, null)))
            .setOutputStreams(Set.of(new OutputStream(output, null)))
            .setStateStream(stateTopic);
    }

}
