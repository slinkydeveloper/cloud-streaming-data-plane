package com.slinkydeveloper.cloud.streaming.runtime.api;

import java.time.Duration;
import java.util.Set;

public class StreamProcessors {

    public static StreamProcessor createTupleJoiner(String topic1, String topic2, String successTopic, Duration timeout, TimeoutStrategy timeoutStrategy) {
        return new StreamProcessor()
                .setInputStreams(Set.of(new InputStream(topic1), new InputStream(topic2)))
                .setOutputStreams(Set.of(new OutputStream(successTopic)))
                .setTimeout(timeout)
                .setTimeoutStrategy(timeoutStrategy);
    }

    public static StreamProcessor createFold(String input, String output) {
        return new StreamProcessor()
                .setInputStreams(Set.of(new InputStream(input), new InputStream(output)))
                .setOutputStreams(Set.of(new OutputStream(output)));
    }

    public static StreamProcessor createStatefulMapper(String input, String output) {
        String stateTopic = "state-" + input + "-" + output;
        return new StreamProcessor()
            .setInputStreams(Set.of(new InputStream(input), new InputStream(stateTopic)))
            .setOutputStreams(Set.of(new OutputStream(output), new OutputStream(stateTopic)));
    }

}
