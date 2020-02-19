package com.slinkydeveloper.cloud.streaming.engine.messaging;

import io.cloudevents.CloudEvent;

import java.time.ZonedDateTime;

public interface Message {

    String stream();

    String key();

    long partition();

    ZonedDateTime timestamp();

    CloudEvent<?, ?> toEvent();

}
