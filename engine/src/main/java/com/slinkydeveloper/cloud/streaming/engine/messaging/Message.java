package com.slinkydeveloper.cloud.streaming.engine.messaging;

import io.cloudevents.CloudEvent;
import io.vertx.core.buffer.Buffer;

import java.time.ZonedDateTime;

public interface Message {

    String stream();

    Buffer key();

    long partition();

    ZonedDateTime timestamp();

    CloudEvent<?, ?> toEvent();

}
