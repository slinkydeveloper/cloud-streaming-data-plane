package com.slinkydeveloper.cloud.streaming.engine.messaging;

import io.vertx.core.Future;

/**
 * SPI to send messages to streams
 */
public interface MessageSender {

    Future<Void> send(Message message);

    Future<Void> close();

}
