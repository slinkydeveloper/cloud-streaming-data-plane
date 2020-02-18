package com.slinkydeveloper.cloud.streaming.engine.messaging;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

/**
 * SPI to implement to start message receiving
 */
public interface MessageReceiver {

    Future<Void> start(Handler<AsyncResult<Message>> handler);

    Future<Void> close();

}

