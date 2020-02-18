package com.slinkydeveloper.cloud.streaming.engine.messaging;

public interface MessageReceiverFactory {

    MessageReceiver create(String stream);

}
