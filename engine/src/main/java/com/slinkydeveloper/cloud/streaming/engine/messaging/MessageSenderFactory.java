package com.slinkydeveloper.cloud.streaming.engine.messaging;

public interface MessageSenderFactory {

    MessageSender create(String stream);

}
