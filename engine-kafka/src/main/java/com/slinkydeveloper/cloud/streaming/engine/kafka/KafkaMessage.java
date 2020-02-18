package com.slinkydeveloper.cloud.streaming.engine.kafka;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.Message;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.time.ZonedDateTime;
import java.util.HashMap;

public class KafkaMessage implements Message {

    private KafkaConsumerRecord<byte[], byte[]> record;
    private HashMap<String, String> headersMap;

    @Override
    public byte[] getKey() {
        return record.key();
    }

    @Override
    public byte[] getValue() {
        return record.value();
    }

    @Override
    public long getPartition() {
        return record.partition();
    }

    @Override
    public String getHeader(String name) {
        return headersMap.get(name);
    }

    @Override
    public ZonedDateTime getExpirationTime() {
        return record.timestampType()
    }
}
