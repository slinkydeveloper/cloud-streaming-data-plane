package com.slinkydeveloper.cloud.streaming.engine.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.slinkydeveloper.cloud.streaming.engine.messaging.Message;
import io.cloudevents.CloudEvent;
import io.cloudevents.json.Json;
import io.cloudevents.v1.CloudEventImpl;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaMessage implements Message {

    private KafkaConsumerRecord<String, byte[]> record;
    private Map<String, String> headersMap;

    public KafkaMessage(KafkaConsumerRecord<String, byte[]> record) {
        this.record = record;
        this.headersMap = record
            .headers()
            .stream()
            .collect(Collectors.toMap(
                KafkaHeader::key,
                kh -> kh.value().toString()
            ));
    }

    @Override
    public String stream() {
        return record.topic();
    }

    @Override
    public String key() {
        return record.key();
    }

    @Override
    public long partition() {
        return record.partition();
    }

    @Override
    public ZonedDateTime timestamp() {
        return new java.util.Date(record.timestamp()).toInstant().atZone(ZoneId.of("UTC"));
    }

    @Override
    public CloudEvent<?, ?> toEvent() {
        //TODO for now we support just structured
        try {
            return Json.MAPPER.readValue(record.value(), new TypeReference<CloudEventImpl>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}
