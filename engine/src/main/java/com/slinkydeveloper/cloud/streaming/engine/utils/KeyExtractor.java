package com.slinkydeveloper.cloud.streaming.engine.utils;

import io.cloudevents.CloudEvent;

public class KeyExtractor {

    public static String extractKey(CloudEvent event, String metadataAsKey, String oldKey) {
        if (metadataAsKey == null) {
            return oldKey;
        }

        switch (metadataAsKey) {
            case "id":
                return event.getAttributes().getId();
            case "type":
                return event.getAttributes().getType();
        }
        return event.getExtensions().get(metadataAsKey).toString();
    }

}
