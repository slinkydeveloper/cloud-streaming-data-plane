package com.slinkydeveloper.cloud.streaming.engine.api;

import java.util.Objects;

public class StateStream {

    private String name;
    private String metadataAsKey;

    public StateStream(String name) {
        this(name, null);
    }

    public StateStream(String name, String metadataAsKey) {
        this.name = name;
        this.metadataAsKey = metadataAsKey;
    }

    public String getName() {
        return name;
    }

    public String getMetadataAsKey() {
        return metadataAsKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StateStream that = (StateStream) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(metadataAsKey, that.metadataAsKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, metadataAsKey);
    }
}
