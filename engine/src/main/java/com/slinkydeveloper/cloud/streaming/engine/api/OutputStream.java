package com.slinkydeveloper.cloud.streaming.engine.api;

import java.util.Objects;

public class OutputStream {

    private String name;
    private String metadataAsKey;

    public OutputStream(String name, String metadataAsKey) {
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
        OutputStream that = (OutputStream) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(metadataAsKey, that.metadataAsKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, metadataAsKey);
    }
}
