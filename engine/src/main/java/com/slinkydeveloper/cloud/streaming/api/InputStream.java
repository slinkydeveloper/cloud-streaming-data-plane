package com.slinkydeveloper.cloud.streaming.api;

import java.util.Objects;

public class InputStream {

    private String name;
    private String metadataAsKey;
    private String functionParameterName;

    public InputStream(String name) {
        this(name, null, null);
    }

    public InputStream(String name, String functionParameterName, String metadataAsKey) {
        this.name = name;
        this.metadataAsKey = metadataAsKey;
        if (functionParameterName != null) {
            this.functionParameterName = functionParameterName;
        } else {
            this.functionParameterName = name;
        }
    }

    public String getName() {
        return name;
    }

    public String getMetadataAsKey() {
        return metadataAsKey;
    }

    public String getFunctionParameterName() {
        return functionParameterName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputStream that = (InputStream) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(metadataAsKey, that.metadataAsKey) &&
            Objects.equals(functionParameterName, that.functionParameterName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, metadataAsKey, functionParameterName);
    }
}
