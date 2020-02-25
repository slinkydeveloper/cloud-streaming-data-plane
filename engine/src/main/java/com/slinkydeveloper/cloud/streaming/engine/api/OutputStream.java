package com.slinkydeveloper.cloud.streaming.engine.api;

import java.util.Objects;

public class OutputStream {

    private String name;
    private String metadataAsKey;
    private String functionReturnName;

    public OutputStream(String name) {
        this(name, null, null);
    }

    public OutputStream(String name, String functionReturnName, String metadataAsKey) {
        this.name = name;
        this.metadataAsKey = metadataAsKey;
        if (functionReturnName != null) {
            this.functionReturnName = functionReturnName;
        } else {
            this.functionReturnName = name;
        }
    }

    public String getName() {
        return name;
    }

    public String getMetadataAsKey() {
        return metadataAsKey;
    }

    public String getFunctionReturnName() {
        return functionReturnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputStream that = (OutputStream) o;
        return Objects.equals(name, that.name) &&
            Objects.equals(metadataAsKey, that.metadataAsKey) &&
            Objects.equals(functionReturnName, that.functionReturnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, metadataAsKey, functionReturnName);
    }
}
