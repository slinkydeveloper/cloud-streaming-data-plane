package com.slinkydeveloper.cloud.streaming.engine.api;

import java.util.Objects;

public class InputStream {

    // Topic name
    String name;

    public InputStream(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InputStream that = (InputStream) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
