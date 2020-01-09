package com.slinkydeveloper.cloud.streaming.runtime.api;

import java.util.Objects;

public class OutputStream {

    // Topic name
    String name;

    public OutputStream(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutputStream that = (OutputStream) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
