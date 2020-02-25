package com.slinkydeveloper.cloud.streaming.engine.utils;

@FunctionalInterface
public interface TriConsumer<T0, T1, T2> {

    void accept(T0 t0, T1 t1, T2 t2);

}
