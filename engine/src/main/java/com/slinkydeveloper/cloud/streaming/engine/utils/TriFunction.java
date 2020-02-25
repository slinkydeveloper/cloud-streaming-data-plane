package com.slinkydeveloper.cloud.streaming.engine.utils;

@FunctionalInterface
public interface TriFunction<T0, T1, T2, R> {

    R accept(T0 t0, T1 t1, T2 t2);

}
