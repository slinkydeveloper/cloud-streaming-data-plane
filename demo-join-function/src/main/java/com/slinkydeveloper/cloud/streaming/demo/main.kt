package com.slinkydeveloper.cloud.streaming.demo

import io.vertx.core.Vertx
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.vertxOptionsOf

suspend fun main() {
    val vertx: Vertx = Vertx.vertx(vertxOptionsOf(preferNativeTransport = true))
    vertx.deployVerticleAwait(JoinFunctionVerticle())
}
