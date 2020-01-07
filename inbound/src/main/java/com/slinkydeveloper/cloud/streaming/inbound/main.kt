package com.slinkydeveloper.cloud.streaming.inbound

import com.slinkydeveloper.cloud.streaming.inbound.model.InboundStreamSpec
import io.vertx.core.Vertx
import io.vertx.kotlin.core.deployVerticleAwait

suspend fun main() {
    // Demo spec
    val spec = InboundStreamSpec(
            name = System.getenv("NAME")!!,
            useMetaAsKey = System.getenv("KEY_METADATA"),
            useCeTime = (System.getenv("USE_CE_TIME") ?: "false").toBoolean()
    )

    val vertx: Vertx = Vertx.vertx()
    vertx.deployVerticleAwait(HttpInboundVerticle(spec, System.getenv("KAFKA_BROKERS")!!))
}
