package com.slinkydeveloper.cloud.streaming.join

import com.slinkydeveloper.cloud.streaming.join.model.JoinSpec
import com.slinkydeveloper.cloud.streaming.join.model.WindowSpec
import io.vertx.core.Vertx
import io.vertx.kotlin.core.vertxOptionsOf
import java.time.Duration


fun main() {
    // Demo spec
    val spec = JoinSpec(
            left = System.getenv("LEFT_STREAM") ?: "left",
            right = System.getenv("RIGHT_STREAM") ?: "right",
            success = System.getenv("SUCCESS_STREAM") ?: "success",
            window = WindowSpec(
                    length = Duration.parse(
                            System.getenv("WINDOW_LENGTH") ?: "PT5M"
                    ),
                    hop = if (System.getenv("WINDOW_HOP") != null) Duration.parse(System.getenv("WINDOW_HOP")) else null
            )
    )

    val vertx: Vertx = Vertx.vertx(vertxOptionsOf(preferNativeTransport = true))
    vertx.deployVerticle(JoinFunctionInvokerVerticle())

    val stream = JoinKafkaStreamTopology(System.getenv("KAFKA_BROKERS")!!, spec, vertx).startTopology()

    Runtime.getRuntime().addShutdownHook(Thread {
        stream.close()
    })
}
