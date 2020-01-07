package com.slinkydeveloper.cloud.streaming.join

import com.slinkydeveloper.cloud.streaming.join.model.JoinSpec
import com.slinkydeveloper.cloud.streaming.join.model.WindowSpec
import io.vertx.core.Vertx
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.Json
import io.vertx.kotlin.core.json.jsonObjectOf
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.TimeWindows
import java.util.*
import java.util.Collections.singletonMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference


class JoinKafkaStreamTopology(bootstrapServers: String, private val joinSpec: JoinSpec, private val vertx: Vertx) {
    private val builder = StreamsBuilder()
    private val props = Properties()

    init {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "cloud-streaming-engine-runtime-poc-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/cloud-streaming-join");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    fun startTopology(): KafkaStreams {
        val outputStream = when(joinSpec.leftJoinLatest to joinSpec.rightJoinLatest) {
            true to true -> generateTablePerTableJoin()
            true to false -> generateTablePerStreamJoin()
            false to true -> generateStreamPerTableJoin()
            false to false -> generateStreamPerStreamJoin()
            else -> generateStreamPerStreamJoin()
        }

        outputStream.to(joinSpec.success)

        builder.build()

        val streams = KafkaStreams(
                builder.build(),
                props
        )

        streams.start()

        println("Started KafkaStreams Topology")

        return streams
    }

    fun generateTablePerTableJoin(): KStream<String, ByteArray?> {
        val left = builder.table<String, ByteArray>(joinSpec.left)
        val right = builder.table<String, ByteArray>(joinSpec.right)

        return left
                .join(right) { leftElement, rightElement -> joinInvoker(leftElement, rightElement) }
                .toStream()!!
    }

    fun generateTablePerStreamJoin(): KStream<String, ByteArray?> {
        val left = builder.table<String, ByteArray>(joinSpec.left)
        val right = builder.stream<String, ByteArray>(joinSpec.right)

        return right
                .join(left) { leftElement, rightElement -> joinInvoker(rightElement, leftElement) }!!
    }

    fun generateStreamPerTableJoin(): KStream<String, ByteArray?> {
        val left = builder.stream<String, ByteArray>(joinSpec.left)
        val right = builder.table<String, ByteArray>(joinSpec.right)

        return left
                .join(right) { leftElement, rightElement -> joinInvoker(leftElement, rightElement) }!!
    }

    fun generateStreamPerStreamJoin(): KStream<String, ByteArray?> {
        val left = builder.stream<String, ByteArray>(joinSpec.left)
        val right = builder.stream<String, ByteArray>(joinSpec.right)

        return left
                .join(
                        right,
                        { leftElement, rightElement -> joinInvoker(leftElement, rightElement) },
                        generateJoinWindow(joinSpec.window)
                )!!
    }

    fun joinInvoker(left: ByteArray?, right: ByteArray?): ByteArray? {
        println("Received new messages in join")

        return runBlocking {
            val res = vertx.eventBus().requestAwait<ByteArray?>(JoinFunctionInvokerVerticle.ADDRESS, jsonObjectOf(
                    "left" to left,
                    "right" to right
            ))

            println("Received reply in join: ${res.body()}")

            if (res.headers()[JoinFunctionInvokerVerticle.IS_ERROR_KEY]!!.toBoolean()) {
                null
            } else {
                res.body()
            }
        }
    }

    fun generateWindow(windowSpec: WindowSpec): TimeWindows {
        val window = TimeWindows.of(windowSpec.length)

        if (windowSpec.hop != null) {
            return window.advanceBy(windowSpec.hop)
        }

        return window
    }

    fun generateJoinWindow(windowSpec: WindowSpec): JoinWindows = JoinWindows.of(windowSpec.length)
}
