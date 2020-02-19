package com.slinkydeveloper.cloud.streaming.inbound.kafka

import com.fasterxml.jackson.core.type.TypeReference
import com.slinkydeveloper.cloud.streaming.inbound.model.InboundStreamSpec
import io.cloudevents.CloudEvent
import io.cloudevents.json.Json
import io.cloudevents.v1.AttributesImpl
import io.cloudevents.v1.CloudEventImpl
import io.cloudevents.v1.http.Unmarshallers
import io.vertx.core.http.HttpHeaders
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import java.util.*

class HttpInboundVerticle(val inboundSpec: InboundStreamSpec, val bootstrapServer: String): CoroutineVerticle() {

    private val BINARY_TYPE = HttpHeaders.createOptimized("application/json")
    private val STRUCTURED_TYPE = HttpHeaders.createOptimized("application/cloudevents+json")

    override suspend fun start() {
        val router = Router.router(vertx)

        val config = mapOf(
                "bootstrap.servers" to bootstrapServer,
                "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer" to "org.apache.kafka.common.serialization.ByteArraySerializer",
                "acks" to "1"
        )
        val producer = KafkaProducer.create<String, ByteArray>(vertx, config)

        router.post().handler(BodyHandler.create()).handler { routingContext ->
            println("Received new inbound event in HttpInboundVerticle")
            val event = parseEvent(routingContext)!!
            println("Parsed inbound event: $event")
            producer.send(buildProducerRecord(event))
            routingContext.response().setStatusCode(202).end()
        }

        router.errorHandler(500) {
            println("${it.failure()}")

            it.failure().printStackTrace()
            it.response().setStatusCode(500).putHeader("content-type", "text/plain").end(it.failure().message)
        }

        vertx.createHttpServer()
                .requestHandler(router)
                .listenAwait((System.getenv("PORT") ?: "8080").toInt())

        println("Started HttpInboundVerticle")
    }

    fun eventToJson(event: CloudEvent<AttributesImpl, Any>): JsonObject {
        return JsonObject(Json.encode(event))
    }

    fun parseEvent(routingContext: RoutingContext): CloudEvent<AttributesImpl, Any>? {
        val headers = routingContext.request().headers()

        // binary mode
        when {
            headers[HttpHeaders.CONTENT_TYPE].contains(BINARY_TYPE, ignoreCase = true) -> {
                println("Event is in binary mode")
                val buff = routingContext.body

                val event = Unmarshallers.binary(Object::class.java)
                        .withHeaders {
                            val result: MutableMap<String, Any> = HashMap()
                            headers.iterator()
                                    .forEachRemaining { header: Map.Entry<String, String> -> result[header.key] = header.value }
                            Collections.unmodifiableMap(result)
                        }
                        .withPayload { buff.toString() }
                        .unmarshal()

                return event as CloudEvent<AttributesImpl, Any>?
            }
            headers[HttpHeaders.CONTENT_TYPE].contains(STRUCTURED_TYPE, ignoreCase = true) -> { // structured read of the body
                println("Event is in structured mode")
                val buff = routingContext.body

                return Json.MAPPER.readValue(buff.bytes, object : TypeReference<CloudEventImpl<Any>?>() {})
            }
            else -> {
                throw IllegalArgumentException("no cloudevent type identified")
            }
        }
    }

    fun buildProducerRecord(event: CloudEvent<AttributesImpl, Any>): KafkaProducerRecord<String, ByteArray> {
        val key = event.attributes.id

        //TODO implement custom key

        if (inboundSpec.useCeTime && event.attributes.time.isPresent) {
            return KafkaProducerRecord
                    .create<String, ByteArray>(
                            inboundSpec.name,
                            key,
                            Json.binaryEncode(event),
                            event.attributes.time.get().toInstant().toEpochMilli(),
                            null
                    )
        }

        return KafkaProducerRecord
                .create<String, ByteArray>(
                        inboundSpec.name,
                        key,
                        Json.binaryEncode(event)
                )
    }
}
