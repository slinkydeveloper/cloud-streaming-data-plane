package com.slinkydeveloper.cloud.streaming.join

import com.fasterxml.jackson.core.type.TypeReference
import io.cloudevents.json.Json
import io.cloudevents.v1.CloudEventImpl
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.core.net.SocketAddress
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.ext.web.client.sendBufferAwait
import kotlinx.coroutines.launch

class JoinFunctionInvokerVerticle: CoroutineVerticle() {

    lateinit var client: WebClient

    companion object {
        const val ADDRESS = "join-function-invoker.cloud-streaming-join"
        val UDS_FUNCTION_ADDRESS = SocketAddress.domainSocketAddress("/data/function")
        const val IS_ERROR_KEY = "is_error"
    }

    override suspend fun start() {
        client = WebClient.create(vertx, WebClientOptions().setFollowRedirects(true))

        vertx.eventBus()
                .consumer<JsonObject>(ADDRESS)
                .handler{ message ->
                    println("Received new message in JoinFunctionInvokerVerticle")
                    launch(this.coroutineContext) {
                        val leftEvent: ByteArray = message.body().getBinary("left")
                        val rightEvent: ByteArray = message.body().getBinary("right")
                        val (isErrorOut, eventOut) = invokeFunction(leftEvent, rightEvent)
                        println("Result of JoinFunctionInvokerVerticle: ${eventOut.toString()}, is error: $isErrorOut")
                        message.reply(eventOut, DeliveryOptions().addHeader(IS_ERROR_KEY, isErrorOut.toString()))
                    }
                }

        println("Started JoinFunctionInvokerVerticle")
    }

    suspend fun invokeFunction(leftEvent: ByteArray, rightEvent: ByteArray): Pair<Boolean, ByteArray?> {
        // Awful shit nobody should do in real life :)
        val buf = Buffer.buffer("{\"left\":".length + leftEvent.size + ",\"right\":".length + rightEvent.size + "}".length)
        buf.appendString("{\"left\":")
        buf.appendBytes(leftEvent)
        buf.appendString(",\"right\":")
        buf.appendBytes(rightEvent)
        buf.appendString("}")

        val response = client
                .request(HttpMethod.POST, UDS_FUNCTION_ADDRESS, "/")
                .putHeader("content-type", "application/json")
                .sendBufferAwait(buf)

        return if (response.statusCode() >= 400) {
            true to null
        } else {
            false to response.body().bytes
        }
    }
}
