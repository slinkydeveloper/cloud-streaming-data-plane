package com.slinkydeveloper.cloud.streaming.demo

import io.cloudevents.json.Json
import io.cloudevents.v1.CloudEventBuilder
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.net.SocketAddress
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import java.net.URI
import java.time.ZonedDateTime

class JoinFunctionVerticle: CoroutineVerticle() {

    override suspend fun start() {
        val router = Router.router(vertx)

        router.post().handler(BodyHandler.create()).handler{ routingContext ->
            println("Received new request in join function")

            val jo = routingContext.bodyAsJson

            val leftEvent: JsonObject = jo["inbound-a"]
            val rightEvent: JsonObject = jo["inbound-b"]

            val leftNumber = leftEvent.getString("data").toDouble()
            val rightNumber = rightEvent.getString("data").toDouble()
            val sum = leftNumber + rightNumber

            val resultEvent = CloudEventBuilder.builder<String>()
                    .withId(leftEvent["id"])
                    .withSubject("sub")
                    .withType("sum.demo")
                    .withTime(ZonedDateTime.now())
                    .withDataContentType("text/plain")
                    .withData(sum.toString())
                    .withSource(URI.create("http://my-cool-app.com"))
                    .build()

            val resultStream = if (sum >= 0) "positive" else "negative"

            routingContext
                    .response()
                    .setStatusCode(200)
                    .putHeader("content-type", "application/json")
                    .end(
                            jsonObjectOf(
                                    resultStream to JsonObject(Json.encode(resultEvent))
                            ).toBuffer()
                    )
        }

        router.errorHandler(500) {
            println("${it.failure()}")

            it.failure().printStackTrace()
            it.response().setStatusCode(500).putHeader("content-type", "text/plain").end(it.failure().message)
        }

        vertx.createHttpServer()
                .requestHandler(router)
                .listenAwait(SocketAddress.domainSocketAddress("/data/function"))

        println("Started demo join function")
    }

}
