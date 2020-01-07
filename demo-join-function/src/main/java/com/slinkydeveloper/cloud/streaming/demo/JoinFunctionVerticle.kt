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
import io.vertx.kotlin.coroutines.CoroutineVerticle
import java.net.URI
import java.time.ZonedDateTime

class JoinFunctionVerticle: CoroutineVerticle() {

    override suspend fun start() {
        val router = Router.router(vertx)

        router.post().handler(BodyHandler.create()).handler{ routingContext ->
            println("Received new request in join function")

            val jo = routingContext.bodyAsJson

            val leftEvent: JsonObject = jo["left"]
            val rightEvent: JsonObject = jo["right"]

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

            routingContext
                    .response()
                    .setStatusCode(200)
                    .putHeader("content-type", "application/cloudevents+json")
                    .end(Buffer.buffer(Json.binaryEncode(resultEvent)))
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
