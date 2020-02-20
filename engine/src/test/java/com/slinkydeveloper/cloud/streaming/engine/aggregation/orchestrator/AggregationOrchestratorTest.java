package com.slinkydeveloper.cloud.streaming.engine.aggregation.orchestrator;

import com.slinkydeveloper.cloud.streaming.engine.aggregation.event.AggregatorEvent;
import com.slinkydeveloper.cloud.streaming.engine.api.InputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.OutputStream;
import com.slinkydeveloper.cloud.streaming.engine.api.StateStream;
import com.slinkydeveloper.cloud.streaming.engine.function.FunctionInvoker;
import com.slinkydeveloper.cloud.streaming.engine.messaging.MockMessage;
import io.cloudevents.CloudEvent;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.extensions.InMemoryFormat;
import io.cloudevents.v1.CloudEventBuilder;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ExtendWith(VertxExtension.class)
@Timeout(value = 2, timeUnit = TimeUnit.SECONDS)
public class AggregationOrchestratorTest {

    private CloudEvent event1 = CloudEventBuilder
        .builder()
        .withId("001")
        .withType("ping.application")
        .withSource(URI.create("http://localhost"))
        .withSubject("pippo")
        .withTime(ZonedDateTime.now())
        .build();
    private CloudEvent event2 = CloudEventBuilder
        .builder()
        .withId("002")
        .withType("pong.application")
        .withSource(URI.create("http://localhost"))
        .withSubject("pluto")
        .withTime(ZonedDateTime.now())
        .build();
    private CloudEvent state1 = CloudEventBuilder
        .builder()
        .withId("011")
        .withType("state.application")
        .withSource(URI.create("http://localhost"))
        .withSubject("pluto")
        .withTime(ZonedDateTime.now())
        .build();
    private CloudEvent state2 = CloudEventBuilder
        .builder()
        .withId("012")
        .withType("state.application")
        .withSource(URI.create("http://localhost"))
        .withSubject("pluto")
        .withTime(ZonedDateTime.now())
        .build();

    @Test
    public void singleInputSingleOutputWithMapping(Vertx vertx, VertxTestContext testContext) {
        Checkpoint functionInvoked = testContext.checkpoint();
        Checkpoint outputProduced = testContext.checkpoint();

        String key = "aaa";

        FunctionInvoker mockFunctionInvoker = in -> {
            testContext.verify(() -> {
                assertThat(in).containsOnlyKeys("input");
                assertThat(in.get("input")).isEqualTo(event1);
            });
            functionInvoked.flag();
            return Future.succeededFuture(Collections.singletonMap("output", in.get("input")));
        };

        AggregationOrchestrator orchestrator = new AggregationOrchestrator(
            vertx,
            mockFunctionInvoker,
            Set.of(new InputStream("stream1", "input", null)),
            Set.of(new OutputStream("streamOutput", "output", null)),
            null,
            null,
            (stream, k, event) -> {
                testContext.verify(() -> {
                    assertThat(stream).isEqualTo("streamOutput");
                    assertThat(event).isEqualTo(event1);
                    assertThat(k).isEqualTo(key);
                });
                outputProduced.flag();
                return Future.succeededFuture();
            }
        );

        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream1", key, 0, ZonedDateTime.now(), event1)));
    }

    @Test
    public void singleInputSingleOutputWithMetadataAsKey(Vertx vertx, VertxTestContext testContext) {
        Checkpoint functionInvoked = testContext.checkpoint();
        Checkpoint outputProduced = testContext.checkpoint();

        CloudEvent event = CloudEventBuilder
            .builder()
            .withId("bbb")
            .withType("ping.application")
            .withSource(URI.create("http://localhost"))
            .withSubject("pippo")
            .withExtension(ExtensionFormat.of(InMemoryFormat.of("ext1", "ccc", String.class)))
            .withTime(ZonedDateTime.now())
            .build();

        FunctionInvoker mockFunctionInvoker = in -> {
            testContext.verify(() -> {
                assertThat(in).containsOnlyKeys("input");
                assertThat(in.get("input")).isEqualTo(event);
            });
            functionInvoked.flag();
            return Future.succeededFuture(Collections.singletonMap("output", in.get("input")));
        };

        AggregationOrchestrator orchestrator = new AggregationOrchestrator(
            vertx,
            mockFunctionInvoker,
            Set.of(new InputStream("stream1", "input", "id")),
            Set.of(new OutputStream("streamOutput", "output", "ext1")),
            null,
            null,
            (stream, k, e) -> {
                testContext.verify(() -> {
                    assertThat(stream).isEqualTo("streamOutput");
                    assertThat(k).isEqualTo("ccc");
                    assertThat(e).isEqualTo(event);
                });
                outputProduced.flag();
                return Future.succeededFuture();
            }
        );

        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream1", "aaa", 0, ZonedDateTime.now(), event)));
    }

    @Test
    public void singleInputNoOutput(Vertx vertx, VertxTestContext testContext) {
        Checkpoint functionInvoked = testContext.checkpoint();

        String key = "aaa";

        FunctionInvoker mockFunctionInvoker = in -> {
            testContext.verify(() -> {
                assertThat(in).containsOnlyKeys("stream1");
                assertThat(in.get("stream1")).isEqualTo(event1);
            });
            functionInvoked.flag();
            return Future.succeededFuture(Collections.emptyMap());
        };

        AggregationOrchestrator orchestrator = new AggregationOrchestrator(
            vertx,
            mockFunctionInvoker,
            Set.of(new InputStream("stream1")),
            Set.of(),
            null,
            null,
            (stream, k, event) -> {
                testContext.failNow(new AssertionError("This should never be invoked"));
                return Future.succeededFuture();
            }
        );

        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream1", key, 0, ZonedDateTime.now(), event1)));
    }

    @Test
    public void multiInputSingleOutput(Vertx vertx, VertxTestContext testContext) {
        Checkpoint functionInvoked = testContext.checkpoint();
        Checkpoint outputProduced = testContext.checkpoint();

        String key = "aaa";

        FunctionInvoker mockFunctionInvoker = in -> {
            testContext.verify(() -> {
                assertThat(in).containsOnlyKeys("stream1", "stream2");
                assertThat(in.get("stream1")).isEqualTo(event1);
                assertThat(in.get("stream2")).isEqualTo(event2);
            });
            functionInvoked.flag();
            return Future.succeededFuture(Collections.singletonMap("output", in.get("stream1")));
        };

        AggregationOrchestrator orchestrator = new AggregationOrchestrator(
            vertx,
            mockFunctionInvoker,
            Set.of(new InputStream("stream1"), new InputStream("stream2")),
            Set.of(new OutputStream("output")),
            null,
            null,
            (stream, k, event) -> {
                testContext.verify(() -> {
                    assertThat(stream).isEqualTo("output");
                    assertThat(event).isEqualTo(event1);
                    assertThat(k).isEqualTo(key);
                });
                outputProduced.flag();
                return Future.succeededFuture();
            }
        );

        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream1", key, 0, ZonedDateTime.now(), event1)));
        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream2", key, 0, ZonedDateTime.now(), event2)));
    }

    @Test
    public void singleInputSingleOutputWithState(Vertx vertx, VertxTestContext testContext) {
        Checkpoint functionInvoked = testContext.checkpoint(2);
        Checkpoint outputProduced = testContext.checkpoint(4);

        String key = "aaa";

        AtomicInteger joinCount = new AtomicInteger();

        FunctionInvoker mockFunctionInvoker = in -> {
            int step = joinCount.incrementAndGet();
            if (step == 1) {
                testContext.verify(() -> {
                    assertThat(in).containsOnlyKeys("stream1");
                    assertThat(in.get("stream1")).isEqualTo(event1);
                });
                functionInvoked.flag();
                HashMap<String, CloudEvent> map = new HashMap<>();
                map.put("output", in.get("stream1"));
                map.put("state", state1);
                return Future.succeededFuture(map);
            } else if (step == 2) {
                testContext.verify(() -> {
                    assertThat(in).containsOnlyKeys("stream1", "state");
                    assertThat(in.get("stream1")).isEqualTo(event1);
                    assertThat(in.get("state")).isEqualTo(state1);
                });
                functionInvoked.flag();
                HashMap<String, CloudEvent> map = new HashMap<>();
                map.put("output", in.get("stream1"));
                map.put("state", state2);
                return Future.succeededFuture(map);
            } else {
                testContext.failNow(new AssertionError("FunctionInvoker invoked more than 2 times"));
                return Future.failedFuture("");
            }
        };

        AggregationOrchestrator orchestrator = new AggregationOrchestrator(
            vertx,
            mockFunctionInvoker,
            Set.of(new InputStream("stream1")),
            Set.of(new OutputStream("output")),
            new StateStream("state"),
            null,
            (stream, k, event) -> {
                testContext.verify(() -> {
                    assertThat(k).isEqualTo(key);
                    switch (stream) {
                        case "output":
                            assertThat(event).isEqualTo(event1);
                            break;
                        case "state":
                            if (joinCount.get() == 1) {
                                assertThat(event).isEqualTo(state1);
                            } else if (joinCount.get() == 2) {
                                assertThat(event).isEqualTo(state2);
                            }
                            break;
                        default:
                            fail("Unrecognized output stream: " + stream);
                    }
                });
                outputProduced.flag();
                return Future.succeededFuture();
            }
        );

        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream1", key, 0, ZonedDateTime.now(), event1)));
        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream1", key, 0, ZonedDateTime.now(), event1)));
        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream2", key, 0, ZonedDateTime.now(), event2)));
        orchestrator.onEvent(AggregatorEvent.createNewMessageEvent(new MockMessage("stream2", key, 0, ZonedDateTime.now(), event2)));
    }

}
