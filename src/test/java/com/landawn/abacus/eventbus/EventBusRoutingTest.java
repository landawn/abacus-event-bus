package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import com.landawn.abacus.TestBase;

@Tag("unit")
public class EventBusRoutingTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = EventBus.create();
    }

    @Test
    void post_whenAnnotatedSubscriberMatches_deliversEventAndReturnsBus() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        EventBus result = eventBus.post("Test Message");
        assertSame(eventBus, result);
        assertEquals(List.of("Test Message"), subscriber.receivedEvents);
    }

    @Test
    void post_whenExplicitIdMatches_deliversOnlyToMatchingChannel() {
        TestSubscriber subscriber1 = new TestSubscriber();
        TestSubscriber subscriber2 = new TestSubscriber();

        eventBus.register(subscriber1, "event1");
        eventBus.register(subscriber2, "event2");

        eventBus.post("event1", "Message 1");

        assertEquals(List.of("Message 1"), subscriber1.receivedEvents);
        assertEquals(0, subscriber2.receivedEvents.size());
    }

    @Test
    void post_whenSubscriberRequiresId_doesNotDeliverUnqualifiedEvent() {
        AtomicReference<String> result = new AtomicReference<>();
        Subscriber<String> subscriber = event -> result.set(event);

        eventBus.register(subscriber, "testId");
        eventBus.post("hello");

        assertNull(result.get()); // Should not receive without matching event ID
    }

    @Test
    void post_whenEventsAreSequential_preservesDeliverySequence() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        eventBus.post("first");
        eventBus.post("second");
        eventBus.post("third");

        assertEquals(List.of("first", "second", "third"), subscriber.receivedEvents);

        eventBus.unregister(subscriber);
    }

    @Test
    void post_whenAnnotationSelectsId_ignoresOtherChannels() {
        AtomicInteger count = new AtomicInteger();
        Object handler = new Object() {
            @Subscribe(eventId = "specific")
            public void handle(String event) {
                count.incrementAndGet();
            }
        };

        eventBus.register(handler);
        eventBus.post("specific", "match");
        eventBus.post("other", "no match");
        eventBus.post("match without ID");

        assertEquals(1, count.get());
        eventBus.unregister(handler);
    }

    @Test
    void post_whenEventIsSubtype_deliversToCompatibleHandlers() {
        HierarchySubscriber subscriber = new HierarchySubscriber();
        eventBus.register(subscriber);

        eventBus.post(new BaseEvent());
        eventBus.post(new SubEvent());
        eventBus.post(new SubSubEvent());

        assertEquals(3, subscriber.baseEventCount);
        assertEquals(2, subscriber.subEventCount);
        assertEquals(1, subscriber.subSubEventCount);
    }

    @Test
    void post_whenHandlerIsStrict_deliversOnlyExactType() {
        AtomicInteger baseEventCount = new AtomicInteger();
        AtomicInteger strictEventCount = new AtomicInteger();

        Object subscriber = new Object() {
            @Subscribe
            public void onBaseEvent(BaseEvent event) {
                baseEventCount.incrementAndGet();
            }

            @Subscribe(strictEventType = true)
            public void onStrictBaseEvent(BaseEvent event) {
                strictEventCount.incrementAndGet();
            }
        };

        eventBus.register(subscriber);
        eventBus.post(new BaseEvent());
        eventBus.post(new SubEvent());

        assertEquals(2, baseEventCount.get());
        assertEquals(1, strictEventCount.get());
    }

    @Test
    void post_whenAnnotatedSubscribersShareType_deliversToEachInstance() {
        TestSubscriber sub1 = new TestSubscriber();
        TestSubscriber sub2 = new TestSubscriber();

        eventBus.register(sub1);
        eventBus.register(sub2);
        eventBus.post("broadcast");

        assertEquals(List.of("broadcast"), sub1.receivedEvents);
        assertEquals(List.of("broadcast"), sub2.receivedEvents);

        eventBus.unregister(sub1);
        eventBus.unregister(sub2);
    }

    @Test
    void post_whenSubscriberHasMultipleMethods_selectsMatchingHandler() {
        MultiMethodSubscriber subscriber = new MultiMethodSubscriber();
        eventBus.register(subscriber);

        eventBus.post("String Event");
        eventBus.post(123);
        eventBus.post(45.67);

        assertEquals(1, subscriber.stringCount);
        assertEquals(1, subscriber.integerCount);
        assertEquals(1, subscriber.doubleCount);
    }

    @Test
    void post_whenAnnotatedSubscriberThrows_continuesHealthyDelivery() {
        List<String> received = new ArrayList<>();
        Object throwingSubscriber = new Object() {
            @Subscribe
            public void onEvent(String event) {
                throw new IllegalStateException("intentional");
            }
        };
        Object goodSubscriber = new Object() {
            @Subscribe
            public void onEvent(String event) {
                received.add(event);
            }
        };
        eventBus.register(throwingSubscriber);
        eventBus.register(goodSubscriber);

        assertDoesNotThrow(() -> eventBus.post("first").post("second"));
        assertEquals(List.of("first", "second"), received);
    }

    @Test
    void post_whenStrictHandlerAcceptsInterface_ignoresImplementations() {
        EventBus eventBus = EventBus.create();
        AtomicInteger normalCount = new AtomicInteger();
        AtomicInteger strictCount = new AtomicInteger();

        class TestClass {
            @Subscribe
            public void onNormalEvent(CharSequence event) {
                normalCount.incrementAndGet();
            }

            @Subscribe(strictEventType = true)
            public void onStrictEvent(CharSequence event) {
                strictCount.incrementAndGet();
            }
        }

        TestClass subscriber = new TestClass();
        eventBus.register(subscriber);

        eventBus.post("String implements CharSequence");
        eventBus.post(new StringBuilder("StringBuilder implements CharSequence"));

        assertEquals(2, normalCount.get());
        assertEquals(0, strictCount.get());
    }

    @Test
    void post_whenTypedLambdasUseDifferentIds_routesEachPayload() {
        AtomicReference<Integer> intReceived = new AtomicReference<>();
        AtomicReference<List<String>> listReceived = new AtomicReference<>();

        Subscriber<Integer> intSubscriber = intReceived::set;
        Subscriber<List<String>> listSubscriber = listReceived::set;

        eventBus.register(intSubscriber, "integers");
        eventBus.register(listSubscriber, "lists");

        eventBus.post("integers", 42);
        List<String> testList = List.of("a", "b", "c");
        eventBus.post("lists", testList);

        assertEquals(42, intReceived.get());
        assertEquals(testList, listReceived.get());
    }

    @Test
    void post_whenLambdaSubscribersShareId_deliversToEachInstance() {
        List<String> subscriber1Events = new ArrayList<>();
        List<String> subscriber2Events = new ArrayList<>();

        Subscriber<String> subscriber1 = subscriber1Events::add;
        Subscriber<String> subscriber2 = subscriber2Events::add;

        eventBus.register(subscriber1, "shared");
        eventBus.register(subscriber2, "shared");

        eventBus.post("shared", "Shared Event");

        assertEquals(1, subscriber1Events.size());
        assertEquals(1, subscriber2Events.size());
        assertEquals("Shared Event", subscriber1Events.get(0));
        assertEquals("Shared Event", subscriber2Events.get(0));
    }

    @Test
    void post_whenLambdaSubscriberThrows_continuesHealthyDelivery() {
        List<String> received = new ArrayList<>();
        Subscriber<String> failing = event -> {
            throw new IllegalStateException("expected subscriber failure");
        };
        Subscriber<String> healthy = received::add;
        eventBus.register(failing, "test").register(healthy, "test");

        assertDoesNotThrow(() -> eventBus.post("test", "first").post("test", "second"));
        assertEquals(List.of("first", "second"), received);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void post_whenIdIsNullOrEmpty_usesUnqualifiedChannel(String eventId) {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        assertSame(eventBus, eventBus.post(eventId, "message"));
        assertEquals(List.of("message"), subscriber.receivedEvents);
    }

    @Test
    void post_whenEventIsNull_rejectsEveryOverload() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.post((Object) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.post("id", null));
    }

    @Test
    void post_whenNoSubscriberMatches_returnsBus() {
        assertSame(eventBus, eventBus.post("orphan"));
        assertSame(eventBus, eventBus.post("unknown", "orphan"));
    }

    @ParameterizedTest
    @MethodSource("payloads")
    void post_whenPayloadIsCustomOrParameterized_preservesObjectIdentity(Object payload) {
        AtomicReference<Object> received = new AtomicReference<>();
        Subscriber<Object> subscriber = received::set;
        eventBus.register(subscriber, "payload");

        eventBus.post("payload", payload);

        assertSame(payload, received.get());
    }

    private static Stream<Object> payloads() {
        return Stream.of(new CustomEvent("value"), List.of(new CustomEvent("first"), new CustomEvent("second")));
    }

    private record CustomEvent(String value) {
    }

    public static class TestSubscriber {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class MultiMethodSubscriber {
        int stringCount;
        int integerCount;
        int doubleCount;

        @Subscribe
        public void onString(String event) {
            stringCount++;
        }

        @Subscribe
        public void onInteger(Integer event) {
            integerCount++;
        }

        @Subscribe
        public void onDouble(Double event) {
            doubleCount++;
        }
    }

    public static class BaseEvent {
    }

    public static class SubEvent extends BaseEvent {
    }

    public static class SubSubEvent extends SubEvent {
    }

    public static class HierarchySubscriber {
        int baseEventCount;
        int subEventCount;
        int subSubEventCount;

        @Subscribe
        public void onBaseEvent(BaseEvent event) {
            baseEventCount++;
        }

        @Subscribe
        public void onSubEvent(SubEvent event) {
            subEventCount++;
        }

        @Subscribe
        public void onSubSubEvent(SubSubEvent event) {
            subSubEventCount++;
        }
    }
}
