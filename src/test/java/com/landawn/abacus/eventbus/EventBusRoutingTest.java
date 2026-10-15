package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
    void post_whenEventsAreSequential_preservesDeliveryAndReturnsBus() {
        StringSubscriber subscriber = new StringSubscriber();
        eventBus.register(subscriber);

        assertSame(eventBus, eventBus.post("first"));
        assertSame(eventBus, eventBus.post("second"));
        assertSame(eventBus, eventBus.post("third"));

        assertEquals(List.of("first", "second", "third"), subscriber.receivedEvents);
    }

    @Test
    void post_whenExplicitIdMatches_deliversOnlyToMatchingChannel() {
        StringSubscriber subscriber1 = new StringSubscriber();
        StringSubscriber subscriber2 = new StringSubscriber();

        eventBus.register(subscriber1, "event1");
        eventBus.register(subscriber2, "event2");

        eventBus.post("event1", "Message 1");

        assertEquals(List.of("Message 1"), subscriber1.receivedEvents);
        assertTrue(subscriber2.receivedEvents.isEmpty());
    }

    @Test
    void post_whenSubscriberRequiresId_doesNotDeliverUnqualifiedEvent() {
        AtomicReference<String> result = new AtomicReference<>();
        Subscriber<String> subscriber = event -> result.set(event);

        eventBus.register(subscriber, "testId");
        eventBus.post("hello");

        assertNull(result.get());
    }

    @Test
    void post_whenAnnotationSelectsId_ignoresOtherChannels() {
        List<String> received = new ArrayList<>();
        Object handler = new Object() {
            @Subscribe(eventId = "specific")
            public void handle(String event) {
                received.add(event);
            }
        };
        eventBus.register(handler);

        eventBus.post("specific", "matching");
        eventBus.post("other", "wrong channel");
        eventBus.post("unqualified");

        assertEquals(List.of("matching"), received);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void post_whenIdIsNullOrEmpty_usesUnqualifiedChannel(String eventId) {
        StringSubscriber subscriber = new StringSubscriber();
        eventBus.register(subscriber);

        assertSame(eventBus, eventBus.post(eventId, "message"));
        assertEquals(List.of("message"), subscriber.receivedEvents);
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

    @ParameterizedTest
    @MethodSource("payloads")
    void post_whenPayloadIsCustomOrParameterized_preservesObjectIdentity(Object payload) {
        AtomicReference<Object> received = new AtomicReference<>();
        Subscriber<Object> subscriber = received::set;
        eventBus.register(subscriber, "payload");

        eventBus.post("payload", payload);

        assertSame(payload, received.get());
    }

    @Test
    void post_whenEventIsSubtype_deliversToCompatibleHandlers() {
        HierarchySubscriber subscriber = new HierarchySubscriber();
        eventBus.register(subscriber);
        BaseEvent base = new BaseEvent();
        SubEvent child = new SubEvent();
        SubSubEvent grandchild = new SubSubEvent();

        eventBus.post(base).post(child).post(grandchild);

        assertEquals(List.of(base, child, grandchild), subscriber.baseEvents);
        assertEquals(List.of(child, grandchild), subscriber.subEvents);
        assertEquals(List.of(grandchild), subscriber.subSubEvents);
    }

    @Test
    void post_whenHandlerIsStrict_deliversOnlyExactType() {
        List<BaseEvent> compatible = new ArrayList<>();
        List<BaseEvent> strict = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe
            public void onCompatible(BaseEvent event) {
                compatible.add(event);
            }

            @Subscribe(strictEventType = true)
            public void onStrict(BaseEvent event) {
                strict.add(event);
            }
        };
        BaseEvent base = new BaseEvent();
        SubEvent child = new SubEvent();
        eventBus.register(subscriber);

        eventBus.post(base).post(child);

        assertEquals(List.of(base, child), compatible);
        assertEquals(List.of(base), strict);
    }

    @Test
    void post_whenStrictHandlerAcceptsInterface_ignoresImplementations() {
        List<CharSequence> compatible = new ArrayList<>();
        List<CharSequence> strict = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe
            public void onCompatible(CharSequence event) {
                compatible.add(event);
            }

            @Subscribe(strictEventType = true)
            public void onStrict(CharSequence event) {
                strict.add(event);
            }
        };
        String text = "text";
        StringBuilder builder = new StringBuilder("builder");
        eventBus.register(subscriber);

        eventBus.post(text).post(builder);

        assertEquals(List.of(text, builder), compatible);
        assertTrue(strict.isEmpty());
    }

    @Test
    void post_whenSubscriberHasMultipleMethods_selectsMatchingHandler() {
        MultiMethodSubscriber subscriber = new MultiMethodSubscriber();
        eventBus.register(subscriber);

        eventBus.post("text").post(123).post(45.67);

        assertEquals(List.of("text"), subscriber.strings);
        assertEquals(List.of(123), subscriber.integers);
        assertEquals(List.of(45.67), subscriber.doubles);
    }

    @ParameterizedTest(name = "subscriber={0}")
    @EnumSource(SubscriberKind.class)
    void post_whenSubscribersShareChannel_deliversToEachInstance(SubscriberKind kind) {
        List<String> first = new ArrayList<>();
        List<String> second = new ArrayList<>();
        eventBus.register(kind.create(first::add), kind.eventId);
        eventBus.register(kind.create(second::add), kind.eventId);

        eventBus.post(kind.eventId, "broadcast");

        assertEquals(List.of("broadcast"), first);
        assertEquals(List.of("broadcast"), second);
    }

    @ParameterizedTest(name = "subscriber={0}")
    @EnumSource(SubscriberKind.class)
    void post_whenSubscriberThrows_continuesHealthyDelivery(SubscriberKind kind) {
        List<String> received = new ArrayList<>();
        Object failing = kind.create(event -> {
            throw new IllegalStateException("expected subscriber failure");
        });
        eventBus.register(failing, kind.eventId);
        eventBus.register(kind.create(received::add), kind.eventId);

        assertDoesNotThrow(() -> eventBus.post(kind.eventId, "first").post(kind.eventId, "second"));

        assertEquals(List.of("first", "second"), received);
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

    private record CustomEvent(String value) {
    }

    enum SubscriberKind {
        ANNOTATED(null), LAMBDA("named");

        final String eventId;

        SubscriberKind(String eventId) {
            this.eventId = eventId;
        }

        Object create(Consumer<String> callback) {
            if (this == LAMBDA) {
                return (Subscriber<String>) callback::accept;
            }
            return new Object() {
                @Subscribe
                public void onEvent(String event) {
                    callback.accept(event);
                }
            };
        }
    }

    public static class StringSubscriber {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class MultiMethodSubscriber {
        final List<String> strings = new ArrayList<>();
        final List<Integer> integers = new ArrayList<>();
        final List<Double> doubles = new ArrayList<>();

        @Subscribe
        public void onString(String event) {
            strings.add(event);
        }

        @Subscribe
        public void onInteger(Integer event) {
            integers.add(event);
        }

        @Subscribe
        public void onDouble(Double event) {
            doubles.add(event);
        }
    }

    public static class BaseEvent {
    }

    public static class SubEvent extends BaseEvent {
    }

    public static class SubSubEvent extends SubEvent {
    }

    public static class HierarchySubscriber {
        final List<BaseEvent> baseEvents = new ArrayList<>();
        final List<SubEvent> subEvents = new ArrayList<>();
        final List<SubSubEvent> subSubEvents = new ArrayList<>();

        @Subscribe
        public void onBaseEvent(BaseEvent event) {
            baseEvents.add(event);
        }

        @Subscribe
        public void onSubEvent(SubEvent event) {
            subEvents.add(event);
        }

        @Subscribe
        public void onSubSubEvent(SubSubEvent event) {
            subSubEvents.add(event);
        }
    }

    private static Stream<Object> payloads() {
        return Stream.of(new CustomEvent("value"), List.of(new CustomEvent("first"), new CustomEvent("second")));
    }
}
