package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.Reference;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class EventBusRegistrationTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = EventBus.create();
    }

    @Test
    void register_whenAnnotated_tracksSubscriberAndReturnsBus() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber);

        assertSame(eventBus, result);
        assertEquals(1, eventBus.allSubscribers().size());
    }

    @Test
    void register_whenIdProvided_tracksSubscriberInChannel() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber, "testEvent");

        assertSame(eventBus, result);
        assertEquals(1, eventBus.subscribers("testEvent", String.class).size());
    }

    @Test
    void register_whenModeProvided_tracksSubscriberAndReturnsBus() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber, ThreadMode.DEFAULT);

        assertSame(eventBus, result);
        assertEquals(1, eventBus.allSubscribers().size());
    }

    @Test
    void register_whenIdAndModeProvided_tracksSubscriberInChannel() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber, "testEvent", ThreadMode.DEFAULT);

        assertSame(eventBus, result);
        assertEquals(1, eventBus.subscribers("testEvent", String.class).size());
    }

    @Test
    void register_whenLambdaHasId_deliversMatchingEvent() {
        AtomicReference<String> received = new AtomicReference<>();
        Subscriber<String> subscriber = event -> received.set(event);

        eventBus.register(subscriber, "lambdaEvent");
        eventBus.post("lambdaEvent", "Hello Lambda");

        assertEquals("Hello Lambda", received.get());
    }

    @Test
    void register_whenLambdaHasExplicitDefaultMode_deliversMatchingEvent() {
        AtomicReference<String> received = new AtomicReference<>();
        Subscriber<String> subscriber = event -> received.set(event);

        eventBus.register(subscriber, "lambdaEvent", ThreadMode.DEFAULT);
        eventBus.post("lambdaEvent", "Hello Lambda");

        assertEquals("Hello Lambda", received.get());
    }

    @Test
    void register_whenSameInstanceIsRegisteredAgain_replacesPreviousChannel() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber, "id1");
        eventBus.register(subscriber, "id2");

        // After re-registration, subscriber should be under id2 now
        assertEquals(1, eventBus.allSubscribers().size());

        eventBus.post("id1", "msg1");
        eventBus.post("id2", "msg2");

        // Should only receive msg2 since re-registered under id2
        assertEquals(List.of("msg2"), subscriber.receivedEvents);

        eventBus.unregister(subscriber);
    }

    @Test
    void register_whenSubscribersAreEqual_tracksEachInstanceSeparately() {
        EqualSubscriber first = new EqualSubscriber("same");
        EqualSubscriber second = new EqualSubscriber("same");

        eventBus.register(first);
        eventBus.register(second);
        eventBus.post("event");

        assertEquals(List.of("event"), first.events);
        assertEquals(List.of("event"), second.events);
        assertEquals(2, eventBus.allSubscribers().size());

        eventBus.unregister(first);
        eventBus.post("event2");

        assertEquals(List.of("event"), first.events);
        assertEquals(List.of("event", "event2"), second.events);
    }

    @Test
    void register_whenNamedObjectSubscriberHasNoId_rejectsUntilIdProvided() {
        final NamedObjectSubscriber subscriber = new NamedObjectSubscriber();

        assertThrows(IllegalStateException.class, () -> eventBus.register(subscriber));

        eventBus.register(subscriber, "named-object");
        eventBus.post("named-object", "hello");
        eventBus.post("hello-without-id");

        assertEquals(List.of("hello"), subscriber.received);
        eventBus.unregister(subscriber);
    }

    @Test
    void register_whenNoSubscriberMethodsExist_rejectsSubscriber() {
        Object noMethodSubscriber = new Object();

        assertThrows(IllegalArgumentException.class, () -> eventBus.register(noMethodSubscriber));
    }

    @Test
    void register_whenAnnotatedMethodIsStatic_rejectsSubscriber() {
        assertThrows(RuntimeException.class, () -> eventBus.register(new StaticAnnotatedSubscriber()));
    }

    @Test
    void register_whenSubscriberIsNull_rejectsArgument() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.register(null));
    }

    @Test
    void register_whenAnnotatedMethodIsPrivateAndStatic_rejectsStaticMethod() {
        final RuntimeException e = assertThrows(RuntimeException.class, () -> eventBus.register(new PrivateStaticAnnotatedSubscriber()));
        assertTrue(e.getMessage().contains("must not be static"), e.getMessage());
    }

    @Test
    void register_whenOnlyAnnotatedMethodIsPrivate_reportsNoSubscriberMethod() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> eventBus.register(new PrivateOnlyAnnotatedSubscriber()));
        assertTrue(e.getMessage().startsWith("No subscriber method found"), e.getMessage());
    }

    @Test
    void register_whenAnnotatedArityIsInvalid_leavesRegistryEmpty() {
        EventBus eventBus = EventBus.create();
        class NoParameters {
            @Subscribe
            public void onEvent() {
            }
        }
        class TwoParameters {
            @Subscribe
            public void onEvent(String first, String second) {
            }
        }

        RuntimeException noParameters = assertThrows(RuntimeException.class, () -> eventBus.register(new NoParameters()));
        assertTrue(noParameters.getMessage().contains("0 parameters"), noParameters.getMessage());
        RuntimeException twoParameters = assertThrows(RuntimeException.class, () -> eventBus.register(new TwoParameters()));
        assertTrue(twoParameters.getMessage().contains("2 parameters"), twoParameters.getMessage());
        assertEquals(0, eventBus.countOfSubscribers(), "Invalid methods must not leave partial registrations");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void register_whenLambdaEventIdIsMissing_rejectsRegistration(String eventId) {
        Subscriber<String> subscriber = event -> {
        };

        assertThrows(IllegalStateException.class, () -> eventBus.register(subscriber, eventId));
        assertThrows(IllegalStateException.class, () -> eventBus.register(subscriber, eventId, ThreadMode.DEFAULT));
        assertEquals(0, eventBus.countOfSubscribers());
    }

    @Test
    void register_whenLambdaUsesUnqualifiedOverload_requiresEventId() {
        List<String> received = new ArrayList<>();
        Subscriber<String> subscriber = received::add;

        assertThrows(IllegalStateException.class, () -> eventBus.register(subscriber));
        eventBus.register(subscriber, "required");
        eventBus.post("required", "value");
        assertEquals(List.of("value"), received);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void register_whenIdOverrideIsNullOrEmpty_preservesAnnotationId(String eventId) {
        List<String> received = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe(eventId = "annotated")
            public void onEvent(String event) {
                received.add(event);
            }
        };
        eventBus.register(subscriber, eventId);
        eventBus.post("annotated", "matching").post("unqualified");

        assertEquals(List.of("matching"), received);
    }

    @Test
    void register_whenIdOverrideIsPresent_replacesAnnotationId() {
        List<String> received = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe(eventId = "annotated")
            public void onEvent(String event) {
                received.add(event);
            }
        };
        eventBus.register(subscriber, "override");
        eventBus.post("annotated", "ignored").post("override", "matching");

        assertEquals(List.of("matching"), received);
        assertTrue(eventBus.subscribers("annotated", String.class).isEmpty());
        assertEquals(List.of(subscriber), eventBus.subscribers("override", String.class));
    }

    @Test
    void register_whenSubscriberIsAnonymous_deliversMatchingEvent() {
        AtomicReference<String> receivedEvent = new AtomicReference<>();

        Subscriber<String> subscriber = new Subscriber<>() {
            @Override
            public void on(String event) {
                receivedEvent.set(event);
            }
        };

        eventBus.register(subscriber, "testEvent");
        eventBus.post("testEvent", "Hello World");

        assertEquals("Hello World", receivedEvent.get());
    }

    @Test
    void register_whenSubscriberIsMethodReference_deliversMatchingEvent() {
        List<String> events = new ArrayList<>();

        Subscriber<String> subscriber = events::add;

        eventBus.register(subscriber, "methodRef");
        eventBus.post("methodRef", "Method Reference Test");

        assertEquals(List.of("Method Reference Test"), events);
    }

    @Test
    void unregister_whenAnnotatedSubscriberIsRemoved_stopsFutureDelivery() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        eventBus.post("before");
        assertEquals(1, subscriber.receivedEvents.size());

        eventBus.unregister(subscriber);
        eventBus.post("after");

        // Should still have only the "before" event
        assertEquals(List.of("before"), subscriber.receivedEvents);
    }

    @Test
    void unregister_whenSubscriberIsUnknown_returnsBus() {
        TestSubscriber subscriber = new TestSubscriber();
        // Unregistering a subscriber that was never registered should not throw
        EventBus result = eventBus.unregister(subscriber);
        assertSame(eventBus, result);
    }

    @Test
    void unregister_whenCalledTwice_remainsIdempotent() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        EventBus result1 = eventBus.unregister(subscriber);
        assertSame(eventBus, result1);

        // Second unregister should not throw
        EventBus result2 = eventBus.unregister(subscriber);
        assertSame(eventBus, result2);

        assertTrue(eventBus.allSubscribers().isEmpty());
    }

    @Test
    void unregister_whenLambdaIsRemoved_stopsFutureDelivery() {
        AtomicReference<String> receivedEvent = new AtomicReference<>();

        Subscriber<String> subscriber = receivedEvent::set;

        eventBus.register(subscriber, "unregTest");
        eventBus.post("unregTest", "First Event");
        assertEquals("First Event", receivedEvent.get());

        eventBus.unregister(subscriber);
        receivedEvent.set(null);

        eventBus.post("unregTest", "Second Event");
        assertNull(receivedEvent.get());
    }

    static final class EqualSubscriber {
        final String id;
        final List<String> events = new ArrayList<>();

        EqualSubscriber(String id) {
            this.id = id;
        }

        @Subscribe
        public void onEvent(String event) {
            events.add(event);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof EqualSubscriber other && id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    public static class TestSubscriber {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class StaticAnnotatedSubscriber {
        @Subscribe
        public static void onEvent(String event) {
            // static method - should be rejected
        }
    }

    public static class NamedObjectSubscriber implements Subscriber<Object> {
        final List<Object> received = new ArrayList<>();

        @Override
        public void on(Object event) {
            received.add(event);
        }
    }

    public static class PrivateStaticAnnotatedSubscriber {
        @Subscribe
        private static void onEvent(String event) {
        }
    }

    public static class PrivateOnlyAnnotatedSubscriber {
        @Subscribe
        private void onEvent(String event) {
        }
    }
}
