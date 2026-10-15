package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class EventBusRegistrationTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = EventBus.create();
    }

    @ParameterizedTest(name = "overload={0}")
    @EnumSource(RegistrationOverload.class)
    void register_whenAnnotatedOverloadIsUsed_tracksSubscriberAndDeliversOnSelectedChannel(RegistrationOverload overload) {
        StringSubscriber subscriber = new StringSubscriber();

        assertSame(eventBus, overload.register(eventBus, subscriber));

        assertEquals(List.of(subscriber), eventBus.allSubscribers());
        assertEquals(List.of(subscriber), eventBus.subscribers(overload.eventId, String.class));
        eventBus.post(overload.eventId, "matching");
        eventBus.post("other", "ignored");
        assertEquals(List.of("matching"), subscriber.receivedEvents);
    }

    @ParameterizedTest(name = "callback={0}, explicitMode={1}")
    @MethodSource("callbackRegistrations")
    void register_whenCallbackFormIsSupported_deliversMatchingEvents(CallbackStyle style, boolean explicitMode) {
        List<String> received = new ArrayList<>();
        Subscriber<String> subscriber = style.create(received);

        EventBus result = explicitMode ? eventBus.register(subscriber, "callback", ThreadMode.DEFAULT) : eventBus.register(subscriber, "callback");

        assertSame(eventBus, result);
        assertEquals(List.of(subscriber), eventBus.subscribers("callback", String.class));
        eventBus.post("other", "ignored").post("callback", "first").post("callback", "second");
        assertEquals(List.of("first", "second"), received);
    }

    @ParameterizedTest(name = "eventId={0}")
    @NullAndEmptySource
    @ValueSource(strings = "override")
    void register_whenIdOverrideIsProvided_usesOverrideOrAnnotationDefault(String eventId) {
        List<String> received = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe(eventId = "annotated")
            public void onEvent(String event) {
                received.add(event);
            }
        };
        String selectedId = eventId == null || eventId.isEmpty() ? "annotated" : eventId;
        String otherId = selectedId.equals("annotated") ? "override" : "annotated";

        eventBus.register(subscriber, eventId);
        eventBus.post(selectedId, "matching").post(otherId, "ignored").post("unqualified");

        assertEquals(List.of("matching"), received);
        assertEquals(List.of(subscriber), eventBus.subscribers(selectedId, String.class));
        assertTrue(eventBus.subscribers(otherId, String.class).isEmpty());
    }

    @Test
    void register_whenSameInstanceIsRegisteredAgain_replacesPreviousChannel() {
        StringSubscriber subscriber = new StringSubscriber();
        eventBus.register(subscriber, "id1");
        eventBus.register(subscriber, "id2");

        assertEquals(1, eventBus.allSubscribers().size());

        eventBus.post("id1", "msg1");
        eventBus.post("id2", "msg2");

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
    void register_whenSubscriberIsNull_rejectsArgument() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.register(null));
    }

    @ParameterizedTest(name = "privateMethod={0}")
    @ValueSource(booleans = { false, true })
    void register_whenAnnotatedMethodIsStatic_rejectsRegardlessOfVisibility(boolean privateMethod) {
        Object subscriber = privateMethod ? new PrivateStaticAnnotatedSubscriber() : new StaticAnnotatedSubscriber();

        RuntimeException failure = assertThrows(RuntimeException.class, () -> eventBus.register(subscriber));

        assertTrue(failure.getMessage().contains("must not be static"), failure.getMessage());
        assertTrue(eventBus.allSubscribers().isEmpty());
    }

    @Test
    void register_whenOnlyAnnotatedMethodIsPrivate_reportsNoSubscriberMethod() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> eventBus.register(new PrivateOnlyAnnotatedSubscriber()));
        assertTrue(e.getMessage().startsWith("No subscriber method found"), e.getMessage());
    }

    @ParameterizedTest(name = "parameterCount={0}")
    @ValueSource(ints = { 0, 2 })
    void register_whenAnnotatedArityIsInvalid_leavesRegistryEmpty(int parameterCount) {
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
        Object subscriber = parameterCount == 0 ? new NoParameters() : new TwoParameters();

        RuntimeException failure = assertThrows(RuntimeException.class, () -> eventBus.register(subscriber));

        assertTrue(failure.getMessage().contains(parameterCount + " parameters"), failure.getMessage());
        assertTrue(eventBus.allSubscribers().isEmpty(), "Invalid methods must not leave partial registrations");
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

    @Test
    void register_whenReplacementIsInvalid_preservesExistingSubscription() {
        List<String> received = new ArrayList<>();
        Subscriber<String> subscriber = received::add;
        eventBus.register(subscriber, "existing");

        assertThrows(IllegalStateException.class, () -> eventBus.register(subscriber));

        assertEquals(List.of(subscriber), eventBus.allSubscribers());
        assertEquals(List.of(subscriber), eventBus.subscribers("existing", String.class));
        eventBus.post("existing", "still registered");
        assertEquals(List.of("still registered"), received);
    }

    @ParameterizedTest(name = "lambdaSubscriber={0}")
    @ValueSource(booleans = { false, true })
    void unregister_whenSubscriberIsRemoved_stopsDeliveryAndRemainsIdempotent(boolean lambdaSubscriber) {
        List<String> received = new ArrayList<>();
        Object subscriber = lambdaSubscriber ? (Subscriber<String>) received::add : new StringSubscriber(received);
        String eventId = lambdaSubscriber ? "callback" : null;
        eventBus.register(subscriber, eventId);
        eventBus.post(eventId, "before");
        assertEquals(List.of("before"), received);

        assertSame(eventBus, eventBus.unregister(subscriber));
        assertSame(eventBus, eventBus.unregister(subscriber));
        eventBus.post(eventId, "after");

        assertEquals(List.of("before"), received);
        assertTrue(eventBus.allSubscribers().isEmpty());
        assertTrue(eventBus.subscribers(eventId, String.class).isEmpty());
    }

    @Test
    void unregister_whenSubscriberIsUnknown_preservesRegisteredSubscribers() {
        StringSubscriber registered = new StringSubscriber();
        eventBus.register(registered);

        assertSame(eventBus, eventBus.unregister(new StringSubscriber()));

        assertEquals(List.of(registered), eventBus.allSubscribers());
        eventBus.post("still registered");
        assertEquals(List.of("still registered"), registered.receivedEvents);
    }

    enum RegistrationOverload {
        SUBSCRIBER(null), SUBSCRIBER_AND_ID("named"), SUBSCRIBER_AND_MODE(null), SUBSCRIBER_ID_AND_MODE("named");

        final String eventId;

        RegistrationOverload(String eventId) {
            this.eventId = eventId;
        }

        EventBus register(EventBus bus, Object subscriber) {
            return switch (this) {
                case SUBSCRIBER -> bus.register(subscriber);
                case SUBSCRIBER_AND_ID -> bus.register(subscriber, eventId);
                case SUBSCRIBER_AND_MODE -> bus.register(subscriber, ThreadMode.DEFAULT);
                case SUBSCRIBER_ID_AND_MODE -> bus.register(subscriber, eventId, ThreadMode.DEFAULT);
            };
        }
    }

    enum CallbackStyle {
        LAMBDA, METHOD_REFERENCE, ANONYMOUS;

        Subscriber<String> create(List<String> received) {
            return switch (this) {
                case LAMBDA -> event -> received.add(event);
                case METHOD_REFERENCE -> received::add;
                case ANONYMOUS -> new Subscriber<>() {
                    @Override
                    public void on(String event) {
                        received.add(event);
                    }
                };
            };
        }
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

    public static class StringSubscriber {
        final List<String> receivedEvents;

        StringSubscriber() {
            this(new ArrayList<>());
        }

        StringSubscriber(List<String> receivedEvents) {
            this.receivedEvents = receivedEvents;
        }

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class StaticAnnotatedSubscriber {
        @Subscribe
        public static void onEvent(String event) {
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

    private static Stream<Arguments> callbackRegistrations() {
        return Stream.of(CallbackStyle.values()).flatMap(style -> Stream.of(Arguments.of(style, false), Arguments.of(style, true)));
    }
}
