package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;

@Tag("unit")
public class EventBusTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = EventBus.create();
    }

    @Test
    void getDefault_whenCalledRepeatedly_returnsNamedSingleton() {
        EventBus defaultBus = EventBus.getDefault();
        assertNotNull(defaultBus);
        assertEquals("default", defaultBus.identifier());
        assertSame(defaultBus, EventBus.getDefault());
    }

    @Test
    void create_whenUnnamed_assignsNonEmptyIdentifier() {
        EventBus bus = EventBus.create();
        assertNotNull(bus);
        assertNotNull(bus.identifier());
        assertFalse(bus.identifier().isEmpty());
    }

    @Test
    void create_whenCalledTwice_assignsDistinctIdentifiers() {
        EventBus bus1 = EventBus.create();
        EventBus bus2 = EventBus.create();
        assertNotEquals(bus1.identifier(), bus2.identifier());
    }

    @Test
    void create_whenNamed_preservesIdentifier() {
        EventBus bus = EventBus.create("myIdentifier");
        assertEquals("myIdentifier", bus.identifier());
    }

    @Test
    void create_whenExecutorIsNull_rejectsArgument() {
        assertThrows(IllegalArgumentException.class, () -> EventBus.create("testBus", null));
    }

    @Test
    void subscribers_whenTypeMatches_returnsOnlyMatchingSubscribers() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        List<Object> subscribers = eventBus.subscribers(String.class);
        assertEquals(1, subscribers.size());
        assertTrue(subscribers.contains(subscriber));

        List<Object> noSubscribers = eventBus.subscribers(Integer.class);
        assertTrue(noSubscribers.isEmpty());

        eventBus.unregister(subscriber);
    }

    @Test
    void subscribers_whenLambdaHasMatchingId_returnsSubscriber() {
        Subscriber<String> subscriber = event -> {
        };
        eventBus.register(subscriber, "testId");

        List<Object> subscribers = eventBus.subscribers("testId", String.class);
        assertTrue(subscribers.contains(subscriber));

        eventBus.unregister(subscriber);
    }

    @Test
    void subscribers_whenTypeIsSubtype_includesCompatibleHandlers() {
        HierarchySubscriber subscriber = new HierarchySubscriber();
        eventBus.register(subscriber);

        // SubEvent extends BaseEvent, so subscriber should appear for SubEvent (because onBaseEvent accepts it)
        List<Object> baseSubs = eventBus.subscribers(BaseEvent.class);
        assertTrue(baseSubs.contains(subscriber));

        List<Object> subSubs = eventBus.subscribers(SubEvent.class);
        assertTrue(subSubs.contains(subscriber));

        eventBus.unregister(subscriber);
    }

    @Test
    void subscribers_whenIdsDiffer_keepsChannelsSeparate() {
        TestSubscriber subscriber1 = new TestSubscriber();
        TestSubscriber subscriber2 = new TestSubscriber();

        eventBus.register(subscriber1, "event1");
        eventBus.register(subscriber2, "event2");

        List<Object> subscribers = eventBus.subscribers("event1", String.class);
        assertEquals(1, subscribers.size());
        assertTrue(subscribers.contains(subscriber1));

        subscribers = eventBus.subscribers("event2", String.class);
        assertEquals(1, subscribers.size());
        assertTrue(subscribers.contains(subscriber2));
    }

    @Test
    void subscribers_whenIdIsUnknown_returnsEmptySnapshot() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber, "myId");

        List<Object> result = eventBus.subscribers("nonExistent", String.class);
        assertTrue(result.isEmpty());

        eventBus.unregister(subscriber);
    }

    @Test
    void subscribers_whenBusIsEmpty_returnsEmptySnapshot() {
        List<Object> subscribers = eventBus.subscribers(String.class);
        assertNotNull(subscribers);
        assertTrue(subscribers.isEmpty());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void subscribers_whenIdIsNullOrEmpty_usesUnqualifiedChannel(String eventId) {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        assertEquals(List.of(subscriber), eventBus.subscribers(eventId, String.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void subscribers_whenQueryResultOrRegistryChanges_keepsSnapshotsIndependent(boolean allSubscribers) {
        TestSubscriber first = new TestSubscriber();
        TestSubscriber second = new TestSubscriber();
        eventBus.register(first);
        List<Object> snapshot = allSubscribers ? eventBus.allSubscribers() : eventBus.subscribers(String.class);

        eventBus.register(second);
        assertEquals(List.of(first), snapshot, "Registration must not update an earlier snapshot");
        snapshot.clear();
        assertEquals(2, eventBus.countOfSubscribers(), "Changing a snapshot must not mutate the registry");
        List<Object> current = allSubscribers ? eventBus.allSubscribers() : eventBus.subscribers(String.class);
        assertHaveSameElements(List.of(first, second), current);
        eventBus.unregister(first).unregister(second);
        assertHaveSameElements(List.of(first, second), current);
        assertTrue(eventBus.allSubscribers().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void subscriberQueries_whenTypeIsNull_rejectRegardlessOfRegistryContents(boolean populated) {
        if (populated) {
            eventBus.register(new TestSubscriber());
        }

        assertThrows(IllegalArgumentException.class, () -> eventBus.subscribers((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.subscribers("id", null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.hasSubscribers(null));
    }

    @Test
    void allSubscribers_whenChannelsDiffer_includesEveryInstance() {
        Subscriber<String> subscriber1 = event -> {
        };
        Subscriber<Integer> subscriber2 = event -> {
        };

        eventBus.register(subscriber1, "id1");
        eventBus.register(subscriber2, "id2");

        List<Object> allSubscribers = eventBus.allSubscribers();
        assertEquals(2, allSubscribers.size());
        assertTrue(allSubscribers.contains(subscriber1));
        assertTrue(allSubscribers.contains(subscriber2));

        eventBus.unregister(subscriber1);
        eventBus.unregister(subscriber2);
    }

    @Test
    void allSubscribers_whenBusIsEmpty_returnsEmptySnapshot() {
        List<Object> all = eventBus.allSubscribers();
        assertNotNull(all);
        assertTrue(all.isEmpty());
    }

    @Test
    void countOfSubscribers_whenRegistrationsChange_countsInstancesRatherThanMethods() {
        MultiMethodSubscriber first = new MultiMethodSubscriber();
        MultiMethodSubscriber second = new MultiMethodSubscriber();
        assertEquals(0, eventBus.countOfSubscribers());

        eventBus.register(first);
        assertEquals(1, eventBus.countOfSubscribers());
        eventBus.register(first, "replacement");
        assertEquals(1, eventBus.countOfSubscribers());
        eventBus.register(second);
        assertEquals(2, eventBus.countOfSubscribers());
        eventBus.unregister(first);
        assertEquals(1, eventBus.countOfSubscribers());
        eventBus.unregister(second);
        assertEquals(0, eventBus.countOfSubscribers());
    }

    @Test
    void hasSubscribers_whenTypeMatches_ignoresEventIdAndTracksRemoval() {
        Object subscriber = new Object() {
            @Subscribe(eventId = "named")
            public void onEvent(CharSequence event) {
            }
        };
        assertFalse(eventBus.hasSubscribers(String.class));
        eventBus.register(subscriber);

        assertTrue(eventBus.hasSubscribers(String.class));
        assertTrue(eventBus.hasSubscribers(CharSequence.class));
        assertFalse(eventBus.hasSubscribers(Integer.class));
        assertTrue(eventBus.subscribers(String.class).isEmpty(), "The unqualified query still filters by event ID");
        eventBus.unregister(subscriber);
        assertFalse(eventBus.hasSubscribers(String.class));
    }

    @Test
    void hasSubscribers_whenHandlerIsStrict_requiresExactType() {
        Object subscriber = new Object() {
            @Subscribe(strictEventType = true)
            public void onEvent(CharSequence event) {
            }
        };
        eventBus.register(subscriber);

        assertTrue(eventBus.hasSubscribers(CharSequence.class));
        assertFalse(eventBus.hasSubscribers(String.class));
        assertFalse(eventBus.hasSubscribers(StringBuilder.class));
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

    public static class SubSubEvent extends SubEvent {
    }
}
