package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
    void create_whenUnnamed_assignsNonEmptyDistinctIdentifiers() {
        EventBus first = EventBus.create();
        EventBus second = EventBus.create();
        assertNotNull(first);
        assertNotNull(second);

        assertNotNull(first.identifier());
        assertNotNull(second.identifier());
        assertFalse(first.identifier().isEmpty());
        assertFalse(second.identifier().isEmpty());
        assertNotEquals(first.identifier(), second.identifier());
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
    void getDefault_whenCalledRepeatedly_returnsNamedSingleton() {
        EventBus defaultBus = EventBus.getDefault();
        assertNotNull(defaultBus);
        assertEquals("default", defaultBus.identifier());
        assertSame(defaultBus, EventBus.getDefault());
    }

    @Test
    void subscribers_whenTypeMatches_returnsOnlyMatchingSubscribers() {
        StringSubscriber subscriber = new StringSubscriber();
        eventBus.register(subscriber);

        List<Object> subscribers = eventBus.subscribers(String.class);
        assertEquals(List.of(subscriber), subscribers);

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
        assertEquals(List.of(subscriber), subscribers);

        eventBus.unregister(subscriber);
    }

    @Test
    void subscribers_whenTypeIsSubtype_includesCompatibleHandlers() {
        HierarchySubscriber subscriber = new HierarchySubscriber();
        eventBus.register(subscriber);

        // SubEvent extends BaseEvent, so subscriber should appear for SubEvent (because onBaseEvent accepts it)
        List<Object> baseSubs = eventBus.subscribers(BaseEvent.class);
        assertEquals(List.of(subscriber), baseSubs);

        List<Object> subSubs = eventBus.subscribers(SubEvent.class);
        assertEquals(List.of(subscriber), subSubs);

        eventBus.unregister(subscriber);
    }

    @Test
    void subscribers_whenIdsDiffer_keepsChannelsSeparate() {
        StringSubscriber subscriber1 = new StringSubscriber();
        StringSubscriber subscriber2 = new StringSubscriber();

        eventBus.register(subscriber1, "event1");
        eventBus.register(subscriber2, "event2");

        List<Object> subscribers = eventBus.subscribers("event1", String.class);
        assertEquals(List.of(subscriber1), subscribers);

        subscribers = eventBus.subscribers("event2", String.class);
        assertEquals(List.of(subscriber2), subscribers);
    }

    @Test
    void subscribers_whenIdIsUnknown_returnsEmptySnapshot() {
        StringSubscriber subscriber = new StringSubscriber();
        eventBus.register(subscriber, "myId");

        List<Object> result = eventBus.subscribers("nonExistent", String.class);
        assertTrue(result.isEmpty());

        eventBus.unregister(subscriber);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void subscribers_whenIdIsNullOrEmpty_usesUnqualifiedChannel(String eventId) {
        StringSubscriber subscriber = new StringSubscriber();
        eventBus.register(subscriber);

        assertEquals(List.of(subscriber), eventBus.subscribers(eventId, String.class));
    }

    @ParameterizedTest(name = "query={0}")
    @EnumSource(SubscriberQuery.class)
    void subscriberQueries_whenBusIsEmpty_returnEmptySnapshots(SubscriberQuery query) {
        List<Object> snapshot = query.read(eventBus);
        assertNotNull(snapshot);
        assertTrue(snapshot.isEmpty());
    }

    @ParameterizedTest(name = "query={0}")
    @EnumSource(SubscriberQuery.class)
    void subscriberQueries_whenSnapshotOrRegistryChanges_keepThemIndependent(SubscriberQuery query) {
        StringSubscriber first = new StringSubscriber();
        StringSubscriber second = new StringSubscriber();
        eventBus.register(first, query.eventId);
        List<Object> snapshot = query.read(eventBus);

        eventBus.register(second, query.eventId);
        assertEquals(List.of(first), snapshot, "Registration must not update an earlier snapshot");
        snapshot.clear();
        assertEquals(2, eventBus.countOfSubscribers(), "Changing a snapshot must not mutate the registry");
        List<Object> current = query.read(eventBus);
        assertHaveSameElements(List.of(first, second), current);
        eventBus.unregister(first).unregister(second);
        assertHaveSameElements(List.of(first, second), current);
        assertTrue(query.read(eventBus).isEmpty());
        assertTrue(eventBus.allSubscribers().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void subscriberQueries_whenTypeIsNull_rejectRegardlessOfRegistryContents(boolean populated) {
        if (populated) {
            eventBus.register(new StringSubscriber());
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
        assertHaveSameElements(List.of(subscriber1, subscriber2), allSubscribers);

        eventBus.unregister(subscriber1);
        eventBus.unregister(subscriber2);
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

    enum SubscriberQuery {
        ALL(null), UNQUALIFIED(null), NAMED("named");

        final String eventId;

        SubscriberQuery(String eventId) {
            this.eventId = eventId;
        }

        List<Object> read(EventBus bus) {
            return switch (this) {
                case ALL -> bus.allSubscribers();
                case UNQUALIFIED -> bus.subscribers(String.class);
                case NAMED -> bus.subscribers(eventId, String.class);
            };
        }
    }

    public static class StringSubscriber {

        @Subscribe
        public void onEvent(String event) {
        }
    }

    public static class MultiMethodSubscriber {

        @Subscribe
        public void onString(String event) {
        }

        @Subscribe
        public void onInteger(Integer event) {
        }

        @Subscribe
        public void onDouble(Double event) {
        }
    }

    public static class BaseEvent {
    }

    public static class SubEvent extends BaseEvent {
    }

    public static class HierarchySubscriber {

        @Subscribe
        public void onBaseEvent(BaseEvent event) {
        }

        @Subscribe
        public void onSubEvent(SubEvent event) {
        }

        @Subscribe
        public void onSubSubEvent(SubSubEvent event) {
        }
    }

    public static class SubSubEvent extends SubEvent {
    }
}
