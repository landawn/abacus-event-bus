package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;

@Tag("unit")
public class EventBusStickyTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = EventBus.create();
    }

    @Test
    void register_whenRepeatedAndStickyReplayIsDeduplicated_preservesDeliveryCounts() {
        List<String> liveEvents = new ArrayList<>();
        List<String> stickyEvents = new ArrayList<>();
        Subscriber<String> live = new Subscriber<>() {
            @Override
            public void on(String event) {
                liveEvents.add(event);
            }
        };
        Object sticky = new Object() {
            @Subscribe(sticky = true, deduplicate = true)
            public void on(String event) {
                stickyEvents.add(event);
            }
        };

        eventBus.register(live).register(live);
        eventBus.post("live").postSticky("retained");
        eventBus.register(sticky);
        eventBus.post("retained").post("next").post("next");

        assertEquals(2, eventBus.countOfSubscribers());
        assertEquals(List.of("live", "retained", "retained", "next", "next"), liveEvents);
        assertEquals(List.of("retained", "next"), stickyEvents);
    }

    @Test
    void register_whenStickyReplayIsThrottled_replaysOnlyFirstRetainedEvent() {
        List<String> stickyEvents = new ArrayList<>();
        Object sticky = new Object() {
            @Subscribe(sticky = true, intervalMillis = 60_000)
            public void on(String event) {
                stickyEvents.add(event);
            }
        };
        eventBus.postSticky("first").postSticky("second");

        eventBus.register(sticky);
        eventBus.post("live");

        // Replay order is unspecified; the interval admits exactly one replay and then suppresses the live post.
        assertEquals(1, stickyEvents.size(), "Replay must pass through interval throttling: " + stickyEvents);
        assertTrue(List.of("first", "second").contains(stickyEvents.get(0)), stickyEvents.toString());
    }

    @Test
    void register_whenOnlySomeHandlersAreSticky_replaysOnlyToThoseHandlers() {
        List<String> stickyEvents = new ArrayList<>();
        List<String> liveEvents = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe(sticky = true)
            public void onSticky(String event) {
                stickyEvents.add(event);
            }

            @Subscribe
            public void onLive(String event) {
                liveEvents.add(event);
            }
        };
        eventBus.postSticky("retained");

        eventBus.register(subscriber);

        assertEquals(List.of("retained"), stickyEvents);
        assertTrue(liveEvents.isEmpty());
        eventBus.post("live");
        assertEquals(List.of("retained", "live"), stickyEvents);
        assertEquals(List.of("live"), liveEvents);
    }

    @Test
    void postSticky_whenPostedBeforeRegistration_replaysOnceAndReturnsBus() {
        StickyStringSubscriber subscriber = new StickyStringSubscriber();

        EventBus result = eventBus.postSticky("Sticky Message");
        assertSame(eventBus, result);

        eventBus.register(subscriber);

        assertEquals(List.of("Sticky Message"), subscriber.receivedEvents);
    }

    @Test
    void postSticky_whenSubscriberIsAlreadyRegistered_deliversExactlyOnce() {
        // Subscriber already visible before the sticky event is posted: delivered once via postSticky,
        // and register's earlier (empty) sticky snapshot must not also deliver it.
        StickyStringSubscriber sub = new StickyStringSubscriber();
        eventBus.register(sub);
        eventBus.postSticky("only-once");

        assertEquals(List.of("only-once"), sub.receivedEvents);
    }

    @Test
    void postSticky_whenSubscriberIdMatches_replaysRetainedEvent() {
        StickyStringSubscriber subscriber = new StickyStringSubscriber();

        eventBus.postSticky("stickyEvent", "Sticky Message");
        eventBus.register(subscriber, "stickyEvent");

        assertEquals(List.of("Sticky Message"), subscriber.receivedEvents);
    }

    @Test
    void postSticky_whenSubscriberIsNotSticky_doesNotReplayRetainedEvent() {
        eventBus.postSticky("old sticky");

        StringSubscriber nonSticky = new StringSubscriber();
        eventBus.register(nonSticky);

        // Non-sticky subscriber should not get the sticky event posted before registration
        assertTrue(nonSticky.receivedEvents.isEmpty());

        eventBus.unregister(nonSticky);
    }

    @Test
    void postSticky_whenMultipleEventsAreRetained_replaysEachEvent() {
        eventBus.postSticky("sticky1");
        eventBus.postSticky("sticky2");

        StickyStringSubscriber subscriber = new StickyStringSubscriber();
        eventBus.register(subscriber);

        assertHaveSameElements(List.of("sticky1", "sticky2"), subscriber.receivedEvents);
    }

    @Test
    void postSticky_whenSubscriberIdDiffers_doesNotReplayEvent() {
        eventBus.postSticky("id1", "sticky msg");

        StickyStringSubscriber subscriber = new StickyStringSubscriber();
        eventBus.register(subscriber, "id2");

        // subscriber registered with id2 should not get sticky posted with id1
        assertTrue(subscriber.receivedEvents.isEmpty());

        eventBus.unregister(subscriber);
    }

    @Test
    void postSticky_whenEventIsNull_rejectsEveryOverload() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.postSticky((Object) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.postSticky("eventId", null));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void postSticky_whenIdIsNullOrEmpty_normalizesReplayQueriesAndRemoval(String eventId) {
        eventBus.postSticky(eventId, "sticky message");

        AtomicReference<String> result = new AtomicReference<>();
        Object handler = new Object() {
            @Subscribe(sticky = true)
            public void handle(String event) {
                result.set(event);
            }
        };

        eventBus.register(handler);
        assertEquals("sticky message", result.get());
        assertEquals(1, eventBus.stickyEvents(eventId, String.class).size());
        assertEquals(1, eventBus.stickyEvents(String.class).size());
        assertTrue(eventBus.removeStickyEvent(eventId, "sticky message"));
        assertTrue(eventBus.stickyEvents(String.class).isEmpty());
        assertFalse(eventBus.removeStickyEvent(eventId, "sticky message"));

        eventBus.unregister(handler);
    }

    @Test
    void postSticky_whenSameObjectIsReposted_updatesItsRetainedChannel() {
        String event = new String("retained");
        eventBus.postSticky("old", event);
        eventBus.postSticky("new", event);

        assertTrue(eventBus.stickyEvents("old", String.class).isEmpty());
        List<String> retained = eventBus.stickyEvents("new", String.class);
        assertEquals(1, retained.size());
        assertSame(event, retained.get(0));
        StickyStringSubscriber oldChannel = new StickyStringSubscriber();
        StickyStringSubscriber newChannel = new StickyStringSubscriber();
        eventBus.register(oldChannel, "old").register(newChannel, "new");
        assertTrue(oldChannel.receivedEvents.isEmpty());
        assertEquals(List.of(event), newChannel.receivedEvents);
        assertFalse(eventBus.removeStickyEvent("old", event));
        assertTrue(eventBus.removeStickyEvent("new", event));
    }

    @Test
    void postSticky_whenEqualObjectsAreDistinct_retainsAndRemovesEachByIdentity() {
        String first = new String("equal");
        String second = new String("equal");
        eventBus.postSticky(first).postSticky(second);

        List<String> retained = eventBus.stickyEvents(String.class);
        assertEquals(2, retained.size());
        assertTrue(retained.stream().anyMatch(event -> event == first));
        assertTrue(retained.stream().anyMatch(event -> event == second));
        assertFalse(eventBus.removeStickyEvent(new String("equal")));
        assertTrue(eventBus.removeStickyEvent(first));
        List<String> remaining = eventBus.stickyEvents(String.class);
        assertEquals(1, remaining.size());
        assertSame(second, remaining.get(0));
        assertTrue(eventBus.removeStickyEvent(second));
        assertTrue(eventBus.stickyEvents(String.class).isEmpty());
    }

    @Test
    void stickyEvents_whenIdsDiffer_returnsOnlyMatchingEvents() {
        eventBus.postSticky("id1", "Event 1");
        eventBus.postSticky("id2", "Event 2");
        eventBus.postSticky("id1", "Event 3");

        List<?> events = eventBus.stickyEvents("id1", String.class);
        assertHaveSameElements(List.of("Event 1", "Event 3"), events);

        events = eventBus.stickyEvents("id2", String.class);
        assertEquals(List.of("Event 2"), events);
    }

    @Test
    void stickyEvents_whenTypesDiffer_returnsOnlyMatchingEvents() {
        eventBus.postSticky("hello");
        eventBus.postSticky("world");
        eventBus.postSticky(123);

        List<String> stringEvents = eventBus.stickyEvents(String.class);
        assertHaveSameElements(List.of("hello", "world"), stringEvents);

        List<Integer> intEvents = eventBus.stickyEvents(Integer.class);
        assertEquals(List.of(123), intEvents);

        List<Double> doubleEvents = eventBus.stickyEvents(Double.class);
        assertTrue(doubleEvents.isEmpty());

        eventBus.removeAllStickyEvents();
    }

    @Test
    void stickyEvents_whenTypeIsSupertype_includesSubtypeInstances() {
        SubEvent subEvent = new SubEvent();
        eventBus.postSticky(subEvent);

        // BaseEvent.class.isAssignableFrom(SubEvent.class) is true
        List<BaseEvent> baseEvents = eventBus.stickyEvents(BaseEvent.class);
        assertEquals(1, baseEvents.size());
        assertSame(subEvent, baseEvents.get(0));

        List<SubEvent> subEvents = eventBus.stickyEvents(SubEvent.class);
        assertEquals(List.of(subEvent), subEvents);

        // SubSubEvent should not match
        List<SubSubEvent> subSubEvents = eventBus.stickyEvents(SubSubEvent.class);
        assertTrue(subSubEvents.isEmpty());
    }

    @Test
    void stickyEvents_whenTypeIsPrimitive_matchesBoxedEvents() {
        Integer boxed = 1234567;
        eventBus.postSticky(boxed);
        eventBus.postSticky("named", 7);

        List<Integer> unqualified = eventBus.stickyEvents(int.class);
        assertEquals(1, unqualified.size());
        assertSame(boxed, unqualified.get(0));
        assertEquals(List.of(7), eventBus.stickyEvents("named", int.class));
        assertTrue(eventBus.stickyEvents(long.class).isEmpty());
    }

    @Test
    void removeStickyEvents_whenTypeIsPrimitive_removesBoxedEvents() {
        eventBus.postSticky(1).postSticky("named", 2).postSticky("text");

        assertFalse(eventBus.removeStickyEvents(long.class));
        assertTrue(eventBus.removeStickyEvents(int.class));

        assertTrue(eventBus.stickyEvents(Integer.class).isEmpty());
        assertEquals(List.of(2), eventBus.stickyEvents("named", Integer.class));
        assertEquals(List.of("text"), eventBus.stickyEvents(String.class));
        assertTrue(eventBus.removeStickyEvents("named", int.class));
        assertTrue(eventBus.stickyEvents("named", Integer.class).isEmpty());
    }

    @Test
    void stickyEvents_whenBusIsEmpty_returnsEmptySnapshot() {
        List<String> events = eventBus.stickyEvents(String.class);
        assertNotNull(events);
        assertTrue(events.isEmpty());
    }

    @Test
    void stickyEvents_whenIdIsUnknown_returnsEmptySnapshot() {
        eventBus.postSticky("existing", "retained");

        assertTrue(eventBus.stickyEvents("missing", String.class).isEmpty());
        assertEquals(List.of("retained"), eventBus.stickyEvents("existing", String.class));
    }

    @ParameterizedTest(name = "explicitId={0}")
    @ValueSource(booleans = { false, true })
    void stickyEvents_whenSnapshotOrStoreChanges_keepsThemIndependent(boolean explicitId) {
        String eventId = explicitId ? "named" : null;
        String first = new String("first");
        String second = new String("second");
        eventBus.postSticky(eventId, first);
        List<String> snapshot = explicitId ? eventBus.stickyEvents(eventId, String.class) : eventBus.stickyEvents(String.class);

        eventBus.postSticky(eventId, second);

        assertEquals(1, snapshot.size());
        assertSame(first, snapshot.get(0), "Adding retained events must not update an earlier snapshot");
        snapshot.clear();
        List<String> current = explicitId ? eventBus.stickyEvents(eventId, String.class) : eventBus.stickyEvents(String.class);
        assertHaveSameElements(List.of(first, second), current);
        assertTrue(eventBus.removeStickyEvent(eventId, first));
        assertHaveSameElements(List.of(first, second), current);
        List<String> remaining = eventBus.stickyEvents(eventId, String.class);
        assertEquals(1, remaining.size());
        assertSame(second, remaining.get(0));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void stickyQueries_whenTypeIsNull_rejectRegardlessOfRetainedEvents(boolean populated) {
        if (populated) {
            eventBus.postSticky("retained");
        }

        assertThrows(IllegalArgumentException.class, () -> eventBus.stickyEvents((Class<Object>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.stickyEvents("id", (Class<Object>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.removeStickyEvents((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.removeStickyEvents("id", null));
    }

    @ParameterizedTest(name = "explicitId={0}")
    @ValueSource(booleans = { false, true })
    void removeStickyEvent_whenEventIsRetained_matchesIdentityAndChannelAndRemovesOnce(boolean explicitId) {
        String event = new String("retained");
        String eventId = explicitId ? "named" : null;
        if (explicitId) {
            eventBus.postSticky(eventId, event);
        } else {
            eventBus.postSticky(event);
        }

        assertFalse(eventBus.removeStickyEvent("wrong", event));
        assertEquals(List.of(event), eventBus.stickyEvents(eventId, String.class));
        assertTrue(explicitId ? eventBus.removeStickyEvent(eventId, event) : eventBus.removeStickyEvent(event));
        assertFalse(explicitId ? eventBus.removeStickyEvent(eventId, event) : eventBus.removeStickyEvent(event));
        assertFalse(eventBus.removeStickyEvent("wrong", event));
        assertTrue(eventBus.stickyEvents(eventId, String.class).isEmpty());
    }

    @Test
    void removeStickyEvent_whenEventIsAbsent_returnsFalse() {
        assertFalse(eventBus.removeStickyEvent("never posted"));
    }

    @Test
    void removeStickyEvents_whenIdAndTypeMatch_removesAllMatchesAndPreservesOthers() {
        eventBus.postSticky("id1", "first");
        eventBus.postSticky("id1", "second");
        eventBus.postSticky("id2", "other channel");
        eventBus.postSticky("id1", 123);
        assertHaveSameElements(List.of("first", "second"), eventBus.stickyEvents("id1", String.class));

        assertTrue(eventBus.removeStickyEvents("id1", String.class));

        assertTrue(eventBus.stickyEvents("id1", String.class).isEmpty());
        assertFalse(eventBus.removeStickyEvents("id1", String.class));
        assertEquals(List.of(123), eventBus.stickyEvents("id1", Integer.class));
        assertEquals(List.of("other channel"), eventBus.stickyEvents("id2", String.class));
    }

    @Test
    void removeStickyEvents_whenTypeMatches_removesEventsOnce() {
        eventBus.postSticky("sticky1");
        eventBus.postSticky("sticky2");

        boolean removed = eventBus.removeStickyEvents(String.class);
        assertTrue(removed);

        List<String> remaining = eventBus.stickyEvents(String.class);
        assertTrue(remaining.isEmpty());

        // Removing again should return false
        assertFalse(eventBus.removeStickyEvents(String.class));
    }

    @Test
    void removeStickyEvents_whenTypeIsSupertype_removesSubtypeInstances() {
        eventBus.postSticky(new SubEvent());
        eventBus.postSticky(new BaseEvent());

        // Removing BaseEvent.class should also remove SubEvent since SubEvent is assignable to BaseEvent
        assertTrue(eventBus.removeStickyEvents(BaseEvent.class));
        assertTrue(eventBus.stickyEvents(BaseEvent.class).isEmpty());
        assertTrue(eventBus.stickyEvents(SubEvent.class).isEmpty());
    }

    @Test
    void removeStickyEvents_whenTypeDoesNotMatch_returnsFalse() {
        eventBus.postSticky("hello");
        assertFalse(eventBus.removeStickyEvents(Integer.class));
    }

    @Test
    void removeStickyEvents_whenIdDoesNotMatch_returnsFalse() {
        eventBus.postSticky("id1", "event");
        assertFalse(eventBus.removeStickyEvents("id2", String.class));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void removeStickyEvents_whenIdIsNullOrEmpty_removesUnqualifiedEvents(String eventId) {
        eventBus.postSticky("first").postSticky("second");
        eventBus.postSticky("named", "preserved");

        assertTrue(eventBus.removeStickyEvents(eventId, String.class));
        assertTrue(eventBus.stickyEvents(String.class).isEmpty());
        assertEquals(List.of("preserved"), eventBus.stickyEvents("named", String.class));
        assertFalse(eventBus.removeStickyEvents(eventId, String.class));
    }

    @Test
    void removeAllStickyEvents_whenEventsAreRetained_clearsEveryChannelAndTypeAndRemainsIdempotent() {
        assertFalse(eventBus.removeAllStickyEvents());
        assertTrue(eventBus.stickyEvents(Object.class).isEmpty());
        eventBus.postSticky("first").postSticky("second").postSticky(123);
        eventBus.postSticky("named", "other channel");

        assertTrue(eventBus.removeAllStickyEvents());

        assertTrue(eventBus.stickyEvents(String.class).isEmpty());
        assertTrue(eventBus.stickyEvents(Integer.class).isEmpty());
        assertTrue(eventBus.stickyEvents("named", String.class).isEmpty());
        assertFalse(eventBus.removeAllStickyEvents());
    }

    public static class StickyStringSubscriber {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe(sticky = true)
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class StringSubscriber {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class BaseEvent {
    }

    public static class SubEvent extends BaseEvent {
    }

    public static class SubSubEvent extends SubEvent {
    }
}
