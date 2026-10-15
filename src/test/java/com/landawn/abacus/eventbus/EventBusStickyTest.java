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
        TestStickySubscriber subscriber = new TestStickySubscriber();

        EventBus result = eventBus.postSticky("Sticky Message");
        assertSame(eventBus, result);

        eventBus.register(subscriber);

        assertEquals(List.of("Sticky Message"), subscriber.receivedEvents);
    }

    @Test
    void postSticky_whenSubscriberIsAlreadyRegistered_deliversExactlyOnce() {
        // Subscriber already visible before the sticky event is posted: delivered once via postSticky,
        // and register's earlier (empty) sticky snapshot must not also deliver it.
        TestStickySubscriber sub = new TestStickySubscriber();
        eventBus.register(sub);
        eventBus.postSticky("only-once");

        assertEquals(List.of("only-once"), sub.receivedEvents);
    }

    @Test
    void postSticky_whenSubscriberIdMatches_replaysRetainedEvent() {
        TestStickySubscriber subscriber = new TestStickySubscriber();

        eventBus.postSticky("stickyEvent", "Sticky Message");
        eventBus.register(subscriber, "stickyEvent");

        assertEquals(List.of("Sticky Message"), subscriber.receivedEvents);
    }

    @Test
    void postSticky_whenSubscriberIsNotSticky_doesNotReplayRetainedEvent() {
        eventBus.postSticky("old sticky");

        TestSubscriber nonSticky = new TestSubscriber();
        eventBus.register(nonSticky);

        // Non-sticky subscriber should not get the sticky event posted before registration
        assertTrue(nonSticky.receivedEvents.isEmpty());

        eventBus.unregister(nonSticky);
    }

    @Test
    void postSticky_whenMultipleEventsAreRetained_replaysEachEvent() {
        eventBus.postSticky("sticky1");
        eventBus.postSticky("sticky2");

        TestStickySubscriber subscriber = new TestStickySubscriber();
        eventBus.register(subscriber);

        assertEquals(2, subscriber.receivedEvents.size());
        assertTrue(subscriber.receivedEvents.contains("sticky1"));
        assertTrue(subscriber.receivedEvents.contains("sticky2"));
    }

    @Test
    void postSticky_whenSubscriberIdDiffers_doesNotReplayEvent() {
        eventBus.postSticky("id1", "sticky msg");

        TestStickySubscriber subscriber = new TestStickySubscriber();
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
        TestStickySubscriber oldChannel = new TestStickySubscriber();
        TestStickySubscriber newChannel = new TestStickySubscriber();
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
        assertEquals(2, stringEvents.size());
        assertTrue(stringEvents.contains("hello"));
        assertTrue(stringEvents.contains("world"));

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
    void stickyEvents_whenBusIsEmpty_returnsEmptySnapshot() {
        List<String> events = eventBus.stickyEvents(String.class);
        assertNotNull(events);
        assertTrue(events.isEmpty());
    }

    @Test
    void stickyEvents_whenIdIsUnknown_returnsEmptySnapshot() {
        List<String> events = eventBus.stickyEvents("nonExistent", String.class);
        assertNotNull(events);
        assertTrue(events.isEmpty());
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

    @Test
    void removeStickyEvent_whenEventIsRetained_removesItOnce() {
        String event = "sticky";
        eventBus.postSticky(event);
        assertTrue(eventBus.removeStickyEvent(event));
        assertFalse(eventBus.removeStickyEvent(event)); // Already removed
    }

    @Test
    void removeStickyEvent_whenEventAndIdMatch_removesEvent() {
        String event = "Sticky Event";
        eventBus.postSticky("eventId", event);

        boolean removed = eventBus.removeStickyEvent("eventId", event);
        assertTrue(removed);

        removed = eventBus.removeStickyEvent("wrongId", event);
        assertFalse(removed);
    }

    @Test
    void removeStickyEvent_whenIdDiffers_preservesRetainedEvent() {
        String event = "sticky";
        eventBus.postSticky("correctId", event);

        assertFalse(eventBus.removeStickyEvent("wrongId", event));
        assertTrue(eventBus.removeStickyEvent("correctId", event));
    }

    @Test
    void removeStickyEvent_whenEventIsAbsent_returnsFalse() {
        assertFalse(eventBus.removeStickyEvent("never posted"));
    }

    @Test
    void removeStickyEvents_whenMultipleEventsMatch_removesAllMatches() {
        eventBus.postSticky("eventId", "test1");
        eventBus.postSticky("eventId", "test2");

        assertTrue(eventBus.removeStickyEvents("eventId", String.class));
        List<?> events = eventBus.stickyEvents("eventId", String.class);
        assertEquals(0, events.size());
    }

    @Test
    void removeStickyEvents_whenIdAndTypeMatch_preservesOtherChannelsAndTypes() {
        eventBus.postSticky("id1", "Event 1");
        eventBus.postSticky("id2", "Event 2");
        eventBus.postSticky("id1", 123);

        boolean removed = eventBus.removeStickyEvents("id1", String.class);
        assertTrue(removed);

        List<?> remaining = eventBus.stickyEvents("id1", String.class);
        assertEquals(0, remaining.size());
        assertEquals(List.of(123), eventBus.stickyEvents("id1", Integer.class));

        remaining = eventBus.stickyEvents("id2", String.class);
        assertEquals(List.of("Event 2"), remaining);
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
    void removeAllStickyEvents_whenEventsAreRetained_clearsEveryType() {
        eventBus.postSticky("Event 1");
        eventBus.postSticky("Event 2");
        eventBus.postSticky(123);

        assertTrue(eventBus.removeAllStickyEvents());

        assertEquals(0, eventBus.stickyEvents(String.class).size());
        assertEquals(0, eventBus.stickyEvents(Integer.class).size());
    }

    @Test
    void removeAllStickyEvents_whenBusIsEmpty_returnsFalse() {
        // Should not throw on empty bus and should report that nothing was removed.
        assertFalse(eventBus.removeAllStickyEvents());
        assertTrue(eventBus.stickyEvents(Object.class).isEmpty());
    }

    public static class TestStickySubscriber {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe(sticky = true)
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class TestSubscriber {
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
