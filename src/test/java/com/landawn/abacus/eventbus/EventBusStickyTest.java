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

import com.landawn.abacus.TestBase;

@Tag("unit")
public class EventBusStickyTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    public void setUp() {
        eventBus = EventBus.create();
    }

    @Test
    public void testStickyPostThenRegister_deliveredExactlyOnce() {
        // Event already recorded before the subscriber registers: delivered once via register's replay.
        TestStickySubscriber sub = new TestStickySubscriber();
        eventBus.postSticky("only-once");
        eventBus.register(sub);

        assertEquals(List.of("only-once"), sub.receivedEvents);
    }

    @Test
    public void testStickyRegisterThenPostSticky_deliveredExactlyOnce() {
        // Subscriber already visible before the sticky event is posted: delivered once via postSticky,
        // and register's earlier (empty) sticky snapshot must not also deliver it.
        TestStickySubscriber sub = new TestStickySubscriber();
        eventBus.register(sub);
        eventBus.postSticky("only-once");

        assertEquals(List.of("only-once"), sub.receivedEvents);
    }

    // ---- postSticky(String, Object) ----

    @Test
    public void testPostStickyWithEventId() {
        TestStickySubscriber subscriber = new TestStickySubscriber();

        eventBus.postSticky("stickyEvent", "Sticky Message");
        eventBus.register(subscriber, "stickyEvent");

        assertEquals(List.of("Sticky Message"), subscriber.receivedEvents);
    }

    @Test
    public void testGetStickyEventsWithEventId() {
        eventBus.postSticky("id1", "Event 1");
        eventBus.postSticky("id2", "Event 2");
        eventBus.postSticky("id1", "Event 3");

        List<?> events = eventBus.stickyEvents("id1", String.class);
        assertHaveSameElements(List.of("Event 1", "Event 3"), events);

        events = eventBus.stickyEvents("id2", String.class);
        assertEquals(List.of("Event 2"), events);
    }

    // ---- postSticky(Object) ----

    @Test
    public void testPostSticky() {
        TestStickySubscriber subscriber = new TestStickySubscriber();

        EventBus result = eventBus.postSticky("Sticky Message");
        assertSame(eventBus, result);

        eventBus.register(subscriber);

        assertEquals(List.of("Sticky Message"), subscriber.receivedEvents);
    }

    @Test
    public void testPostSticky_NonStickySubscriberDoesNotGetOldEvent() {
        eventBus.postSticky("old sticky");

        TestSubscriber nonSticky = new TestSubscriber();
        eventBus.register(nonSticky);

        // Non-sticky subscriber should not get the sticky event posted before registration
        assertTrue(nonSticky.receivedEvents.isEmpty());

        eventBus.unregister(nonSticky);
    }

    @Test
    public void testPostSticky_MultipleStickyEventsDeliveredToNewSubscriber() {
        eventBus.postSticky("sticky1");
        eventBus.postSticky("sticky2");

        TestStickySubscriber subscriber = new TestStickySubscriber();
        eventBus.register(subscriber);

        assertEquals(2, subscriber.receivedEvents.size());
        assertTrue(subscriber.receivedEvents.contains("sticky1"));
        assertTrue(subscriber.receivedEvents.contains("sticky2"));
    }

    @Test
    public void testStickyEventWithEmptyEventIdBehavesAsNoEventId() {
        eventBus.postSticky("", "sticky message");

        AtomicReference<String> result = new AtomicReference<>();
        Object handler = new Object() {
            @Subscribe(sticky = true)
            public void handle(String event) {
                result.set(event);
            }
        };

        eventBus.register(handler);
        assertEquals("sticky message", result.get());
        assertEquals(1, eventBus.stickyEvents("", String.class).size());
        assertEquals(1, eventBus.stickyEvents(String.class).size());
        assertTrue(eventBus.removeStickyEvent("", "sticky message"));
        assertTrue(eventBus.stickyEvents(String.class).isEmpty());

        eventBus.unregister(handler);
    }

    @Test
    public void testPostSticky_WithEventId_NonMatchingSubscriberIgnored() {
        eventBus.postSticky("id1", "sticky msg");

        TestStickySubscriber subscriber = new TestStickySubscriber();
        eventBus.register(subscriber, "id2");

        // subscriber registered with id2 should not get sticky posted with id1
        assertTrue(subscriber.receivedEvents.isEmpty());

        eventBus.unregister(subscriber);
    }

    @Test
    public void testPostStickyRejectsNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.postSticky((Object) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.postSticky("eventId", null));
    }

    // ---- removeStickyEvent(Object) ----

    @Test
    public void testRemoveStickyEvent() {
        String event = "sticky";
        eventBus.postSticky(event);
        assertTrue(eventBus.removeStickyEvent(event));
        assertFalse(eventBus.removeStickyEvent(event)); // Already removed
    }

    // ---- removeStickyEvent(String, Object) ----

    @Test
    public void testRemoveStickyEventWithEventId() {
        String event = "Sticky Event";
        eventBus.postSticky("eventId", event);

        boolean removed = eventBus.removeStickyEvent("eventId", event);
        assertTrue(removed);

        removed = eventBus.removeStickyEvent("wrongId", event);
        assertFalse(removed);
    }

    @Test
    public void testRemoveStickyEvent_WrongEventId() {
        String event = "sticky";
        eventBus.postSticky("correctId", event);

        assertFalse(eventBus.removeStickyEvent("wrongId", event));
        assertTrue(eventBus.removeStickyEvent("correctId", event));
    }

    @Test
    public void testRemoveStickyEvent_NonExistent() {
        assertFalse(eventBus.removeStickyEvent("never posted"));
    }

    @Test
    public void testRemoveStickyEvent_WithNullEventId() {
        String event = "sticky";
        eventBus.postSticky(event);

        // Removing with null eventId should match event posted without eventId
        assertTrue(eventBus.removeStickyEvent(null, event));
        assertFalse(eventBus.removeStickyEvent(null, event));
    }

    @Test
    public void testRemoveStickyEvents_NoMatchingType() {
        eventBus.postSticky("hello");
        assertFalse(eventBus.removeStickyEvents(Integer.class));
    }

    // ---- removeStickyEvents(String, Class) ----

    @Test
    public void testRemoveStickyEvents() {
        eventBus.postSticky("eventId", "test1");
        eventBus.postSticky("eventId", "test2");

        assertTrue(eventBus.removeStickyEvents("eventId", String.class));
        List<?> events = eventBus.stickyEvents("eventId", String.class);
        assertEquals(0, events.size());
    }

    @Test
    public void testRemoveStickyEventsWithEventId() {
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
    public void testRemoveStickyEvents_WithEventId_NoMatch() {
        eventBus.postSticky("id1", "event");
        assertFalse(eventBus.removeStickyEvents("id2", String.class));
    }

    // ---- removeStickyEvents(Class) ----

    @Test
    public void testRemoveStickyEventsByType() {
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
    public void testRemoveStickyEventsWithEmptyEventIdBehavesAsNoEventId() {
        eventBus.postSticky("", "test1");
        eventBus.postSticky("", "test2");

        assertTrue(eventBus.removeStickyEvents("", String.class));
        assertEquals(0, eventBus.stickyEvents("", String.class).size());
    }

    @Test
    public void testRemoveStickyEvents_TypeHierarchy() {
        eventBus.postSticky(new SubEvent());
        eventBus.postSticky(new BaseEvent());

        // Removing BaseEvent.class should also remove SubEvent since SubEvent is assignable to BaseEvent
        assertTrue(eventBus.removeStickyEvents(BaseEvent.class));
        assertTrue(eventBus.stickyEvents(BaseEvent.class).isEmpty());
        assertTrue(eventBus.stickyEvents(SubEvent.class).isEmpty());
    }

    // ---- removeAllStickyEvents ----

    @Test
    public void testRemoveAllStickyEvents() {
        eventBus.postSticky("Event 1");
        eventBus.postSticky("Event 2");
        eventBus.postSticky(123);

        assertTrue(eventBus.removeAllStickyEvents());

        assertEquals(0, eventBus.stickyEvents(String.class).size());
        assertEquals(0, eventBus.stickyEvents(Integer.class).size());
    }

    @Test
    public void testRemoveAllStickyEvents_EmptyBus() {
        // Should not throw on empty bus and should report that nothing was removed.
        assertFalse(eventBus.removeAllStickyEvents());
        assertTrue(eventBus.stickyEvents(Object.class).isEmpty());
    }

    // ---- stickyEvents(String, Class) ----

    @Test
    public void testGetStickyEvents() {
        eventBus.postSticky("event1", "sticky1");
        eventBus.postSticky("event1", "sticky2");

        List<?> events = eventBus.stickyEvents("event1", String.class);
        assertEquals(2, events.size());

        eventBus.removeAllStickyEvents();
    }

    // ---- stickyEvents(Class) ----

    @Test
    public void testStickyEventsByType() {
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
    public void testStickyEvents_EmptyBus() {
        List<String> events = eventBus.stickyEvents(String.class);
        assertNotNull(events);
        assertTrue(events.isEmpty());
    }

    @Test
    public void testStickyEvents_TypeHierarchy() {
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
    public void testStickyEvents_WithEventId_NonExistent() {
        List<String> events = eventBus.stickyEvents("nonExistent", String.class);
        assertNotNull(events);
        assertTrue(events.isEmpty());
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
