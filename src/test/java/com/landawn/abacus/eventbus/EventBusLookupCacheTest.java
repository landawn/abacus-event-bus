package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;

@Tag("unit")
public class EventBusLookupCacheTest extends TestBase {
    private static Map<?, ?> cache(EventBus bus) throws Exception {
        var field = EventBus.class.getDeclaredField("listOfEventIdSubMap");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(bus);
    }

    @Test
    public void absentIdsAreNotRetained() throws Exception {
        EventBus bus = EventBus.create();
        for (int i = 0; i < 10_000; i++) {
            bus.post("\u8def\u7531\ud83d\ude80-" + i, "");
        }
        assertTrue(cache(bus).isEmpty());
    }

    @Test
    public void aPreviouslyAbsentIdCanBeRegisteredReplacedAndRemoved() throws Exception {
        EventBus bus = EventBus.create();
        String id = "\u4e8b\u4ef6\ud83d\ude80";
        bus.post(id, "before registration");
        List<String> first = new ArrayList<>();
        List<String> second = new ArrayList<>();
        Subscriber<String> one = first::add;
        Subscriber<String> two = second::add;
        bus.register(one, id).post(id, "");
        assertEquals(List.of(""), first);
        assertEquals(1, cache(bus).size());
        bus.register(two, id).post(id, "both");
        assertEquals(List.of("", "both"), first);
        assertEquals(List.of("both"), second);
        bus.unregister(one).post(id, "remaining");
        assertEquals(List.of("both", "remaining"), second);
        bus.unregister(two).post(id, "absent again");
        assertTrue(cache(bus).isEmpty());
    }

    public static class Unfiltered {
        final List<String> events = new ArrayList<>();

        @Subscribe
        public void on(String event) {
            events.add(event);
        }
    }

    @Test
    public void nullEmptyAndStickyIdsKeepTheirDeliveryRules() throws Exception {
        EventBus bus = EventBus.create();
        Unfiltered handler = new Unfiltered();
        bus.register(handler);
        bus.post((String) null, "null").post("", "").post("missing", "ignored");
        assertEquals(List.of("null", ""), handler.events);
        assertTrue(cache(bus).isEmpty());
        String id = "\u9ecf\u6027\ud83d\ude80";
        bus.postSticky(id, "retained");
        assertTrue(cache(bus).isEmpty());
        class Sticky {
            final List<String> events = new ArrayList<>();

            @Subscribe(sticky = true)
            public void on(String event) {
                events.add(event);
            }
        }
        Sticky sticky = new Sticky();
        bus.register(sticky, id);
        assertEquals(List.of("retained"), sticky.events);
        bus.post(id, "next");
        assertEquals(List.of("retained", "next"), sticky.events);
        assertTrue(bus.removeAllStickyEvents());
        bus.unregister(sticky);
        assertTrue(cache(bus).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> bus.post(id, null));
    }

    private static Map<?, ?> eventIdIndex(EventBus bus) throws Exception {
        var field = EventBus.class.getDeclaredField("registeredEventIdSubMap");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(bus);
    }

    public static class MixedIds {
        final List<String> events = new ArrayList<>();

        @Subscribe(eventId = "annotatedA")
        public void onA(String event) {
            events.add("A:" + event);
        }

        @Subscribe(eventId = "annotatedB")
        public void onB(String event) {
            events.add("B:" + event);
        }

        @Subscribe
        public void onAny(String event) {
            events.add("any:" + event);
        }
    }

    // unregister touches only the event IDs of the removed identifiers; other IDs keep delivering.
    @Test
    public void unregisterInvalidatesOnlyAffectedEventIds() throws Exception {
        EventBus bus = EventBus.create();
        List<String> received = new ArrayList<>();
        List<Subscriber<String>> subscribers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int k = i;
            Subscriber<String> s = e -> received.add(k + ":" + e);
            subscribers.add(s);
            bus.register(s, "id" + i);
        }
        Subscriber<String> shared = e -> received.add("shared:" + e);
        bus.register(shared, "id2");
        for (int i = 0; i < 5; i++) {
            bus.post("id" + i, "p");
        }
        assertEquals(List.of("0:p", "1:p", "2:p", "shared:p", "3:p", "4:p"), received);
        assertEquals(5, cache(bus).size());

        received.clear();
        bus.unregister(subscribers.get(2));
        assertEquals(java.util.Set.of("id0", "id1", "id2", "id3", "id4"), eventIdIndex(bus).keySet());
        assertFalse(cache(bus).containsKey("id2"));
        bus.unregister(subscribers.get(4));
        assertEquals(java.util.Set.of("id0", "id1", "id2", "id3"), eventIdIndex(bus).keySet());
        assertFalse(cache(bus).containsKey("id4"));
        for (int i = 0; i < 5; i++) {
            bus.post("id" + i, "q");
        }
        assertEquals(List.of("0:q", "1:q", "shared:q", "3:q"), received);
        assertEquals(java.util.Set.of("id0", "id1", "id2", "id3"), cache(bus).keySet());

        // unregistering an unknown or already-removed subscriber is a no-op
        bus.unregister(subscribers.get(4));
        bus.unregister("not registered");
        received.clear();
        for (int i = 0; i < 5; i++) {
            bus.post("id" + i, "r");
        }
        assertEquals(List.of("0:r", "1:r", "shared:r", "3:r"), received);

        bus.unregister(shared);
        bus.unregister(subscribers.get(0));
        bus.unregister(subscribers.get(1));
        bus.unregister(subscribers.get(3));
        assertTrue(eventIdIndex(bus).isEmpty());
        assertTrue(cache(bus).isEmpty());
        received.clear();
        for (int i = 0; i < 5; i++) {
            bus.post("id" + i, "s");
        }
        assertTrue(received.isEmpty());
        assertTrue(cache(bus).isEmpty());
    }

    // annotation-declared event IDs, re-registration under an explicit ID, and final removal.
    @Test
    public void reregistrationReplacesAnnotatedEventIds() throws Exception {
        EventBus bus = EventBus.create();
        MixedIds handler = new MixedIds();
        MixedIds other = new MixedIds();
        bus.register(handler);
        bus.register(other);
        assertEquals(java.util.Set.of("annotatedA", "annotatedB"), eventIdIndex(bus).keySet());
        bus.post("annotatedA", "1").post("annotatedB", "2").post("3");
        assertEquals(List.of("A:1", "B:2", "any:3"), handler.events);
        assertEquals(List.of("A:1", "B:2", "any:3"), other.events);

        // Re-registering under an explicit ID files every method under that ID and drops the annotated ones.
        bus.register(handler, "explicit");
        assertEquals(java.util.Set.of("annotatedA", "annotatedB", "explicit"), eventIdIndex(bus).keySet());
        handler.events.clear();
        other.events.clear();
        bus.post("annotatedA", "4").post("annotatedB", "5").post("explicit", "6").post("7");
        assertEquals(3, handler.events.size());
        assertTrue(handler.events.containsAll(List.of("A:6", "B:6", "any:6")));
        assertEquals(List.of("A:4", "B:5", "any:7"), other.events);

        bus.unregister(other);
        assertEquals(java.util.Set.of("explicit"), eventIdIndex(bus).keySet());
        assertFalse(cache(bus).containsKey("annotatedA"));
        assertFalse(cache(bus).containsKey("annotatedB"));
        other.events.clear();
        handler.events.clear();
        bus.post("annotatedA", "8").post("annotatedB", "9").post("10");
        assertTrue(handler.events.isEmpty());
        assertTrue(other.events.isEmpty());

        bus.register(handler);
        assertEquals(java.util.Set.of("annotatedA", "annotatedB"), eventIdIndex(bus).keySet());
        bus.post("explicit", "11").post("annotatedA", "12");
        assertEquals(List.of("A:12"), handler.events);

        bus.unregister(handler);
        assertTrue(eventIdIndex(bus).isEmpty());
        assertTrue(cache(bus).isEmpty());
        assertTrue(bus.allSubscribers().isEmpty());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "concurrent-id")
    public void concurrentPostsDeliverOncePerSubscriberWhileCacheIsCold(String eventId) throws InterruptedException {
        EventBus bus = EventBus.create();
        AtomicInteger deliveries = new AtomicInteger();
        Object subscriber = new Object() {
            @Subscribe
            public void on(String event) {
                deliveries.incrementAndGet();
            }
        };
        bus.register(subscriber, eventId);

        runConcurrently(20, () -> bus.post(eventId, "event"));

        assertEquals(20, deliveries.get(), "Every concurrent post must deliver exactly once");
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "cached-id")
    public void registrationInvalidatesWarmCacheBeforeConcurrentPosts(String eventId) throws InterruptedException {
        EventBus bus = EventBus.create();
        AtomicInteger earlyDeliveries = new AtomicInteger();
        AtomicInteger lateDeliveries = new AtomicInteger();
        Object early = new Object() {
            @Subscribe
            public void on(String event) {
                earlyDeliveries.incrementAndGet();
            }
        };
        Object late = new Object() {
            @Subscribe
            public void on(String event) {
                lateDeliveries.incrementAndGet();
            }
        };
        bus.register(early, eventId).post(eventId, "warmup");
        bus.register(late, eventId);

        runConcurrently(20, () -> bus.post(eventId, "after registration"));

        assertEquals(21, earlyDeliveries.get());
        assertEquals(20, lateDeliveries.get(), "A new subscriber must be visible after cache invalidation");
    }
}
