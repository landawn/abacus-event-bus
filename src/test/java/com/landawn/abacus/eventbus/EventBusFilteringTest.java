package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class EventBusFilteringTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    void setUp() {
        eventBus = EventBus.create();
    }

    @Test
    void post_whenConsecutiveEventsAreEqual_suppressesOnlyConsecutiveDuplicates() {
        List<String> receivedEvents = new ArrayList<>();

        Object subscriber = new Object() {
            @Subscribe(deduplicate = true)
            public void onEvent(String event) {
                receivedEvents.add(event);
            }
        };

        eventBus.register(subscriber);

        eventBus.post("Event A");
        eventBus.post("Event A");
        eventBus.post("Event B");
        eventBus.post("Event B");
        eventBus.post("Event A");

        assertEquals(List.of("Event A", "Event B", "Event A"), receivedEvents);
    }

    @Test
    void post_whenMinimumIntervalExpires_deliversNextEvent() throws InterruptedException {
        List<String> received = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe(intervalMillis = 20)
            public void onEvent(String event) {
                received.add(event);
            }
        };
        eventBus.register(subscriber);
        eventBus.post("first");
        TimeUnit.MILLISECONDS.sleep(30);
        eventBus.post("after interval");
        assertEquals(List.of("first", "after interval"), received);
    }

    @Test
    void post_whenIntervalIsMaximal_deliversFirstEventAndSuppressesNext() {
        AtomicInteger eventCount = new AtomicInteger();

        class TestClass {
            @Subscribe(intervalMillis = Long.MAX_VALUE)
            public void onEvent(String event) {
                eventCount.incrementAndGet();
            }
        }

        eventBus.register(new TestClass());
        eventBus.post("first");
        eventBus.post("second");

        assertEquals(1, eventCount.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void post_whenIntervalIsNonPositive_deliversEveryEvent(boolean negativeInterval) {
        List<String> received = new ArrayList<>();
        Object subscriber = negativeInterval ? new NegativeIntervalSubscriber(received) : new ZeroIntervalSubscriber(received);
        eventBus.register(subscriber);

        eventBus.post("same").post("same").post("same");

        assertEquals(List.of("same", "same", "same"), received);
    }

    @ParameterizedTest
    @EnumSource(value = ThreadMode.class, names = { "DEFAULT", "THREAD_POOL_EXECUTOR" })
    void post_whenAcceptedEventEqualityThrows_keepsDeliveringLaterEvents(ThreadMode mode) {
        EventBus bus = EventBus.create("inline", Runnable::run);
        List<String> received = new ArrayList<>();
        bus.register(new Object() {
            @Subscribe(deduplicate = true)
            public void onEvent(NullableName event) {
                received.add(event.name);
            }
        }, mode);

        // The posted event is asked whether it equals the previous one, so the broken equals() of the unnamed event
        // fails only its own post; asked of the previous event instead, it would be accepted and then reject every
        // later post.
        bus.post(new NullableName("alice")).post(new NullableName(null)).post(new NullableName("bob")).post(new NullableName("bob"));

        assertEquals(List.of("alice", "bob"), received);
    }

    private static final class NullableName {
        final String name;

        NullableName(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof NullableName event && name.equals(event.name); // throws if this name is null
        }

        @Override
        public int hashCode() {
            return 0;
        }
    }

    private static class ZeroIntervalSubscriber {
        final List<String> received;

        ZeroIntervalSubscriber(List<String> received) {
            this.received = received;
        }

        @Subscribe(intervalMillis = 0)
        public void onEvent(String event) {
            received.add(event);
        }
    }

    private static class NegativeIntervalSubscriber extends ZeroIntervalSubscriber {
        NegativeIntervalSubscriber(List<String> received) {
            super(received);
        }

        @Override
        @Subscribe(intervalMillis = -1)
        public void onEvent(String event) {
            received.add(event);
        }
    }
}
