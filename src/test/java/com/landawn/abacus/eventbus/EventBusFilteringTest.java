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
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;

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
