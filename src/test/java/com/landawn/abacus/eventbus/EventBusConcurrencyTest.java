package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;

@Tag("unit")
@Timeout(30)
@ResourceLock("eventbus-logger")
public class EventBusConcurrencyTest extends TestBase {
    public static class A {
    }

    public static class B {
    }

    public static class CrossPosting {
        final EventBus bus;
        final A a = new A();
        final B b = new B();
        final CountDownLatch entered = new CountDownLatch(2);
        final AtomicInteger calls = new AtomicInteger();
        final AtomicBoolean coordinationFailed = new AtomicBoolean();

        CrossPosting(EventBus bus) {
            this.bus = bus;
        }

        void accept(Object opposite) throws InterruptedException {
            calls.incrementAndGet();
            entered.countDown();
            if (!entered.await(5, TimeUnit.SECONDS)) {
                coordinationFailed.set(true);
                return;
            }
            bus.post(opposite);
        }
    }

    public static class DeduplicatedCrossPosting extends CrossPosting {
        DeduplicatedCrossPosting(EventBus bus) {
            super(bus);
        }

        @Subscribe(deduplicate = true)
        public void a(A event) throws InterruptedException {
            accept(b);
        }

        @Subscribe(deduplicate = true)
        public void b(B event) throws InterruptedException {
            accept(a);
        }
    }

    public static class ThrottledCrossPosting extends CrossPosting {
        ThrottledCrossPosting(EventBus bus) {
            super(bus);
        }

        @Subscribe(intervalMillis = Long.MAX_VALUE)
        public void a(A event) throws InterruptedException {
            accept(b);
        }

        @Subscribe(intervalMillis = Long.MAX_VALUE)
        public void b(B event) throws InterruptedException {
            accept(a);
        }
    }

    // Daemon threads make the pre-fix deadlock a bounded test failure rather than a stuck test JVM.
    private static Thread start(Runnable action, AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> {
            try {
                action.run();
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        }, "eventbus-cross-post");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void finished(List<Thread> threads, AtomicReference<Throwable> failure) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        for (Thread thread : threads) {
            long remaining = deadline - System.nanoTime();
            if (remaining > 0) {
                thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            }
        }
        assertTrue(threads.stream().noneMatch(Thread::isAlive), "Posting threads must finish without a monitor deadlock");
        if (failure.get() != null) {
            fail("Concurrent worker failed", failure.get());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void crossPostingSuppressedEventsDoesNotDeadlock(boolean throttle) throws Exception {
        EventBus bus = EventBus.create();
        CrossPosting handler = throttle ? new ThrottledCrossPosting(bus) : new DeduplicatedCrossPosting(bus);
        bus.register(handler);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread first = start(() -> bus.post(handler.a), failure);
        Thread second = start(() -> bus.post(handler.b), failure);
        finished(List.of(first, second), failure);
        assertEquals(2, handler.calls.get());
        assertFalse(handler.coordinationFailed.get(), "Both handlers must enter concurrently");
    }

    public static class EqualityEvent {
        final String value;
        Runnable onCompare;

        EqualityEvent(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            if (onCompare != null) {
                Runnable callback = onCompare;
                onCompare = null;
                callback.run();
            }
            return other instanceof EqualityEvent event && getClass() == other.getClass() && value.equals(event.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public String toString() {
            return value;
        }
    }

    public static class EA extends EqualityEvent {
        EA(String value) {
            super(value);
        }
    }

    public static class EB extends EqualityEvent {
        EB(String value) {
            super(value);
        }
    }

    public static class EqualityHandlers {
        final AtomicInteger calls = new AtomicInteger();

        @Subscribe(deduplicate = true)
        public void a(EA event) {
            calls.incrementAndGet();
        }

        @Subscribe(deduplicate = true)
        public void b(EB event) {
            calls.incrementAndGet();
        }
    }

    @Test
    public void equalityCallbacksCanCrossPostWithoutHoldingHandlerMonitors() throws Exception {
        EventBus bus = EventBus.create();
        EqualityHandlers handler = new EqualityHandlers();
        bus.register(handler);
        EA a = new EA("");
        EB b = new EB("\u4e8b\u4ef6\ud83d\ude80");
        bus.post(a).post(b);
        CountDownLatch comparing = new CountDownLatch(2);
        CountDownLatch posted = new CountDownLatch(2);
        a.onCompare = () -> compareAndPost(comparing, posted, bus, b);
        b.onCompare = () -> compareAndPost(comparing, posted, bus, a);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread first = start(() -> bus.post(new EA("next")), failure);
        Thread second = start(() -> bus.post(new EB("next")), failure);
        finished(List.of(first, second), failure);
        assertEquals(4, handler.calls.get());
    }

    private static void compareAndPost(CountDownLatch comparing, CountDownLatch posted, EventBus bus, Object event) {
        comparing.countDown();
        try {
            assertTrue(comparing.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
        bus.post(event);
        posted.countDown();
        try {
            assertTrue(posted.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    public static class EqualityCollector {
        final List<EqualityEvent> events = new ArrayList<>();

        @Subscribe(deduplicate = true)
        public void on(EqualityEvent event) {
            events.add(event);
        }
    }

    @Test
    public void reentrantEqualityRevalidatesTheLatestReservation() {
        EventBus bus = EventBus.create();
        EqualityCollector handler = new EqualityCollector();
        bus.register(handler);
        EqualityEvent first = new EqualityEvent("");
        EqualityEvent next = new EqualityEvent("\u4e8b\u4ef6\ud83d\ude80");
        bus.post(first);
        first.onCompare = () -> bus.post(next);
        bus.post(next);
        assertEquals(List.of(first, next), handler.events);
    }

    public static class StringCollector {
        final List<String> events = Collections.synchronizedList(new ArrayList<>());

        @Subscribe(deduplicate = true)
        public void on(String event) {
            events.add(event);
        }
    }

    @Test
    public void concurrentEqualEventsAreReservedOnce() throws InterruptedException {
        EventBus bus = EventBus.create();
        StringCollector handler = new StringCollector();
        bus.register(handler);
        // Distinct instances ensure deduplication uses equality, not identity.
        runConcurrently(12, () -> bus.post(new String("\u4e8b\u4ef6\ud83d\ude80")));

        assertEquals(List.of("\u4e8b\u4ef6\ud83d\ude80"), handler.events);
        assertThrows(IllegalArgumentException.class, () -> bus.post((Object) null));
        bus.post("").post("");
        assertEquals(List.of("\u4e8b\u4ef6\ud83d\ude80", ""), handler.events);
    }

    public static class ReentrantHandler {
        final EventBus bus;
        final AtomicInteger calls = new AtomicInteger();
        final AtomicBoolean first = new AtomicBoolean(true);

        ReentrantHandler(EventBus bus) {
            this.bus = bus;
        }

        @Subscribe(deduplicate = true)
        public void on(String event) {
            calls.incrementAndGet();
            bus.post(event);
            if (first.getAndSet(false)) {
                bus.post("\ud83d\ude80");
            }
            throw new IllegalStateException("expected subscriber failure");
        }
    }

    @Test
    public void callbackReentrancyAndFailuresKeepReservedState() {
        EventBus bus = EventBus.create();
        ReentrantHandler handler = new ReentrantHandler(bus);
        bus.register(handler);
        assertDoesNotThrow(() -> bus.post(""));
        assertEquals(2, handler.calls.get());
        bus.post("\ud83d\ude80");
        assertEquals(2, handler.calls.get());
    }

    public static class DebugEvent {
        final EventBus bus;
        final Object monitor;
        final AtomicBoolean violation;
        final AtomicInteger strings;
        final AtomicInteger comparisons;
        final AtomicBoolean reposted = new AtomicBoolean();

        DebugEvent(EventBus bus, Object monitor, AtomicBoolean violation, AtomicInteger strings, AtomicInteger comparisons) {
            this.bus = bus;
            this.monitor = monitor;
            this.violation = violation;
            this.strings = strings;
            this.comparisons = comparisons;
        }

        @Override
        public boolean equals(Object other) {
            comparisons.incrementAndGet();
            if (Thread.holdsLock(monitor)) {
                violation.set(true);
            }
            return this == other;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }

        @Override
        public String toString() {
            strings.incrementAndGet();
            if (Thread.holdsLock(monitor)) {
                violation.set(true);
            }
            if (reposted.compareAndSet(false, true)) {
                bus.post(this);
            }
            return "\u4e8b\u4ef6\ud83d\ude80";
        }
    }

    public static class DebugHandler {
        Object monitor;
        final AtomicBoolean violation = new AtomicBoolean();
        int calls;

        @Subscribe(deduplicate = true)
        public void on(DebugEvent event) {
            if (Thread.holdsLock(monitor)) {
                violation.set(true);
            }
            calls++;
        }
    }

    @Test
    public void equalityFormattingAndInvocationRunOutsideTheMonitor() throws Exception {
        EventBus bus = EventBus.create();
        DebugHandler handler = new DebugHandler();
        bus.register(handler);
        var field = EventBus.class.getDeclaredField("registeredSubMap");
        field.setAccessible(true);
        handler.monitor = ((List<?>) ((Map<?, ?>) field.get(bus)).get(handler)).get(0);
        String name = EventBus.class.getName();
        var context = (org.apache.logging.log4j.core.LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        var config = context.getConfiguration();
        var old = config.getLoggers().get(name);
        config.removeLogger(name);
        config.addLogger(name, new org.apache.logging.log4j.core.config.LoggerConfig(name, org.apache.logging.log4j.Level.DEBUG, false));
        context.updateLoggers();
        try {
            AtomicInteger strings = new AtomicInteger();
            AtomicInteger comparisons = new AtomicInteger();
            bus.post(new DebugEvent(bus, handler.monitor, handler.violation, strings, comparisons));
            bus.post(new DebugEvent(bus, handler.monitor, handler.violation, strings, comparisons));
            assertTrue(strings.get() > 0, "Debug formatting must be exercised");
            assertTrue(comparisons.get() > 0);
            assertFalse(handler.violation.get(), "Application callbacks must not hold the filtering monitor");
            assertEquals(2, handler.calls, "Formatting reentry must see the event already reserved");
        } finally {
            config.removeLogger(name);
            if (old != null) {
                config.addLogger(name, old);
            }
            context.updateLoggers();
        }
    }

    @Test
    public void testUnregisterWaitsForConcurrentRegistrationToUpdateBothIndexes() throws Exception {
        final EventBus eventBus = EventBus.create();
        final RegistrationSubscriber subscriber = new RegistrationSubscriber();
        final java.lang.reflect.Field indexField = EventBus.class.getDeclaredField("registeredEventIdSubMap");
        indexField.setAccessible(true);
        final Object eventIdIndex = indexField.get(eventBus);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread registering = new Thread(() -> {
            try {
                eventBus.register(subscriber, "concurrent");
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        final Thread unregistering = new Thread(() -> {
            try {
                eventBus.unregister(subscriber);
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });

        registering.setDaemon(true);
        unregistering.setDaemon(true);
        try {
            synchronized (eventIdIndex) {
                // Pause registration after its main-registry update, before its event-ID update.
                registering.start();
                awaitBlockedThread(registering);
                assertEquals(List.of(subscriber), eventBus.allSubscribers());

                unregistering.start();
                awaitBlockedThread(unregistering);
                // Removal must wait for the pending registration transaction, otherwise its
                // later event-ID insertion can leave an unlisted subscriber receiving events.
                assertEquals(List.of(subscriber), eventBus.allSubscribers());
            }
        } finally {
            registering.join(5000);
            unregistering.join(5000);
        }

        assertFalse(registering.isAlive());
        assertFalse(unregistering.isAlive());
        assertNull(failure.get());
        assertTrue(eventBus.allSubscribers().isEmpty());
        eventBus.post("concurrent", "after removal");
        assertNull(subscriber.lastEvent);
    }

    private static void awaitBlockedThread(final Thread thread) throws InterruptedException {
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.BLOCKED && thread.isAlive() && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(Thread.State.BLOCKED, thread.getState());
    }

    public static class RegistrationSubscriber {
        String lastEvent;

        @Subscribe
        public void onEvent(String event) {
            lastEvent = event;
        }
    }
}
