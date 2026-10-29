package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
@ResourceLock("eventbus-logger")
public class EventBusDiagnosticsTest extends TestBase {

    @Test
    void register_whenStickyReplayDiagnosticsFail_keepsRegistrationUsable() {
        withLogs(Level.INFO, logs -> {
            EventBus bus = EventBus.create("sticky", task -> {
                throw new RejectedExecutionException("expected rejection");
            });
            ThrowingText event = new ThrowingText();
            bus.postSticky("id", event);
            assertDoesNotThrow(() -> bus.register(new Collector(), "id", ThreadMode.THREAD_POOL_EXECUTOR));
            assertDoesNotThrow(() -> bus.register(new BrokenHandlerText(), "id"));
            Collector healthy = new Collector();
            assertDoesNotThrow(() -> bus.register(healthy, "id"));
            assertEquals(1, healthy.events.size());
            assertSame(event, healthy.events.get(0));
            assertTrue(logs.stream().anyMatch(log -> log.getMessage().getFormattedMessage().startsWith("Failed to post sticky event:")));
        });
    }

    @Test
    void post_whenEventDescriptionThrows_preservesFailureAndHealthyDelivery() {
        withLogs(Level.INFO, logs -> {
            EventBus bus = EventBus.create();
            Collector healthy = new Collector();
            bus.register(new FailingHandler(), "\u4e8b\u4ef6\ud83d\ude80").register(healthy, "\u4e8b\u4ef6\ud83d\ude80");
            ThrowingText event = new ThrowingText();
            assertDoesNotThrow(() -> bus.post("\u4e8b\u4ef6\ud83d\ude80", event));
            assertEquals(1, healthy.events.size());
            assertSame(event, healthy.events.get(0));
            assertTrue(logs.stream().anyMatch(log -> log.getMessage().getFormattedMessage().contains(ThrowingText.class.getName() + "@")));
            assertTrue(logs.stream()
                    .anyMatch(log -> log.getThrown() != null && log.getThrown().getCause() != null
                            && "original handler failure".equals(log.getThrown().getCause().getMessage())));
        });
    }

    @Test
    void post_whenFailedEventDescriptionOverflows_continuesHealthyDelivery() {
        withLogs(Level.INFO, logs -> {
            EventBus bus = EventBus.create();
            Collector healthy = new Collector();
            bus.register(new FailingHandler(), "cyclic").register(healthy, "cyclic");
            List<Object> cyclic = new ArrayList<>();
            cyclic.add(cyclic); // describing a self-containing collection overflows the stack

            assertDoesNotThrow(() -> bus.post("cyclic", cyclic));

            assertEquals(1, healthy.events.size());
            assertSame(cyclic, healthy.events.get(0));
            assertTrue(logs.stream().anyMatch(log -> log.getMessage().getFormattedMessage().contains(ArrayList.class.getName() + "@")));
        });
    }

    @Test
    void post_whenDebugEventDescriptionOverflows_stillDeliversEvent() {
        withLogs(Level.DEBUG, logs -> {
            EventBus bus = EventBus.create();
            Collector healthy = new Collector();
            bus.register(healthy);
            List<Object> cyclic = new ArrayList<>();
            cyclic.add(cyclic);

            assertDoesNotThrow(() -> bus.post(cyclic));

            assertEquals(1, healthy.events.size());
            assertSame(cyclic, healthy.events.get(0));
        });
    }

    @Test
    void post_whenSubscriberDescriptionThrows_preservesEmptyAndUnicodeDelivery() {
        withLogs(Level.INFO, logs -> {
            EventBus bus = EventBus.create();
            Collector healthy = new Collector();
            bus.register(new BrokenHandlerText()).register(healthy);
            assertDoesNotThrow(() -> bus.post("").post("\ud83d\ude80"));
            assertEquals(List.of("", "\ud83d\ude80"), healthy.events);
            assertTrue(logs.stream().anyMatch(log -> log.getMessage().getFormattedMessage().contains(BrokenHandlerText.class.getName() + "@")));
        });
    }

    @Test
    void post_whenExecutorRejects_logsFailureAndContinuesSynchronousDelivery() {
        withLogs(Level.INFO, logs -> {
            EventBus bus = EventBus.create("reject", task -> {
                throw new RejectedExecutionException("expected rejection");
            });
            Collector healthy = new Collector();
            bus.register(new Collector(), "id", ThreadMode.THREAD_POOL_EXECUTOR).register(healthy, "id");
            ThrowingText event = new ThrowingText();
            assertDoesNotThrow(() -> bus.post("id", event));
            assertEquals(1, healthy.events.size());
            assertSame(event, healthy.events.get(0));
            assertTrue(logs.stream().anyMatch(log -> log.getThrown() instanceof RejectedExecutionException));
        });
    }

    @ParameterizedTest
    @EnumSource(value = ThreadMode.class, names = { "DEFAULT", "THREAD_POOL_EXECUTOR" })
    void post_whenEqualityThrows_logsFailureAndAllowsRetry(ThreadMode mode) {
        withLogs(Level.INFO, logs -> {
            List<Runnable> queued = new ArrayList<>();
            EventBus bus = EventBus.create("broken-equality", queued::add);
            List<BrokenEquality> filtered = new ArrayList<>();
            Object subscriber = new Object() {
                @Subscribe(deduplicate = true)
                public void onEvent(BrokenEquality event) {
                    filtered.add(event);
                }
            };
            Collector healthy = new Collector();
            bus.register(subscriber, mode).register(healthy);
            BrokenEquality first = new BrokenEquality("first");
            BrokenEquality next = new BrokenEquality("next");
            bus.post(first);
            queued.forEach(Runnable::run);
            queued.clear();

            assertDoesNotThrow(() -> bus.post(next));

            assertTrue(queued.isEmpty(), "A failed equality check must not enqueue a callback");
            assertEquals(1, filtered.size());
            assertSame(first, filtered.get(0));
            assertEquals(2, healthy.events.size());
            assertSame(next, healthy.events.get(1));
            assertTrue(logs.stream().anyMatch(log -> log.getThrown() == first.failure));

            first.fail = false;
            bus.post(next).post(next);
            queued.forEach(Runnable::run);
            assertEquals(2, filtered.size(), "Failed filtering must leave the previous accepted event intact");
            assertSame(next, filtered.get(1));
            assertEquals(4, healthy.events.size());
        });
    }

    @Test
    void diagnostics_whenIntervalSuppressesEvent_logsSafelyWithDeduplicationEnabled() {
        withLogs(Level.DEBUG, logs -> {
            EventBus bus = EventBus.create();
            List<Object> received = new ArrayList<>();
            Object subscriber = new Object() {
                @Subscribe(intervalMillis = Long.MAX_VALUE, deduplicate = true)
                public void onEvent(Object event) {
                    received.add(event);
                }

                @Override
                public String toString() {
                    throw new IllegalStateException("broken subscriber diagnostic");
                }
            };
            bus.register(subscriber);
            ThrowingText first = new ThrowingText();
            ThrowingText suppressed = new ThrowingText();

            assertDoesNotThrow(() -> bus.post(first).post(suppressed));

            assertEquals(1, received.size());
            assertSame(first, received.get(0));
            assertTrue(logs.stream()
                    .anyMatch(log -> log.getLevel() == Level.DEBUG && log.getMessage().getFormattedMessage().contains("because it is within the interval:")
                            && log.getMessage().getFormattedMessage().contains(ThrowingText.class.getName() + "@")));
        });
    }

    @Test
    void diagnostics_whenDebugDescriptionsThrow_preservesSubscriberLifecycle() {
        withLogs(Level.DEBUG, logs -> {
            EventBus bus = EventBus.create();
            BrokenCollectorText handler = new BrokenCollectorText();
            assertDoesNotThrow(() -> bus.register(handler));
            ThrowingText event = new ThrowingText();
            assertDoesNotThrow(() -> bus.post((String) null, event).post("", "\ud83d\ude80").post(""));
            assertEquals(3, handler.events.size());
            assertSame(event, handler.events.get(0));
            assertEquals(List.of("\ud83d\ude80", ""), handler.events.subList(1, 3));
            assertThrows(IllegalArgumentException.class, () -> bus.post((Object) null));
            assertDoesNotThrow(() -> bus.unregister(handler));
            assertEquals(0, bus.countOfSubscribers());
            assertTrue(logs.stream().anyMatch(log -> log.getMessage().getFormattedMessage().contains("Posting event: \ud83d\ude80")));
            assertTrue(logs.stream().anyMatch(log -> log.getMessage().getFormattedMessage().startsWith("Unregistering subscriber:")));
        });
    }

    private static final class BrokenEquality {
        final String value;
        final IllegalStateException failure = new IllegalStateException("broken event equality");
        boolean fail = true;

        BrokenEquality(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object other) {
            if (fail) {
                throw failure;
            }
            return other instanceof BrokenEquality event && value.equals(event.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }

    public static class ThrowingText {
        @Override
        public String toString() {
            throw new IllegalStateException("broken event diagnostic");
        }
    }

    public static class FailingHandler {
        @Subscribe(sticky = true)
        public void on(Object event) {
            throw new IllegalArgumentException("original handler failure");
        }
    }

    public static class BrokenHandlerText extends FailingHandler {
        @Override
        public String toString() {
            throw new IllegalStateException("broken subscriber diagnostic");
        }
    }

    public static class Collector {
        final List<Object> events = new ArrayList<>();

        @Subscribe(sticky = true)
        public void on(Object event) {
            events.add(event);
        }
    }

    public static class BrokenCollectorText extends Collector {
        @Override
        public String toString() {
            throw new IllegalStateException("broken subscriber diagnostic");
        }
    }

    private static void withLogs(Level level, Consumer<List<LogEvent>> test) {
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        var configuration = context.getConfiguration();
        String name = EventBus.class.getName();
        LoggerConfig previous = configuration.getLoggers().get(name);
        List<LogEvent> events = new ArrayList<>();
        AbstractAppender appender = new AbstractAppender("eventbus-test-log-capture", null, null, false, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                events.add(event.toImmutable());
            }
        };
        appender.start();
        LoggerConfig config = new LoggerConfig(name, level, false);
        config.addAppender(appender, null, null);
        configuration.removeLogger(name);
        configuration.addLogger(name, config);
        context.updateLoggers();
        try {
            test.accept(events);
        } finally {
            configuration.removeLogger(name);
            if (previous != null) {
                configuration.addLogger(name, previous);
            }
            context.updateLoggers();
            appender.stop();
        }
    }
}
