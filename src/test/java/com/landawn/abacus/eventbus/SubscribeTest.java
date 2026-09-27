package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class SubscribeTest extends TestBase {

    public static class TestHandler {
        @Subscribe
        public void handleDefault(String event) {
        }

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
        public void handleAsync(String event) {
        }

        @Subscribe(eventId = "myEvents")
        public void handleWithEventId(String event) {
        }

        @Subscribe(sticky = true)
        public void handleSticky(String event) {
        }

        @Subscribe(strictEventType = true)
        public void handleStrict(String event) {
        }

        @Subscribe(intervalMillis = 1000)
        public void handleWithInterval(String event) {
        }

        @Subscribe(deduplicate = true)
        public void handleDeduplicate(String event) {
        }

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, eventId = "testId", sticky = true, strictEventType = true, intervalMillis = 5000, deduplicate = true)
        public void handleAll(String event) {
        }
    }

    @Test
    public void testDefaultValues() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleDefault", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertNotNull(annotation);
        assertEquals(ThreadMode.DEFAULT, annotation.threadMode());
        assertFalse(annotation.strictEventType());
        assertFalse(annotation.sticky());
        assertEquals("", annotation.eventId());
        assertEquals(0, annotation.intervalMillis());
        assertFalse(annotation.deduplicate());
    }

    @Test
    public void testThreadMode() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleAsync", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertEquals(ThreadMode.THREAD_POOL_EXECUTOR, annotation.threadMode());
    }

    @Test
    public void testAllAttributes() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleAll", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertNotNull(annotation);
        assertEquals(ThreadMode.THREAD_POOL_EXECUTOR, annotation.threadMode());
        assertEquals("testId", annotation.eventId());
        assertTrue(annotation.sticky());
        assertTrue(annotation.strictEventType());
        assertEquals(5000, annotation.intervalMillis());
        assertTrue(annotation.deduplicate());
    }

    @Test
    public void testThreadModeAttribute() throws InterruptedException {
        EventBus eventBus = EventBus.create();
        AtomicReference<Thread> executionThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        class TestClass {
            @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
            public void onEvent(String event) {
                executionThread.set(Thread.currentThread());
                latch.countDown();
            }
        }

        TestClass subscriber = new TestClass();
        eventBus.register(subscriber);
        eventBus.post("test");

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Async subscriber did not receive the event");
        assertNotSame(Thread.currentThread(), executionThread.get());
    }

    @Test
    public void testStrictEventTypeAttribute() {
        EventBus eventBus = EventBus.create();
        AtomicInteger normalCount = new AtomicInteger();
        AtomicInteger strictCount = new AtomicInteger();

        class TestClass {
            @Subscribe
            public void onNormalEvent(CharSequence event) {
                normalCount.incrementAndGet();
            }

            @Subscribe(strictEventType = true)
            public void onStrictEvent(CharSequence event) {
                strictCount.incrementAndGet();
            }
        }

        TestClass subscriber = new TestClass();
        eventBus.register(subscriber);

        eventBus.post("String implements CharSequence");
        eventBus.post(new StringBuilder("StringBuilder implements CharSequence"));

        assertEquals(2, normalCount.get());
        assertEquals(0, strictCount.get());
    }

    @Test
    public void testStrictEventType() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleStrict", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertTrue(annotation.strictEventType());
    }

    @Test
    public void testStickyAttribute() {
        EventBus eventBus = EventBus.create();
        AtomicReference<String> receivedEvent = new AtomicReference<>();

        eventBus.postSticky("Sticky Event");

        class TestClass {
            @Subscribe(sticky = true)
            public void onEvent(String event) {
                receivedEvent.set(event);
            }
        }

        TestClass subscriber = new TestClass();
        eventBus.register(subscriber);

        assertEquals("Sticky Event", receivedEvent.get());
    }

    @Test
    public void testSticky() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleSticky", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertTrue(annotation.sticky());
    }

    @Test
    public void testEventIdAttribute() {
        EventBus eventBus = EventBus.create();
        AtomicInteger event1Count = new AtomicInteger();
        AtomicInteger event2Count = new AtomicInteger();

        class TestClass {
            @Subscribe(eventId = "event1")
            public void onEvent1(String event) {
                event1Count.incrementAndGet();
            }

            @Subscribe(eventId = "event2")
            public void onEvent2(String event) {
                event2Count.incrementAndGet();
            }
        }

        TestClass subscriber = new TestClass();
        eventBus.register(subscriber);

        eventBus.post("event1", "Message 1");
        eventBus.post("event2", "Message 2");
        eventBus.post("No event ID");

        assertEquals(1, event1Count.get());
        assertEquals(1, event2Count.get());
    }

    @Test
    public void testEventId() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleWithEventId", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertEquals("myEvents", annotation.eventId());
    }

    @Test
    public void testInterval() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleWithInterval", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertEquals(1000, annotation.intervalMillis());
    }

    @Test
    public void testDeduplicateAttribute() {
        EventBus eventBus = EventBus.create();
        AtomicInteger eventCount = new AtomicInteger();
        AtomicReference<String> lastEvent = new AtomicReference<>();

        class TestClass {
            @Subscribe(deduplicate = true)
            public void onEvent(String event) {
                eventCount.incrementAndGet();
                lastEvent.set(event);
            }
        }

        TestClass subscriber = new TestClass();
        eventBus.register(subscriber);

        eventBus.post("Event A");
        eventBus.post("Event A");
        eventBus.post("Event B");
        eventBus.post("Event B");
        eventBus.post("Event A");

        assertEquals(3, eventCount.get());
        assertEquals("Event A", lastEvent.get());
    }

    @Test
    public void testDeduplicate() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleDeduplicate", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertTrue(annotation.deduplicate());
    }

    @Test
    public void testMethodAnnotation() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleDefault", String.class);
        assertTrue(method.isAnnotationPresent(Subscribe.class));
    }

    @Test
    public void testRetentionPolicy() {
        Retention retention = Subscribe.class.getAnnotation(Retention.class);
        assertNotNull(retention);
        assertEquals(RetentionPolicy.RUNTIME, retention.value());
    }

    @Test
    public void testTargetElements() {
        Target target = Subscribe.class.getAnnotation(Target.class);
        assertNotNull(target);
        assertArrayEquals(new ElementType[] { ElementType.METHOD }, target.value());
    }

    @Test
    public void testIsAnnotation() {
        assertTrue(Subscribe.class.isAnnotation());
    }

    @Test
    public void testAnnotationType() throws NoSuchMethodException {
        Method method = TestHandler.class.getDeclaredMethod("handleDefault", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertNotNull(annotation);
        assertEquals(Subscribe.class, annotation.annotationType());
    }

    @Test
    public void testIntervalAlwaysAllowsFirstEvent() {
        EventBus eventBus = EventBus.create();
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

    @Test
    public void testMultipleAttributesCombined() {
        List<Runnable> queued = new ArrayList<>();
        EventBus eventBus = EventBus.create("combined-attributes", queued::add);
        List<String> received = new ArrayList<>();
        class Handler {
            @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, eventId = "combined", sticky = true,
                    strictEventType = true, deduplicate = true)
            public void onEvent(String event) {
                received.add(event);
            }
        }
        eventBus.postSticky("combined", "retained");
        eventBus.register(new Handler());
        eventBus.post("wrong-id", "ignored");
        eventBus.post("combined", new StringBuilder("wrong type"));
        eventBus.post("combined", "retained");
        eventBus.post("combined", "next");
        eventBus.post("combined", "next");

        assertTrue(received.isEmpty(), "Async callbacks must wait for the executor");
        assertEquals(2, queued.size(), "Sticky replay and one distinct live event should be queued");
        queued.forEach(Runnable::run);
        assertEquals(List.of("retained", "next"), received);
    }

    @Test
    public void testSubscriberMethodRequiresExactlyOneParameter() {
        EventBus eventBus = EventBus.create();
        class NoParameters {
            @Subscribe
            public void onEvent() {
            }
        }
        class TwoParameters {
            @Subscribe
            public void onEvent(String first, String second) {
            }
        }

        RuntimeException noParameters = assertThrows(RuntimeException.class, () -> eventBus.register(new NoParameters()));
        assertTrue(noParameters.getMessage().contains("0 parameters"), noParameters.getMessage());
        RuntimeException twoParameters = assertThrows(RuntimeException.class, () -> eventBus.register(new TwoParameters()));
        assertTrue(twoParameters.getMessage().contains("2 parameters"), twoParameters.getMessage());
        assertEquals(0, eventBus.countOfSubscribers(), "Invalid methods must not leave partial registrations");
    }

    @Test
    public void testAnnotationOnValidMethod() {
        EventBus eventBus = EventBus.create();
        AtomicReference<String> received = new AtomicReference<>();

        class ValidSubscriber {
            @Subscribe
            public void validMethod(String event) {
                received.set(event);
            }
        }

        ValidSubscriber subscriber = new ValidSubscriber();
        eventBus.register(subscriber);
        eventBus.post("Test Event");

        assertEquals("Test Event", received.get());
    }

}
