package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class EventBusTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    public void setUp() {
        eventBus = EventBus.create();
    }

    public static class TestHandler {
        String lastEvent;

        @Subscribe
        public void onEvent(String event) {
            this.lastEvent = event;
        }
    }

    static final class EqualSubscriber {
        final String id;
        final List<String> events = new ArrayList<>();

        EqualSubscriber(String id) {
            this.id = id;
        }

        @Subscribe
        public void onEvent(String event) {
            events.add(event);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof EqualSubscriber other && id.equals(other.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
    }

    public static class BaseOverrideSubscriber {
        @Subscribe(eventId = "base")
        public void onEvent(String event) {
            throw new AssertionError("Base subscriber metadata should be overridden");
        }
    }

    static final class ChildOverrideSubscriber extends BaseOverrideSubscriber {
        final List<String> events = new ArrayList<>();

        @Override
        @Subscribe(eventId = "child")
        public void onEvent(String event) {
            events.add(event);
        }
    }

    public static class TestSubscriber {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class StaticAnnotatedSubscriber {
        @Subscribe
        public static void onEvent(String event) {
            // static method - should be rejected
        }
    }

    public static class InheritedSubscriberParent {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public static class InheritedSubscriberChild extends InheritedSubscriberParent {
    }

    public static class MultiMethodSubscriber {
        int stringCount;
        int integerCount;
        int doubleCount;

        @Subscribe
        public void onString(String event) {
            stringCount++;
        }

        @Subscribe
        public void onInteger(Integer event) {
            integerCount++;
        }

        @Subscribe
        public void onDouble(Double event) {
            doubleCount++;
        }
    }

    public static class BaseEvent {
    }

    public static class SubEvent extends BaseEvent {
    }

    public static class SubSubEvent extends SubEvent {
    }

    public static class HierarchySubscriber {
        int baseEventCount;
        int subEventCount;
        int subSubEventCount;

        @Subscribe
        public void onBaseEvent(BaseEvent event) {
            baseEventCount++;
        }

        @Subscribe
        public void onSubEvent(SubEvent event) {
            subEventCount++;
        }

        @Subscribe
        public void onSubSubEvent(SubSubEvent event) {
            subSubEventCount++;
        }
    }

    // ---- getDefault ----

    @Test
    public void testGetDefault() {
        EventBus defaultBus = EventBus.getDefault();
        assertNotNull(defaultBus);
        assertEquals("default", defaultBus.identifier());
        assertSame(defaultBus, EventBus.getDefault());
    }

    @Test
    public void testCreateTwoBusesHaveDifferentIdentifiers() {
        EventBus bus1 = EventBus.create();
        EventBus bus2 = EventBus.create();
        assertNotEquals(bus1.identifier(), bus2.identifier());
    }

    // ---- create ----

    @Test
    public void testCreate() {
        EventBus bus = EventBus.create();
        assertNotNull(bus);
        assertNotNull(bus.identifier());
        assertFalse(bus.identifier().isEmpty());
    }

    @Test
    public void testCreateWithNullExecutor() {
        assertThrows(IllegalArgumentException.class, () -> EventBus.create("testBus", null));
    }

    @Test
    public void testCreateWithActualExecutor() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            EventBus bus = EventBus.create("executorBus", executor);
            assertEquals("executorBus", bus.identifier());

            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Thread> eventThread = new AtomicReference<>();

            Object subscriber = new Object() {
                @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
                public void onEvent(String event) {
                    eventThread.set(Thread.currentThread());
                    latch.countDown();
                }
            };

            bus.register(subscriber);
            bus.post("test");

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertNotEquals(Thread.currentThread(), eventThread.get());
            bus.unregister(subscriber);
        } finally {
            executor.shutdownNow();
        }
    }

    // ---- identifier ----

    @Test
    public void testIdentifier() {
        EventBus bus = EventBus.create("myIdentifier");
        assertEquals("myIdentifier", bus.identifier());
    }

    @Test
    public void testSubscribers_TypeHierarchy() {
        HierarchySubscriber subscriber = new HierarchySubscriber();
        eventBus.register(subscriber);

        // SubEvent extends BaseEvent, so subscriber should appear for SubEvent (because onBaseEvent accepts it)
        List<Object> baseSubs = eventBus.subscribers(BaseEvent.class);
        assertTrue(baseSubs.contains(subscriber));

        List<Object> subSubs = eventBus.subscribers(SubEvent.class);
        assertTrue(subSubs.contains(subscriber));

        eventBus.unregister(subscriber);
    }

    // ---- subscribers(String, Class) ----

    @Test
    public void testGetSubscribers() {
        Subscriber<String> subscriber = event -> {
        };
        eventBus.register(subscriber, "testId");

        List<Object> subscribers = eventBus.subscribers("testId", String.class);
        assertTrue(subscribers.contains(subscriber));

        eventBus.unregister(subscriber);
    }

    @Test
    public void testSubscribersTreatsEmptyEventIdAsNoEventId() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        assertTrue(eventBus.subscribers("", String.class).contains(subscriber));

        eventBus.unregister(subscriber);
    }

    // ---- subscribers(Class) ----

    @Test
    public void testSubscribers() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        List<Object> subscribers = eventBus.subscribers(String.class);
        assertEquals(1, subscribers.size());
        assertTrue(subscribers.contains(subscriber));

        List<Object> noSubscribers = eventBus.subscribers(Integer.class);
        assertTrue(noSubscribers.isEmpty());

        eventBus.unregister(subscriber);
    }

    @Test
    public void testSubscribers_EmptyBus() {
        List<Object> subscribers = eventBus.subscribers(String.class);
        assertNotNull(subscribers);
        assertTrue(subscribers.isEmpty());
    }

    @Test
    public void testSubscribersWithEventId_NonExistentId() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber, "myId");

        List<Object> result = eventBus.subscribers("nonExistent", String.class);
        assertTrue(result.isEmpty());

        eventBus.unregister(subscriber);
    }

    // ---- allSubscribers ----

    @Test
    public void testGetAllSubscribers() {
        Subscriber<String> subscriber1 = event -> {
        };
        Subscriber<Integer> subscriber2 = event -> {
        };

        eventBus.register(subscriber1, "id1");
        eventBus.register(subscriber2, "id2");

        List<Object> allSubscribers = eventBus.allSubscribers();
        assertEquals(2, allSubscribers.size());
        assertTrue(allSubscribers.contains(subscriber1));
        assertTrue(allSubscribers.contains(subscriber2));

        eventBus.unregister(subscriber1);
        eventBus.unregister(subscriber2);
    }

    @Test
    public void testAllSubscribers_EmptyBus() {
        List<Object> all = eventBus.allSubscribers();
        assertNotNull(all);
        assertTrue(all.isEmpty());
    }

    @Test
    public void testGetSubscribersWithEventId() {
        TestSubscriber subscriber1 = new TestSubscriber();
        TestSubscriber subscriber2 = new TestSubscriber();

        eventBus.register(subscriber1, "event1");
        eventBus.register(subscriber2, "event2");

        List<Object> subscribers = eventBus.subscribers("event1", String.class);
        assertEquals(1, subscribers.size());
        assertTrue(subscribers.contains(subscriber1));

        subscribers = eventBus.subscribers("event2", String.class);
        assertEquals(1, subscribers.size());
        assertTrue(subscribers.contains(subscriber2));
    }

    @Test
    public void testRegisterWithAnnotation() {
        TestHandler handler = new TestHandler();
        eventBus.register(handler);
        eventBus.post("test event");

        assertEquals("test event", handler.lastEvent);
    }

    @Test
    public void testRegisterSubscriberWithInheritedOnMethod() {
        InheritedSubscriberChild subscriber = new InheritedSubscriberChild();

        eventBus.register(subscriber, "inherited");
        eventBus.post("inherited", "inherited-event");

        assertEquals(List.of("inherited-event"), subscriber.receivedEvents);
    }

    @Test
    public void testSubclassOverrideKeepsSubclassSubscriptionMetadata() {
        ChildOverrideSubscriber subscriber = new ChildOverrideSubscriber();

        eventBus.register(subscriber);
        eventBus.post("base", "ignored");
        eventBus.post("child", "handled");

        assertEquals(List.of("handled"), subscriber.events);
    }

    @Test
    public void testRegister_ReRegistrationReplaces() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber, "id1");
        eventBus.register(subscriber, "id2");

        // After re-registration, subscriber should be under id2 now
        assertEquals(1, eventBus.allSubscribers().size());

        eventBus.post("id1", "msg1");
        eventBus.post("id2", "msg2");

        // Should only receive msg2 since re-registered under id2
        assertEquals(List.of("msg2"), subscriber.receivedEvents);

        eventBus.unregister(subscriber);
    }

    // ---- register(Subscriber, String) ----

    @Test
    public void testRegisterLambdaSubscriber() {
        AtomicReference<String> received = new AtomicReference<>();
        Subscriber<String> subscriber = event -> received.set(event);

        eventBus.register(subscriber, "lambdaEvent");
        eventBus.post("lambdaEvent", "Hello Lambda");

        assertEquals("Hello Lambda", received.get());
    }

    // ---- register(Subscriber, String, ThreadMode) ----

    @Test
    public void testRegisterLambdaSubscriberWithThreadMode() {
        AtomicReference<String> received = new AtomicReference<>();
        Subscriber<String> subscriber = event -> received.set(event);

        eventBus.register(subscriber, "lambdaEvent", ThreadMode.DEFAULT);
        eventBus.post("lambdaEvent", "Hello Lambda");

        assertEquals("Hello Lambda", received.get());
    }

    @Test
    public void testEventHierarchy() {
        HierarchySubscriber subscriber = new HierarchySubscriber();
        eventBus.register(subscriber);

        eventBus.post(new BaseEvent());
        eventBus.post(new SubEvent());
        eventBus.post(new SubSubEvent());

        assertEquals(3, subscriber.baseEventCount);
        assertEquals(2, subscriber.subEventCount);
        assertEquals(1, subscriber.subSubEventCount);
    }

    @Test
    public void testStrictEventType() {
        AtomicInteger baseEventCount = new AtomicInteger();
        AtomicInteger strictEventCount = new AtomicInteger();

        Object subscriber = new Object() {
            @Subscribe
            public void onBaseEvent(BaseEvent event) {
                baseEventCount.incrementAndGet();
            }

            @Subscribe(strictEventType = true)
            public void onStrictBaseEvent(BaseEvent event) {
                strictEventCount.incrementAndGet();
            }
        };

        eventBus.register(subscriber);
        eventBus.post(new BaseEvent());
        eventBus.post(new SubEvent());

        assertEquals(2, baseEventCount.get());
        assertEquals(1, strictEventCount.get());
    }

    @Test
    public void testDeduplicate() {
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
    public void testDeduplicate_FirstEventAlwaysDelivered() {
        List<String> received = new ArrayList<>();
        Object subscriber = new Object() {
            @Subscribe(deduplicate = true)
            public void onEvent(String event) {
                received.add(event);
            }
        };

        eventBus.register(subscriber);
        eventBus.post("only");

        assertEquals(List.of("only"), received);

        eventBus.unregister(subscriber);
    }

    @Test
    public void testEventIdFiltering() {
        AtomicInteger count = new AtomicInteger();
        Object handler = new Object() {
            @Subscribe(eventId = "specific")
            public void handle(String event) {
                count.incrementAndGet();
            }
        };

        eventBus.register(handler);
        eventBus.post("specific", "match");
        eventBus.post("other", "no match");
        eventBus.post("match without ID");

        assertEquals(1, count.get());
        eventBus.unregister(handler);
    }

    // ---- register(Object) ----

    @Test
    public void testRegister() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber);

        assertSame(eventBus, result);
        assertEquals(1, eventBus.allSubscribers().size());
    }

    @Test
    public void testEqualSubscribersAreTrackedByIdentity() {
        EqualSubscriber first = new EqualSubscriber("same");
        EqualSubscriber second = new EqualSubscriber("same");

        eventBus.register(first);
        eventBus.register(second);
        eventBus.post("event");

        assertEquals(List.of("event"), first.events);
        assertEquals(List.of("event"), second.events);
        assertEquals(2, eventBus.allSubscribers().size());

        eventBus.unregister(first);
        eventBus.post("event2");

        assertEquals(List.of("event"), first.events);
        assertEquals(List.of("event", "event2"), second.events);
    }

    @Test
    public void testRegisterMultipleSubscribersForSameEventType() {
        TestSubscriber sub1 = new TestSubscriber();
        TestSubscriber sub2 = new TestSubscriber();

        eventBus.register(sub1);
        eventBus.register(sub2);
        eventBus.post("broadcast");

        assertEquals(List.of("broadcast"), sub1.receivedEvents);
        assertEquals(List.of("broadcast"), sub2.receivedEvents);

        eventBus.unregister(sub1);
        eventBus.unregister(sub2);
    }

    // ---- register(Object, String) ----

    @Test
    public void testRegisterWithEventId() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber, "testEvent");

        assertSame(eventBus, result);
        assertEquals(1, eventBus.subscribers("testEvent", String.class).size());
    }

    @Test
    public void testTypeQueriesRejectNullDeterministically() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.subscribers((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.subscribers(null, (Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.stickyEvents((Class<Object>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.stickyEvents(null, (Class<Object>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.removeStickyEvents((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.removeStickyEvents(null, (Class<?>) null));

        eventBus.register(new TestSubscriber());
        eventBus.postSticky("sticky");

        assertThrows(IllegalArgumentException.class, () -> eventBus.subscribers((Class<?>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.stickyEvents((Class<Object>) null));
        assertThrows(IllegalArgumentException.class, () -> eventBus.removeStickyEvents((Class<?>) null));
    }

    // ---- register(Object, ThreadMode) ----

    @Test
    public void testRegisterWithThreadMode() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber, ThreadMode.DEFAULT);

        assertSame(eventBus, result);
        assertEquals(1, eventBus.allSubscribers().size());
    }

    // ---- register(Object, String, ThreadMode) ----

    @Test
    public void testRegisterWithEventIdAndThreadMode() {
        TestSubscriber subscriber = new TestSubscriber();
        EventBus result = eventBus.register(subscriber, "testEvent", ThreadMode.DEFAULT);

        assertSame(eventBus, result);
        assertEquals(1, eventBus.subscribers("testEvent", String.class).size());
    }

    @Test
    public void testMultipleAnnotatedMethods() {
        MultiMethodSubscriber subscriber = new MultiMethodSubscriber();
        eventBus.register(subscriber);

        eventBus.post("String Event");
        eventBus.post(123);
        eventBus.post(45.67);

        assertEquals(1, subscriber.stringCount);
        assertEquals(1, subscriber.integerCount);
        assertEquals(1, subscriber.doubleCount);
    }

    @Test
    public void testRegisterThrowsExceptionForNoSubscriberMethods() {
        Object noMethodSubscriber = new Object();

        assertThrows(IllegalArgumentException.class, () -> eventBus.register(noMethodSubscriber));
    }

    @Test
    public void testRegisterRejectsStaticAnnotatedSubscriberMethod() {
        assertThrows(RuntimeException.class, () -> eventBus.register(new StaticAnnotatedSubscriber()));
    }

    @Test
    public void testRegister_NullSubscriber() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.register(null));
    }

    @Test
    public void testRegisterThrowsExceptionForLambdaWithoutEventId() {
        Subscriber<Object> generalSubscriber = event -> {
        };

        assertThrows(IllegalStateException.class, () -> eventBus.register(generalSubscriber));
    }

    @Test
    public void testRegisterLambdaSubscriberWithThreadPoolMode() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> received = new AtomicReference<>();

        Subscriber<String> subscriber = event -> {
            received.set(event);
            latch.countDown();
        };

        eventBus.register(subscriber, "asyncLambda", ThreadMode.THREAD_POOL_EXECUTOR);
        eventBus.post("asyncLambda", "async value");

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals("async value", received.get());

        eventBus.unregister(subscriber);
    }

    @Test
    public void testEventIsDeliveredAfterIntervalExpires() throws InterruptedException {
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
    public void testThreadPoolExecutorMode() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Thread> eventThread = new AtomicReference<>();

        Object subscriber = new Object() {
            @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
            public void onEvent(String event) {
                eventThread.set(Thread.currentThread());
                latch.countDown();
            }
        };

        eventBus.register(subscriber);
        eventBus.post("Test");

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotEquals(Thread.currentThread(), eventThread.get());
    }

    @Test
    public void testUnregister_VerifiesNoEventsAfter() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        eventBus.post("before");
        assertEquals(1, subscriber.receivedEvents.size());

        eventBus.unregister(subscriber);
        eventBus.post("after");

        // Should still have only the "before" event
        assertEquals(List.of("before"), subscriber.receivedEvents);
    }

    // ---- unregister ----

    @Test
    public void testUnregister() {
        AtomicReference<String> result = new AtomicReference<>();
        Subscriber<String> subscriber = event -> result.set(event);

        eventBus.register(subscriber, "testId");
        eventBus.unregister(subscriber);
        eventBus.post("testId", "hello");

        assertNull(result.get());
    }

    @Test
    public void testUnregister_NotRegistered() {
        TestSubscriber subscriber = new TestSubscriber();
        // Unregistering a subscriber that was never registered should not throw
        EventBus result = eventBus.unregister(subscriber);
        assertSame(eventBus, result);
    }

    @Test
    public void testUnregister_DoubleUnregister() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        EventBus result1 = eventBus.unregister(subscriber);
        assertSame(eventBus, result1);

        // Second unregister should not throw
        EventBus result2 = eventBus.unregister(subscriber);
        assertSame(eventBus, result2);

        assertTrue(eventBus.allSubscribers().isEmpty());
    }

    // ---- post(String, Object) ----

    @Test
    public void testPostWithEventId() {
        TestSubscriber subscriber1 = new TestSubscriber();
        TestSubscriber subscriber2 = new TestSubscriber();

        eventBus.register(subscriber1, "event1");
        eventBus.register(subscriber2, "event2");

        eventBus.post("event1", "Message 1");

        assertEquals(List.of("Message 1"), subscriber1.receivedEvents);
        assertEquals(0, subscriber2.receivedEvents.size());
    }

    // ---- post(Object) ----

    @Test
    public void testPost() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        EventBus result = eventBus.post("Test Message");
        assertSame(eventBus, result);
        assertEquals(List.of("Test Message"), subscriber.receivedEvents);
    }

    @Test
    public void testPost_MultipleEventsInSequence() {
        TestSubscriber subscriber = new TestSubscriber();
        eventBus.register(subscriber);

        eventBus.post("first");
        eventBus.post("second");
        eventBus.post("third");

        assertEquals(List.of("first", "second", "third"), subscriber.receivedEvents);

        eventBus.unregister(subscriber);
    }

    @Test
    public void testPostWithoutEventId() {
        AtomicReference<String> result = new AtomicReference<>();
        Subscriber<String> subscriber = event -> result.set(event);

        eventBus.register(subscriber, "testId");
        eventBus.post("hello");

        assertNull(result.get()); // Should not receive without matching event ID
    }

    @Test
    public void testPostWithEmptyEventIdBehavesAsNoEventId() {
        AtomicReference<String> result = new AtomicReference<>();
        Object subscriber = new Object() {
            @Subscribe
            public void onEvent(String event) {
                result.set(event);
            }
        };

        eventBus.register(subscriber);
        eventBus.post("", "hello");

        assertEquals("hello", result.get());
    }

    @Test
    public void testPost_NullEvent() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.post((Object) null));
    }

    @Test
    public void testPost_NoSubscribers() {
        // Posting to a bus with no subscribers should not throw
        EventBus result = eventBus.post("orphan event");
        assertSame(eventBus, result);
    }

    @Test
    public void testPost_SubscriberExceptionDoesNotStopOthers() {
        List<String> received = new ArrayList<>();
        Object throwingSubscriber = new Object() {
            @Subscribe
            public void onEvent(String event) {
                throw new IllegalStateException("intentional");
            }
        };
        Object goodSubscriber = new Object() {
            @Subscribe
            public void onEvent(String event) {
                received.add(event);
            }
        };
        eventBus.register(throwingSubscriber);
        eventBus.register(goodSubscriber);

        assertDoesNotThrow(() -> eventBus.post("first").post("second"));
        assertEquals(List.of("first", "second"), received);
    }

    @Test
    public void testPost_EventIdNoSubscribers() {
        // Posting with eventId that has no subscribers should not throw
        EventBus result = eventBus.post("nonExistentId", "orphan");
        assertSame(eventBus, result);
    }

    @Test
    public void testPost_EventIdNullEvent() {
        assertThrows(IllegalArgumentException.class, () -> eventBus.post("someId", null));
    }

    // ---- isSupportedThreadMode ----

    @Test
    public void testIsSupportedThreadMode() {
        EventBus bus = EventBus.create();

        assertTrue(bus.isSupportedThreadMode(null));
        assertTrue(bus.isSupportedThreadMode(ThreadMode.DEFAULT));
        assertTrue(bus.isSupportedThreadMode(ThreadMode.THREAD_POOL_EXECUTOR));
    }

    public static class NamedObjectSubscriber implements Subscriber<Object> {
        final List<Object> received = new ArrayList<>();

        @Override
        public void on(Object event) {
            received.add(event);
        }
    }

    public static class PrivateStaticAnnotatedSubscriber {
        @Subscribe
        private static void onEvent(String event) {
        }
    }

    public static class PrivateOnlyAnnotatedSubscriber {
        @Subscribe
        private void onEvent(String event) {
        }
    }

    @Test
    public void testNamedSubscriberOfObjectRequiresEventIdLikeALambda() {
        final NamedObjectSubscriber subscriber = new NamedObjectSubscriber();

        assertThrows(IllegalStateException.class, () -> eventBus.register(subscriber));

        eventBus.register(subscriber, "named-object");
        eventBus.post("named-object", "hello");
        eventBus.post("hello-without-id");

        assertEquals(List.of("hello"), subscriber.received);
        eventBus.unregister(subscriber);
    }

    @Test
    public void testPrivateStaticAnnotatedMethodIsRejectedNotIgnored() {
        final RuntimeException e = assertThrows(RuntimeException.class, () -> eventBus.register(new PrivateStaticAnnotatedSubscriber()));
        assertTrue(e.getMessage().contains("must not be static"), e.getMessage());
    }

    @Test
    public void testPrivateAnnotatedMethodAloneIsIgnoredSoNoSubscriberMethodIsFound() {
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> eventBus.register(new PrivateOnlyAnnotatedSubscriber()));
        assertTrue(e.getMessage().startsWith("No subscriber method found"), e.getMessage());
    }

    @Test
    public void testReregistrationAndStickyReplayPreserveDeduplication() {
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

}
