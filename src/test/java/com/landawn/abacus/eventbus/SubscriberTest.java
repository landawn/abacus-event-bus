package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class SubscriberTest extends TestBase {

    private EventBus eventBus;

    @BeforeEach
    public void setUp() {
        eventBus = EventBus.create();
    }

    public static class TestHandler {
        String lastEvent;

        public void handle(String event) {
            this.lastEvent = event;
        }
    }

    private static class CustomEvent {
        final String data;

        CustomEvent(String data) {
            this.data = data;
        }
    }

    @Test
    public void testOnMethod() {
        AtomicReference<String> receivedEvent = new AtomicReference<>();

        Subscriber<String> subscriber = new Subscriber<>() {
            @Override
            public void on(String event) {
                receivedEvent.set(event);
            }
        };

        eventBus.register(subscriber, "testEvent");
        eventBus.post("testEvent", "Hello World");

        assertEquals("Hello World", receivedEvent.get());
    }

    @Test
    public void testOnMethodExists() throws NoSuchMethodException {
        Method onMethod = Subscriber.class.getDeclaredMethod("on", Object.class);
        assertNotNull(onMethod);
        assertEquals(void.class, onMethod.getReturnType());
        assertEquals(1, onMethod.getParameterCount());
    }

    @Test
    public void testIsFunctionalInterface() {
        assertTrue(Subscriber.class.isAnnotationPresent(FunctionalInterface.class));
    }

    @Test
    public void testIsInterface() {
        assertTrue(Subscriber.class.isInterface());
    }

    @Test
    public void testLambdaImplementation() {
        AtomicReference<String> result = new AtomicReference<>();
        Subscriber<String> subscriber = event -> result.set(event);

        subscriber.on("test");
        assertEquals("test", result.get());
    }

    @Test
    public void testAnonymousClassImplementation() {
        AtomicReference<Integer> result = new AtomicReference<>();
        Subscriber<Integer> subscriber = new Subscriber<>() {
            @Override
            public void on(Integer event) {
                result.set(event * 2);
            }
        };

        subscriber.on(5);
        assertEquals(10, result.get());
    }

    @Test
    public void testMethodReferenceImplementation() {
        TestHandler handler = new TestHandler();
        Subscriber<String> subscriber = handler::handle;

        subscriber.on("hello");
        assertEquals("hello", handler.lastEvent);
    }

    @Test
    public void testNullEvent() {
        AtomicReference<String> result = new AtomicReference<>("initial");
        Subscriber<String> subscriber = event -> result.set(event);

        subscriber.on(null);
        assertNull(result.get());
    }

    @Test
    public void testMultipleInvocations() {
        AtomicReference<String> result = new AtomicReference<>();
        Subscriber<String> subscriber = event -> result.set(event);

        subscriber.on("first");
        assertEquals("first", result.get());

        subscriber.on("second");
        assertEquals("second", result.get());

        subscriber.on("third");
        assertEquals("third", result.get());
    }

    @Test
    public void testGenericTypeParameter() throws NoSuchMethodException {
        Method method = Subscriber.class.getDeclaredMethod("on", Object.class);
        Type[] paramTypes = method.getGenericParameterTypes();
        assertNotNull(paramTypes);
        assertEquals(1, paramTypes.length);
        assertEquals(Subscriber.class.getTypeParameters()[0], paramTypes[0]);
    }

    @Test
    public void testSingleAbstractMethod() {
        Method[] methods = Subscriber.class.getDeclaredMethods();
        long abstractMethods = java.util.Arrays.stream(methods).filter(m -> java.lang.reflect.Modifier.isAbstract(m.getModifiers())).count();
        assertEquals(1, abstractMethods);
    }

    @Test
    public void testMethodReference() {
        List<String> events = new ArrayList<>();

        Subscriber<String> subscriber = events::add;

        eventBus.register(subscriber, "methodRef");
        eventBus.post("methodRef", "Method Reference Test");

        assertEquals(List.of("Method Reference Test"), events);
    }

    @Test
    public void testGenericTypes() {
        AtomicReference<Integer> intReceived = new AtomicReference<>();
        AtomicReference<List<String>> listReceived = new AtomicReference<>();

        Subscriber<Integer> intSubscriber = intReceived::set;
        Subscriber<List<String>> listSubscriber = listReceived::set;

        eventBus.register(intSubscriber, "integers");
        eventBus.register(listSubscriber, "lists");

        eventBus.post("integers", 42);
        List<String> testList = List.of("a", "b", "c");
        eventBus.post("lists", testList);

        assertEquals(42, intReceived.get());
        assertEquals(testList, listReceived.get());
    }

    @Test
    public void testMultipleSubscribers() {
        List<String> subscriber1Events = new ArrayList<>();
        List<String> subscriber2Events = new ArrayList<>();

        Subscriber<String> subscriber1 = subscriber1Events::add;
        Subscriber<String> subscriber2 = subscriber2Events::add;

        eventBus.register(subscriber1, "shared");
        eventBus.register(subscriber2, "shared");

        eventBus.post("shared", "Shared Event");

        assertEquals(1, subscriber1Events.size());
        assertEquals(1, subscriber2Events.size());
        assertEquals("Shared Event", subscriber1Events.get(0));
        assertEquals("Shared Event", subscriber2Events.get(0));
    }

    @Test
    public void testNullEventHandling() {
        AtomicReference<String> receivedEvent = new AtomicReference<>("not null");

        Subscriber<String> subscriber = receivedEvent::set;

        eventBus.register(subscriber, "nullEvent");
        assertThrows(IllegalArgumentException.class, () -> eventBus.post("nullEvent", null));
    }

    @Test
    public void testExceptionInSubscriber() {
        List<String> received = new ArrayList<>();
        Subscriber<String> failing = event -> {
            throw new IllegalStateException("expected subscriber failure");
        };
        Subscriber<String> healthy = received::add;
        eventBus.register(failing, "test").register(healthy, "test");

        assertDoesNotThrow(() -> eventBus.post("test", "first").post("test", "second"));
        assertEquals(List.of("first", "second"), received);
    }

    @Test
    public void testSubscriberWithThreadMode() throws InterruptedException {
        AtomicReference<Thread> executionThread = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Subscriber<String> subscriber = event -> {
            executionThread.set(Thread.currentThread());
            latch.countDown();
        };

        eventBus.register(subscriber, "threadTest", ThreadMode.THREAD_POOL_EXECUTOR);
        eventBus.post("threadTest", "Thread Test");

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotEquals(Thread.currentThread(), executionThread.get());
    }

    @Test
    public void testSubscriberUnregistration() {
        AtomicReference<String> receivedEvent = new AtomicReference<>();

        Subscriber<String> subscriber = receivedEvent::set;

        eventBus.register(subscriber, "unregTest");
        eventBus.post("unregTest", "First Event");
        assertEquals("First Event", receivedEvent.get());

        eventBus.unregister(subscriber);
        receivedEvent.set(null);

        eventBus.post("unregTest", "Second Event");
        assertNull(receivedEvent.get());
    }

    @Test
    public void testSubscribersReceiveEventsInRegistrationOrder() {
        List<String> received = new ArrayList<>();
        Subscriber<String> first = event -> received.add("first:" + event);
        Subscriber<String> second = event -> received.add("second:" + event);
        eventBus.register(first, "chain").register(second, "chain");

        eventBus.post("chain", "value");
        assertEquals(List.of("first:value", "second:value"), received);
    }

    @Test
    public void testCustomEventTypes() {
        AtomicReference<CustomEvent> receivedEvent = new AtomicReference<>();

        Subscriber<CustomEvent> subscriber = receivedEvent::set;

        eventBus.register(subscriber, "customEvent");

        CustomEvent event = new CustomEvent("Test Data");
        eventBus.post("customEvent", event);

        assertNotNull(receivedEvent.get());
        assertSame(event, receivedEvent.get());
        assertEquals("Test Data", receivedEvent.get().data);
    }

    @Test
    public void testSubscriberRequiresEventId() {
        Subscriber<Object> generalSubscriber = event -> {
        };

        assertThrows(IllegalStateException.class, () -> eventBus.register(generalSubscriber));

        assertDoesNotThrow(() -> eventBus.register(generalSubscriber, "required"));
    }

    @Test
    public void testComplexTypeSubscriber() {
        AtomicReference<List<CustomEvent>> receivedList = new AtomicReference<>();

        Subscriber<List<CustomEvent>> complexSubscriber = receivedList::set;

        eventBus.register(complexSubscriber, "complexType");

        List<CustomEvent> eventList = List.of(new CustomEvent("Event 1"), new CustomEvent("Event 2"), new CustomEvent("Event 3"));

        eventBus.post("complexType", eventList);

        assertSame(eventList, receivedList.get());
        assertEquals(3, receivedList.get().size());
        assertEquals("Event 2", receivedList.get().get(1).data);
    }

}
