package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.landawn.abacus.TestBase;

@Tag("unit")
public class EventBusClassLoaderTest extends TestBase {
    @Test
    @Timeout(15)
    void unregisteredSubscriberMetadataDoesNotPinItsLoader() throws Exception {
        EventBus bus = EventBus.create();
        WeakReference<ClassLoader> loader = registerDisposableSubscriber(bus);
        awaitCollected(loader);
        Reference.reachabilityFence(bus);
    }

    @Test
    @Timeout(15)
    void unmatchedEventTypesDoNotAccumulateInLongLivedSubscribers() throws Exception {
        EventBus bus = EventBus.create();
        ParentSubscriber subscriber = new ParentSubscriber();
        bus.register(subscriber);
        WeakReference<ClassLoader> loader = postDisposableEvent(bus);
        awaitCollected(loader);
        bus.post("still registered");
        assertEquals(1, subscriber.events);
        bus.unregister(subscriber);
    }

    public static class ParentSubscriber {
        int events;

        @Subscribe
        public void on(String value) {
            events++;
        }
    }

    public static class DisposableSubscriber {
        @Subscribe
        public void on(String value) {
        }
    }

    public static class DisposableEvent {
    }

    private static WeakReference<ClassLoader> registerDisposableSubscriber(EventBus bus) throws Exception {
        ClassLoader loader = isolatedLoader(DisposableSubscriber.class);
        Object subscriber = loader.loadClass(DisposableSubscriber.class.getName()).getConstructor().newInstance();
        bus.register(subscriber);
        bus.post("delivered");
        bus.unregister(subscriber);
        return new WeakReference<>(loader);
    }

    private static WeakReference<ClassLoader> postDisposableEvent(EventBus bus) throws Exception {
        ClassLoader loader = isolatedLoader(DisposableEvent.class);
        bus.post(loader.loadClass(DisposableEvent.class.getName()).getConstructor().newInstance());
        return new WeakReference<>(loader);
    }

    private static ClassLoader isolatedLoader(Class<?> fixture) throws Exception {
        String name = fixture.getName();
        byte[] bytes;
        try (var input = fixture.getResourceAsStream("/" + name.replace('.', '/') + ".class")) {
            assertNotNull(input, "Missing bytecode for class-loader fixture " + name);
            bytes = input.readAllBytes();
        }
        return new ClassLoader(fixture.getClassLoader()) {
            @Override
            protected Class<?> loadClass(String requested, boolean resolve) throws ClassNotFoundException {
                if (!requested.equals(name)) {
                    return super.loadClass(requested, resolve);
                }
                synchronized (getClassLoadingLock(requested)) {
                    Class<?> type = findLoadedClass(requested);
                    if (type == null) {
                        type = defineClass(requested, bytes, 0, bytes.length);
                    }
                    if (resolve) {
                        resolveClass(type);
                    }
                    return type;
                }
            }
        };
    }

}
