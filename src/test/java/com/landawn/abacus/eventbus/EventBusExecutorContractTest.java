package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class EventBusExecutorContractTest extends TestBase {
    public static class Listener {
        Thread thread;
        String value;

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
        public void receive(String event) {
            thread = Thread.currentThread();
            value = event;
        }
    }

    @Test
    void executorDispatchCanRunOnThePostingThread() {
        final EventBus bus = EventBus.create("direct-contract", Runnable::run);
        final Listener listener = new Listener();
        bus.register(listener);
        try {
            bus.post("你好🙂");
            assertSame(Thread.currentThread(), listener.thread);
            assertEquals("你好🙂", listener.value);
        } finally {
            bus.unregister(listener);
        }
    }

    @Test
    public void testDefaultExecutorDeliversOnDaemonNormalPriorityThread() throws InterruptedException {
        final EventBus bus = EventBus.create();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Thread> eventThread = new AtomicReference<>();
        final Object subscriber = new Object() {
            @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
            public void onEvent(String event) {
                eventThread.set(Thread.currentThread());
                latch.countDown();
            }
        };

        bus.register(subscriber);
        bus.post("daemon-check \u03bb");

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        final Thread worker = eventThread.get();
        assertNotNull(worker);
        assertNotEquals(Thread.currentThread(), worker);
        assertTrue(worker.isDaemon(), "default executor worker must be a daemon thread: " + worker.getName());
        assertEquals(Thread.NORM_PRIORITY, worker.getPriority());
        bus.unregister(subscriber);
    }

    @Test
    public void testDefaultBusAsyncDeliveryUsesDaemonThreadAndSyncDeliveryStaysOnCaller() throws InterruptedException {
        final EventBus bus = EventBus.getDefault();
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Thread> asyncThread = new AtomicReference<>();
        final AtomicReference<Thread> syncThread = new AtomicReference<>();
        final Object subscriber = new Object() {
            @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, eventId = "daemon-async")
            public void onAsync(String event) {
                asyncThread.set(Thread.currentThread());
                latch.countDown();
            }

            @Subscribe(eventId = "daemon-sync")
            public void onSync(String event) {
                syncThread.set(Thread.currentThread());
            }
        };

        try {
            bus.register(subscriber);
            bus.post("daemon-sync", "s");
            bus.post("daemon-async", "a");

            assertTrue(latch.await(5, TimeUnit.SECONDS));
            assertTrue(asyncThread.get().isDaemon());
            // Regression guard: DEFAULT mode is unaffected by the executor change.
            assertEquals(Thread.currentThread(), syncThread.get());
        } finally {
            bus.unregister(subscriber);
        }
    }

    private static int applicationShutdownHookCount() throws Exception {
        final Class<?> hooksClass = Class.forName("java.lang.ApplicationShutdownHooks");
        final java.lang.reflect.Field hooksField = hooksClass.getDeclaredField("hooks");
        hooksField.setAccessible(true);
        synchronized (hooksClass) {
            return ((Map<?, ?>) hooksField.get(null)).size();
        }
    }

    @Test
    public void testManyBusesOverOneExecutorServiceRegisterAtMostOneShutdownHook() throws Exception {
        final ExecutorService shared = Executors.newSingleThreadExecutor();
        final ExecutorService other = Executors.newSingleThreadExecutor();
        try {
            // Installs the single process-wide hook if this JVM has not done so yet.
            EventBus.create("hook-warmup", shared);
            final int before = applicationShutdownHookCount();

            for (int i = 0; i < 25; i++) {
                EventBus.create("hook-" + i, shared);
            }

            assertEquals(before, applicationShutdownHookCount(), "one process-wide hook, not one hook per bus");

            // A second, distinct executor service joins the same hook.
            EventBus.create("hook-other", other);
            assertEquals(before, applicationShutdownHookCount());
        } finally {
            shared.shutdownNow();
            other.shutdownNow();
        }
    }

    @Test
    public void testPlainExecutorAndDefaultExecutorRegisterNoShutdownHookAndNullExecutorStillRejected() throws Exception {
        final int before = applicationShutdownHookCount();

        EventBus.create("plain-executor", Runnable::run);
        EventBus.create("plain-executor-2", (Executor) Runnable::run);
        EventBus.create();
        EventBus.create("default-executor");

        assertEquals(before, applicationShutdownHookCount());
        assertThrows(IllegalArgumentException.class, () -> EventBus.create("null-executor", null));
    }

    private static WeakReference<ExecutorService> createBusOverUnreferencedExecutorAndDropIt() {
        final ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        EventBus.create("discarded-" + System.nanoTime(), executor);
        return new WeakReference<>(executor);
    }

    @Test
    public void testDiscardedBusDoesNotPinItsExecutorService() throws Exception {
        final WeakReference<ExecutorService> ref = createBusOverUnreferencedExecutorAndDropIt();

        awaitCollected(ref);
    }

}
