package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
@Timeout(30)
@ResourceLock("eventbus-shutdown-hooks")
public class EventBusExecutorContractTest extends TestBase {

    @Test
    void create_whenBusesShareExecutors_registersAtMostOneShutdownHook() throws Exception {
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
            shutdownAndAwaitTermination(shared);
            shutdownAndAwaitTermination(other);
        }
    }

    @Test
    void create_whenExecutorNeedsNoLifecycleManagement_addsNoShutdownHook() throws Exception {
        // Initialize the shared runtime before measuring hooks added by individual factory calls.
        EventBus.getDefault();
        final int before = applicationShutdownHookCount();

        EventBus.create("plain-executor", Runnable::run);
        EventBus.create();
        EventBus.create("default-executor");

        assertEquals(before, applicationShutdownHookCount());
    }

    @Test
    void create_whenBusAndExecutorAreDiscarded_allowsExecutorCollection() throws Exception {
        final WeakReference<ExecutorService> ref = createBusOverUnreferencedExecutorAndDropIt();

        awaitCollected(ref);
    }

    @Test
    void create_whenExecutorIsProvided_dispatchesThroughThatExecutor() throws InterruptedException {
        ExecutorService executor = Executors.newSingleThreadExecutor(task -> {
            Thread worker = new Thread(task, "eventbus-custom-executor");
            worker.setDaemon(true);
            return worker;
        });
        try {
            EventBus bus = EventBus.create("custom", executor);
            AtomicReference<Thread> callbackThread = new AtomicReference<>();
            AtomicReference<String> received = new AtomicReference<>();
            CountDownLatch delivered = new CountDownLatch(1);
            Object subscriber = new Object() {
                @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
                public void onEvent(String event) {
                    received.set(event);
                    callbackThread.set(Thread.currentThread());
                    delivered.countDown();
                }
            };
            bus.register(subscriber);
            bus.post("payload");

            assertTrue(delivered.await(5, TimeUnit.SECONDS), "Custom executor did not deliver the event");
            assertEquals("custom", bus.identifier());
            assertEquals("payload", received.get());
            assertNotSame(Thread.currentThread(), callbackThread.get());
            assertEquals("eventbus-custom-executor", callbackThread.get().getName());
        } finally {
            shutdownAndAwaitTermination(executor);
        }
    }

    @Test
    void create_whenJvmIsAlreadyShuttingDown_keepsDefaultAndCustomExecutorsUsable(@TempDir Path directory) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        // The child must dump coverage after the work inside its shutdown hook, not at hook entry.
        ManagementFactory.getRuntimeMXBean()
                .getInputArguments()
                .stream()
                .filter(argument -> argument.startsWith("-javaagent:") && argument.contains("jacoco"))
                .map(argument -> argument + ",dumponexit=false,append=true")
                .forEach(command::add);
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(ShutdownProbe.class.getName());
        Path output = directory.resolve("shutdown-probe.log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(20, TimeUnit.SECONDS), "Child JVM did not complete shutdown");
            String diagnostics = Files.readString(output);
            assertEquals(0, process.exitValue(), diagnostics);
            assertTrue(diagnostics.lines().anyMatch("EVENTBUS_SHUTDOWN_OK"::equals), diagnostics);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Child JVM did not terminate after cancellation");
            }
        }
    }

    @ParameterizedTest
    @NullSource
    @EnumSource(value = ThreadMode.class, names = { "DEFAULT", "THREAD_POOL_EXECUTOR" })
    void register_whenThreadModeOverridesAnnotation_usesSelectedExecutorPolicy(ThreadMode override) {
        List<Runnable> queued = new ArrayList<>();
        EventBus bus = EventBus.create("mode-override", queued::add);
        Listener listener = new Listener();
        bus.register(listener, override);

        bus.post("payload");

        assertTrue(bus.isSupportedThreadMode(override));
        if (override == ThreadMode.DEFAULT) {
            assertTrue(queued.isEmpty());
            assertEquals("payload", listener.value);
        } else {
            assertNull(listener.value);
            assertEquals(1, queued.size());
            queued.get(0).run();
            assertEquals("payload", listener.value);
        }
    }

    @Test
    void post_whenExecutorRunsInline_deliversOnPostingThread() {
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

    @ParameterizedTest(name = "lambdaSubscriber={0}")
    @ValueSource(booleans = { false, true })
    void post_whenDefaultExecutorIsSelected_usesDaemonNormalPriorityWorker(boolean lambdaSubscriber) throws InterruptedException {
        EventBus bus = EventBus.create();
        CountDownLatch delivered = new CountDownLatch(1);
        AtomicReference<Thread> callbackThread = new AtomicReference<>();
        AtomicReference<String> received = new AtomicReference<>();
        Subscriber<String> callback = event -> {
            received.set(event);
            callbackThread.set(Thread.currentThread());
            delivered.countDown();
        };
        Object subscriber;
        if (lambdaSubscriber) {
            subscriber = callback;
            bus.register(callback, "worker", ThreadMode.THREAD_POOL_EXECUTOR);
        } else {
            subscriber = new Object() {
                @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
                public void onEvent(String event) {
                    callback.on(event);
                }
            };
            bus.register(subscriber, "worker");
        }
        bus.post("worker", "daemon-check \u03bb");

        assertTrue(delivered.await(5, TimeUnit.SECONDS), "Default executor did not deliver the event");
        Thread worker = callbackThread.get();
        assertNotNull(worker);
        assertNotSame(Thread.currentThread(), worker);
        assertTrue(worker.isDaemon(), "Default executor worker must be a daemon: " + worker.getName());
        assertEquals(Thread.NORM_PRIORITY, worker.getPriority());
        assertEquals("daemon-check \u03bb", received.get());
        bus.unregister(subscriber);
    }

    @Test
    void post_whenDefaultBusUsesMixedModes_honorsEachHandlerMode() throws InterruptedException {
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

    @Test
    void shutdown_whenExecutorHasAcceptedCallbacks_completesThemAndTerminatesExecutor() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            EventBus bus = EventBus.create("shutdown-contract", executor);
            AtomicInteger delivered = new AtomicInteger();
            Subscriber<Integer> subscriber = event -> delivered.incrementAndGet();
            bus.register(subscriber, "work", ThreadMode.THREAD_POOL_EXECUTOR);
            for (int event = 0; event < 32; event++) {
                bus.post("work", event);
            }

            // Exercise the hook while the executor is strongly reachable, without terminating the test JVM.
            invokeShutdownHook();

            assertTrue(executor.isShutdown());
            assertTrue(executor.isTerminated(), "The hook must await accepted callbacks");
            assertEquals(32, delivered.get());
        } finally {
            shutdownAndAwaitTermination(executor);
        }
    }

    @Test
    void shutdown_whenOneExecutorThrows_continuesShuttingDownAndAwaitingOthers() throws InterruptedException {
        LifecycleExecutor failing = new LifecycleExecutor();
        LifecycleExecutor healthy = new LifecycleExecutor();
        failing.failShutdown = true;
        EventBus.create("failing-shutdown", failing);
        EventBus.create("healthy-shutdown", healthy);
        try {
            assertDoesNotThrow(EventBusExecutorContractTest::invokeShutdownHook);

            assertEquals(1, failing.shutdownCalls);
            assertEquals(1, healthy.shutdownCalls);
            assertEquals(1, failing.awaitCalls);
            assertEquals(1, healthy.awaitCalls);
            assertTrue(healthy.isTerminated());
        } finally {
            shutdownAndAwaitTermination(failing);
            shutdownAndAwaitTermination(healthy);
        }
    }

    @Test
    void shutdown_whenAwaitIsInterrupted_restoresInterruptAfterShuttingDownEveryExecutor() throws Exception {
        LifecycleExecutor interrupted = new LifecycleExecutor();
        LifecycleExecutor healthy = new LifecycleExecutor();
        interrupted.interruptAwait = true;
        EventBus.create("interrupted-shutdown", interrupted);
        EventBus.create("other-shutdown", healthy);
        assertFalse(Thread.currentThread().isInterrupted());
        try {
            invokeShutdownHook();

            assertTrue(Thread.currentThread().isInterrupted(), "The shutdown hook must preserve the interrupt signal");
            assertTrue(interrupted.isShutdown());
            assertTrue(healthy.isShutdown(), "All executors must be shut down before waiting starts");
            assertEquals(1, interrupted.awaitCalls);
        } finally {
            Thread.interrupted();
            shutdownAndAwaitTermination(interrupted);
            shutdownAndAwaitTermination(healthy);
        }
    }

    public static class Listener {
        Thread thread;
        String value;

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
        public void receive(String event) {
            thread = Thread.currentThread();
            value = event;
        }
    }

    /** Loads EventBus for the first time inside a real shutdown hook in a separate JVM. */
    public static final class ShutdownProbe {
        public static void main(String[] args) throws ClassNotFoundException {
            // Initialize the common utility runtime before shutdown; only EventBus is loaded late.
            Class.forName("com.landawn.abacus.util.N");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    verifyDispatch(EventBus.create(), true);
                    ExecutorService custom = Executors.newSingleThreadExecutor();
                    try {
                        verifyDispatch(EventBus.create("during-shutdown", custom), false);
                    } finally {
                        custom.shutdownNow();
                        if (!custom.awaitTermination(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("Custom executor did not terminate");
                        }
                    }
                    dumpCoverageIfEnabled();
                    System.out.println("EVENTBUS_SHUTDOWN_OK");
                } catch (Throwable failure) {
                    failure.printStackTrace();
                }
            }, "eventbus-shutdown-probe"));
        }

        private static void verifyDispatch(EventBus bus, boolean requireDaemon) throws InterruptedException {
            CountDownLatch delivered = new CountDownLatch(1);
            AtomicReference<Thread> worker = new AtomicReference<>();
            AtomicReference<String> received = new AtomicReference<>();
            Subscriber<String> subscriber = event -> {
                worker.set(Thread.currentThread());
                received.set(event);
                delivered.countDown();
            };
            bus.register(subscriber, "shutdown", ThreadMode.THREAD_POOL_EXECUTOR);
            bus.post("shutdown", "accepted during shutdown");
            if (!delivered.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Event was not delivered during shutdown");
            }
            if (!"accepted during shutdown".equals(received.get()) || worker.get() == Thread.currentThread()) {
                throw new AssertionError("Incorrect asynchronous delivery during shutdown");
            }
            if (requireDaemon && !worker.get().isDaemon()) {
                throw new AssertionError("Fallback default executor must use daemon threads");
            }
        }

        private static void dumpCoverageIfEnabled() throws ReflectiveOperationException {
            final Class<?> runtime;
            try {
                runtime = Class.forName("org.jacoco.agent.rt.RT");
            } catch (ClassNotFoundException noCoverageAgent) {
                return;
            }
            Object agent = runtime.getMethod("getAgent").invoke(null);
            Class.forName("org.jacoco.agent.rt.IAgent").getMethod("dump", boolean.class).invoke(agent, false);
        }
    }

    private static final class LifecycleExecutor extends AbstractExecutorService {
        boolean failShutdown;
        boolean interruptAwait;
        boolean shutdown;
        int shutdownCalls;
        int awaitCalls;

        @Override
        public void shutdown() {
            shutdownCalls++;
            if (failShutdown) {
                throw new RejectedExecutionException("expected shutdown failure");
            }
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            failShutdown = false;
            interruptAwait = false;
            shutdown();
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            awaitCalls++;
            if (interruptAwait) {
                throw new InterruptedException("expected termination interruption");
            }
            return isTerminated();
        }

        @Override
        public void execute(Runnable command) {
            if (shutdown) {
                throw new RejectedExecutionException("executor is shut down");
            }
            command.run();
        }
    }

    private static void invokeShutdownHook() throws ReflectiveOperationException {
        Method shutdown = EventBus.class.getDeclaredMethod("shutdownTrackedExecutors");
        shutdown.setAccessible(true);
        shutdown.invoke(null);
    }

    private static int applicationShutdownHookCount() throws Exception {
        final Class<?> hooksClass = Class.forName("java.lang.ApplicationShutdownHooks");
        final java.lang.reflect.Field hooksField = hooksClass.getDeclaredField("hooks");
        hooksField.setAccessible(true);
        synchronized (hooksClass) {
            return ((Map<?, ?>) hooksField.get(null)).size();
        }
    }

    private static WeakReference<ExecutorService> createBusOverUnreferencedExecutorAndDropIt() {
        final ExecutorService executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        EventBus.create("discarded-" + System.nanoTime(), executor);
        return new WeakReference<>(executor);
    }
}
