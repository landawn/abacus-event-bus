package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
    void create_whenDistinctExecutorServicesAreEqual_tracksEachByIdentity() throws Exception {
        ValueEqualExecutor first = new ValueEqualExecutor("equal-key");
        ValueEqualExecutor second = new ValueEqualExecutor("equal-key");
        EventBus firstBus = EventBus.create("equal-first", first);
        EventBus secondBus = EventBus.create("equal-second", second);

        invokeShutdownHook();

        assertNotSame(firstBus, secondBus);
        assertTrue(first.isShutdown());
        assertTrue(second.isShutdown(), "Executor services must be tracked by identity, not equals()");
    }

    @ParameterizedTest(name = "illegalStateFailure={0}")
    @ValueSource(booleans = { true, false })
    void create_whenCallerHasThrowingInheritableThreadLocal_stillTracksExecutor(boolean illegalStateFailure) throws Exception {
        ExecutorService warmup = Executors.newSingleThreadExecutor();
        LifecycleExecutor tracked = new LifecycleExecutor();
        AtomicInteger childValueCalls = new AtomicInteger();
        InheritableThreadLocal<Object> requestContext = new InheritableThreadLocal<>() {
            @Override
            protected Object childValue(Object parentValue) {
                childValueCalls.incrementAndGet();
                // IllegalStateException is also what the shutdown probe reports once the JVM is exiting.
                throw illegalStateFailure ? new IllegalStateException("expected childValue failure")
                        : new UnsupportedOperationException("expected childValue failure");
            }
        };
        try {
            // Installs the process-wide hook first, so the next registration takes the shutdown-probe path.
            EventBus.create("probe-warmup", warmup);
            requestContext.set("request");
            try {
                assertDoesNotThrow(() -> EventBus.create("inheriting-caller", tracked));
            } finally {
                requestContext.remove();
            }

            invokeShutdownHook();

            assertEquals(0, childValueCalls.get(), "No application thread-local callback may run under the registry lock");
            assertTrue(tracked.isShutdown(), "The executor must still be tracked for shutdown");
        } finally {
            shutdownAndAwaitTermination(warmup);
            shutdownAndAwaitTermination(tracked);
        }
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

    @Test
    void shutdown_whenDefaultExecutorHasRunningDelivery_drainsItBeforeExit(@TempDir Path directory) throws Exception {
        assertProbeSucceeds(directory, "EVENTBUS_DEFAULT_DRAIN_OK", DefaultExecutorDrainProbe.class);
    }

    @Test
    void create_whenShutdownHookIsAlreadyRunning_leavesExecutorSuppliedDuringShutdownUsable(@TempDir Path directory) throws Exception {
        assertProbeSucceeds(directory, "EVENTBUS_LATE_EXECUTOR_OK", InstalledHookShutdownProbe.class);
    }

    @ParameterizedTest(name = "hook={0}")
    @ValueSource(strings = { "tracked-executor-hook", "default-executor-hook" })
    void shutdownHook_whenInstalledFromApplicationThread_doesNotPinItsClassLoader(String hook, @TempDir Path directory) throws Exception {
        assertProbeSucceeds(directory, "EVENTBUS_HOOK_LOADER_OK", HookClassLoaderProbe.class, hook);
    }

    @Test
    void post_whenApplicationThreadStartsFirstDefaultWorker_runsWorkerInTopLevelGroup(@TempDir Path directory) throws Exception {
        assertProbeSucceeds(directory, "EVENTBUS_WORKER_GROUP_OK", DefaultWorkerGroupProbe.class);
    }

    @Test
    void create_whenSecurityManagerGuardsSystemGroup_stillUsesTopLevelThreadGroup(@TempDir Path directory) throws Exception {
        // JDK 24 and later refuse to start with a security manager enabled.
        assumeTrue(Runtime.version().feature() < 24, "A security manager can only be enabled on JDK 21-23");
        assertProbeSucceeds(directory, "EVENTBUS_SECURITY_MANAGER_GROUP_OK", List.of("-Djava.security.manager=allow"), SecurityManagerThreadGroupProbe.class);
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

    @Test
    void shutdown_whenAwaitTerminationThrows_stillAwaitsOtherExecutors() throws InterruptedException {
        LifecycleExecutor failing = new LifecycleExecutor();
        failing.failAwait = true;
        List<LifecycleExecutor> healthy = new ArrayList<>();
        EventBus.create("failing-await", failing);
        for (int i = 0; i < 4; i++) {
            LifecycleExecutor executor = new LifecycleExecutor();
            healthy.add(executor);
            EventBus.create("healthy-await-" + i, executor);
        }
        try {
            assertDoesNotThrow(EventBusExecutorContractTest::invokeShutdownHook);

            assertEquals(1, failing.awaitCalls);
            for (LifecycleExecutor executor : healthy) {
                assertEquals(1, executor.shutdownCalls);
                assertEquals(1, executor.awaitCalls, "A throwing awaitTermination must not end the wait for the others");
            }
        } finally {
            shutdownAndAwaitTermination(failing);
            for (LifecycleExecutor executor : healthy) {
                shutdownAndAwaitTermination(executor);
            }
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

    /**
     * Exits while a default-executor delivery is still running. Its worker is a daemon thread, so only the default
     * executor's shutdown hook keeps the JVM alive long enough for the delivery to finish and print the marker.
     */
    public static final class DefaultExecutorDrainProbe {
        public static void main(String[] args) throws InterruptedException {
            CountDownLatch started = new CountDownLatch(1);
            Subscriber<String> slow = event -> {
                started.countDown();
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                System.out.println("EVENTBUS_DEFAULT_DRAIN_OK");
            };
            EventBus.create().register(slow, "drain", ThreadMode.THREAD_POOL_EXECUTOR).post("drain", "accepted before exit");
            if (!started.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Delivery did not start");
            }
            System.exit(0);
        }
    }

    /**
     * Installs the EventBus hook normally, then supplies a new executor service while the JVM is shutting down and
     * before that hook has snapshotted its registry (forced by holding the registry monitor until the hook blocks on it).
     */
    public static final class InstalledHookShutdownProbe {
        public static void main(String[] args) throws Exception {
            ExecutorService appExecutor = Executors.newSingleThreadExecutor(InstalledHookShutdownProbe::daemon);
            EventBus.create("installs-hook", appExecutor);
            java.lang.reflect.Field registry = EventBus.class.getDeclaredField("HOOKED_EXECUTORS");
            registry.setAccessible(true);
            Object registryMonitor = registry.get(null);
            ExecutorService lateExecutor = Executors.newSingleThreadExecutor(InstalledHookShutdownProbe::daemon);
            AtomicReference<Thread> eventBusHook = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch holdingMonitor = new CountDownLatch(1);
            Thread lateCreator = new Thread(() -> {
                try {
                    synchronized (registryMonitor) {
                        holdingMonitor.countDown();
                        Thread hook = awaitThread("EventBus-shutdown");
                        eventBusHook.set(hook);
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                        while (hook.getState() != Thread.State.BLOCKED) {
                            if (System.nanoTime() > deadline) {
                                throw new AssertionError("EventBus hook never blocked on its registry");
                            }
                            Thread.onSpinWait();
                        }
                        EventBus.create("created-during-shutdown", lateExecutor);
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "late-creator");
            lateCreator.setDaemon(true);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    lateCreator.join(5000);
                    Thread hook = eventBusHook.get();
                    if (hook != null) {
                        hook.join(5000);
                    }
                    if (failure.get() != null) {
                        failure.get().printStackTrace();
                    } else if (lateExecutor.isShutdown()) {
                        System.out.println("Executor supplied during shutdown was shut down by the EventBus hook");
                    } else {
                        System.out.println("EVENTBUS_LATE_EXECUTOR_OK");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "late-executor-observer"));
            lateCreator.start();
            holdingMonitor.await();
            System.exit(0);
        }

        private static Thread awaitThread(String name) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                for (Thread thread : Thread.getAllStackTraces().keySet()) {
                    if (name.equals(thread.getName())) {
                        return thread;
                    }
                }
                Thread.onSpinWait();
            }
            throw new AssertionError("Thread not found: " + name);
        }

        private static Thread daemon(Runnable task) {
            Thread thread = new Thread(task);
            thread.setDaemon(true);
            return thread;
        }
    }

    /**
     * Installs one of EventBus's process-wide hooks from a thread whose context class loader, inheritable
     * thread-local and thread group (an instance of a {@code ThreadGroup} subclass) belong to a discarded
     * "application", with a class defined by that application's loader on the stack (its protection domain is what
     * JDK 21-23 threads capture), then checks that nothing EventBus keeps retains that loader.
     */
    public static final class HookClassLoaderProbe {
        private static final InheritableThreadLocal<Object> REQUEST_CONTEXT = new InheritableThreadLocal<>();

        public static void main(String[] args) throws Exception {
            boolean defaultExecutorHook = "default-executor-hook".equals(args[0]);
            // Initialize the common utility runtime on the main thread; only the hook under test is installed by the application.
            Class.forName("com.landawn.abacus.util.N");
            if (!defaultExecutorHook) {
                EventBus.getDefault();
            }
            WeakReference<ClassLoader> applicationLoader = runApplication(defaultExecutorHook);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (applicationLoader.get() != null && System.nanoTime() < deadline) {
                System.gc();
                TimeUnit.MILLISECONDS.sleep(20);
            }
            System.out.println(applicationLoader.get() == null ? "EVENTBUS_HOOK_LOADER_OK" : "Application class loader is still retained");
        }

        private static WeakReference<ClassLoader> runApplication(boolean defaultExecutorHook) throws Exception {
            ApplicationLoader loader = new ApplicationLoader(HookClassLoaderProbe.class.getClassLoader());
            Method applicationEntry = loader.defineFromClassPath(ApplicationCode.class).getMethod("run", boolean.class, ExecutorService.class);
            ThreadGroup applicationGroup = (ThreadGroup) loader.defineFromClassPath(ApplicationThreadGroup.class)
                    .getConstructor(String.class)
                    .newInstance("application");
            Object requestScoped = new Object[] { loader };
            ExecutorService executor = Executors.newSingleThreadExecutor();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread applicationThread = new Thread(applicationGroup, () -> {
                try {
                    REQUEST_CONTEXT.set(requestScoped);
                    applicationEntry.invoke(null, defaultExecutorHook, executor);
                } catch (Throwable t) {
                    failure.set(t);
                } finally {
                    REQUEST_CONTEXT.remove();
                }
            }, "application-thread");
            applicationThread.setContextClassLoader(loader);
            applicationThread.start();
            applicationThread.join(5000);
            executor.shutdownNow();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS) || failure.get() != null) {
                throw new AssertionError("Application thread failed", failure.get());
            }
            return new WeakReference<>(loader);
        }
    }

    /**
     * Starts the first default-executor worker from a thread in an application's own group: the worker must still
     * join the top-level group rather than the group of whichever thread happened to post first.
     */
    public static final class DefaultWorkerGroupProbe {
        public static void main(String[] args) throws Exception {
            System.out.println(describeFirstWorkerGroup(Thread.currentThread().getThreadGroup()));
        }

        static String describeFirstWorkerGroup(ThreadGroup expected) throws InterruptedException {
            EventBus bus = EventBus.create();
            AtomicReference<ThreadGroup> workerGroup = new AtomicReference<>();
            CountDownLatch delivered = new CountDownLatch(1);
            Subscriber<String> recorder = event -> {
                workerGroup.set(Thread.currentThread().getThreadGroup());
                delivered.countDown();
            };
            bus.register(recorder, "group", ThreadMode.THREAD_POOL_EXECUTOR);
            // In a fresh JVM no default worker exists yet, so this post creates one from the application thread.
            Thread poster = new Thread(new ThreadGroup("application"), () -> bus.post("group", "first"), "application-poster");
            poster.start();
            poster.join(5000);
            if (!delivered.await(5, TimeUnit.SECONDS)) {
                return "Event was not delivered";
            }
            return workerGroup.get() == expected ? "EVENTBUS_WORKER_GROUP_OK" : "Worker joined group " + workerGroup.get().getName();
        }
    }

    /**
     * Initializes EventBus on an application-group thread under a security manager that, like the default one
     * without {@code modifyThreadGroup}, guards only the system group; hooks and workers must still use "main".
     */
    public static final class SecurityManagerThreadGroupProbe {
        @SuppressWarnings("removal")
        public static void main(String[] args) throws Exception {
            Class.forName("com.landawn.abacus.util.N"); // initialize the common utility runtime before the security manager
            ThreadGroup main = Thread.currentThread().getThreadGroup();
            System.setSecurityManager(new SecurityManager() {
                @Override
                public void checkPermission(java.security.Permission permission) {
                    if (permission instanceof RuntimePermission && "modifyThreadGroup".equals(permission.getName())) {
                        throw new SecurityException("expected: the system thread group is guarded");
                    }
                }

                @Override
                public void checkPermission(java.security.Permission permission, Object context) {
                    checkPermission(permission);
                }
            });
            AtomicReference<String> workerResult = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            // EventBus is first initialized, and its first default worker started, on this application-group thread.
            Thread application = new Thread(new ThreadGroup("application"), () -> {
                try {
                    workerResult.set(DefaultWorkerGroupProbe.describeFirstWorkerGroup(main));
                } catch (Throwable t) {
                    failure.set(t);
                }
            }, "application-initializer");
            application.start();
            application.join(10000);
            if (failure.get() != null) {
                failure.get().printStackTrace();
                return;
            }
            java.lang.reflect.Field probe = EventBus.class.getDeclaredField("SHUTDOWN_PROBE");
            probe.setAccessible(true);
            ThreadGroup hookGroup = ((Thread) probe.get(null)).getThreadGroup();
            if (hookGroup != main) {
                System.out.println("Hook thread joined group " + hookGroup.getName());
            } else if (!"EVENTBUS_WORKER_GROUP_OK".equals(workerResult.get())) {
                System.out.println(workerResult.get());
            } else {
                System.out.println("EVENTBUS_SECURITY_MANAGER_GROUP_OK");
            }
        }
    }

    /** Application code; {@link HookClassLoaderProbe} runs a copy defined by a discarded application loader. */
    public static final class ApplicationCode {
        public static void run(boolean defaultExecutorHook, ExecutorService executor) {
            if (defaultExecutorHook) {
                EventBus.getDefault(); // class initialization installs the default-executor hook
            } else {
                EventBus.create("application-bus", executor); // the first tracked executor installs that hook
            }
        }
    }

    /** An application's own thread group type; {@link HookClassLoaderProbe} runs its thread in a copy defined by the discarded loader. */
    public static final class ApplicationThreadGroup extends ThreadGroup {
        public ApplicationThreadGroup(String name) {
            super(name);
        }
    }

    /** Defines its own copy of a class-path class, as a web application's loader would for application code. */
    private static final class ApplicationLoader extends ClassLoader {
        ApplicationLoader(ClassLoader parent) {
            super(parent);
        }

        Class<?> defineFromClassPath(Class<?> type) throws java.io.IOException {
            try (java.io.InputStream in = type.getResourceAsStream(type.getName().substring(type.getPackageName().length() + 1) + ".class")) {
                byte[] bytes = in.readAllBytes();
                return defineClass(type.getName(), bytes, 0, bytes.length);
            }
        }
    }

    private static final class LifecycleExecutor extends AbstractExecutorService {
        boolean failShutdown;
        boolean failAwait;
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
            failAwait = false;
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
            if (failAwait) {
                throw new IllegalStateException("expected await failure: lifecycle managed elsewhere");
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

    /** Value equality, as a record-style executor wrapper would have. */
    private static final class ValueEqualExecutor extends AbstractExecutorService {
        private final String key;
        private volatile boolean shutdown;

        ValueEqualExecutor(String key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ValueEqualExecutor executor && executor.key.equals(key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
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
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    /** Runs a probe's main method in a bounded child JVM and requires its success marker line. */
    private static void assertProbeSucceeds(Path directory, String successLine, Class<?> probe, String... args) throws Exception {
        assertProbeSucceeds(directory, successLine, List.of(), probe, args);
    }

    private static void assertProbeSucceeds(Path directory, String successLine, List<String> jvmOptions, Class<?> probe, String... args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.addAll(jvmOptions);
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(probe.getName());
        command.addAll(List.of(args));
        Path output = directory.resolve(probe.getSimpleName() + ".log");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(20, TimeUnit.SECONDS), "Child JVM did not complete");
            String diagnostics = Files.readString(output);
            assertEquals(0, process.exitValue(), diagnostics);
            assertTrue(diagnostics.lines().anyMatch(successLine::equals), diagnostics);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5, TimeUnit.SECONDS), "Child JVM did not terminate after cancellation");
            }
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
