/*
 * Copyright (C) 2016 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.eventbus;

import java.lang.ref.WeakReference;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.AndroidUtil;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.ThreadMode;
import com.landawn.abacus.util.cs;

// DO NOT try to move me out of this project. Somebody tried and gave up then. I'm small. I stay here.

/**
 * A publish-subscribe event bus that simplifies communication between components.
 * It allows components to communicate without requiring explicit references to one another,
 * thus promoting loose coupling.
 *
 * <p>Supported features:</p>
 * <ul>
 *   <li>Event delivery to subscribers matched by event type hierarchy.</li>
 *   <li>Sticky events that persist and are replayed to subscriber methods declaring
 *       {@code @Subscribe(sticky = true)} when they register.</li>
 *   <li>Thread mode control for synchronous or asynchronous event delivery.</li>
 *   <li>Event filtering by event ID.</li>
 *   <li>Interval-based throttling and deduplication of consecutive identical events.</li>
 * </ul>
 *
 * <p>Subscribers can be registered in two ways:</p>
 * <ol>
 *   <li>By implementing the {@link Subscriber} interface (suitable for lambda expressions).</li>
 *   <li>By annotating public instance methods with {@link Subscribe}.</li>
 * </ol>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Subscriber using the Subscriber interface (lambda-friendly)
 * final Subscriber<String> strSubscriber = event -> System.out.println("Received: " + event);
 *
 * // Subscriber using the @Subscribe annotation
 * final Object annotatedSubscriber = new Object() {
 *     @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR)
 *     public void handleEvent(String event) {
 *         System.out.println("Handled: " + event);
 *     }
 * };
 *
 * EventBus eventBus = EventBus.getDefault();
 * eventBus.register(strSubscriber, "myEventId");
 * eventBus.register(annotatedSubscriber);
 *
 * eventBus.post("Hello World");
 * eventBus.post("myEventId", "Targeted Event");
 * eventBus.postSticky("Sticky Message");
 *
 * eventBus.unregister(strSubscriber);
 * eventBus.unregister(annotatedSubscriber);
 * }</pre>
 *
 * <p>This class is thread-safe and can be used in multi-threaded environments. Filtering atomically
 * accepts or suppresses delivery attempts; it does not serialize subscriber callbacks. Accepted
 * callbacks may execute concurrently when events are posted from multiple threads, and
 * {@link ThreadMode#THREAD_POOL_EXECUTOR} callbacks may also run concurrently and out of posting order
 * even when every event is posted from a single thread.</p>
 *
 * <p>Logging uses an event or subscriber's normal string description when available. If describing
 * that object throws an exception, diagnostics fall back to its class name and identity hash so
 * the description failure does not interrupt registration or event delivery.</p>
 *
 * <p>Subscriber event types are resolved through generic superclasses and interfaces; parameterized
 * event types are matched by their raw class. Overridden handlers are registered once, using the
 * most-specific {@code @Subscribe} declaration. Class declarations take precedence over interface
 * declarations. Conflicting annotations on unrelated interfaces require an annotated override and
 * otherwise cause registration to throw {@link IllegalArgumentException}. Unannotated overloads of
 * {@code on} do not become handlers merely because the class implements {@link Subscriber}.</p>
 *
 * @see Subscriber
 * @see Subscribe
 */
public class EventBus {

    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);

    // Tie reflective metadata to the subscriber class's lifetime instead of pinning its loader in a global map.
    private static final ClassValue<List<SubIdentifier>> classMetaSubMap = new ClassValue<>() {
        @Override
        protected List<SubIdentifier> computeValue(final Class<?> subscriberClass) {
            return discoverSubscriberMethods(subscriberClass);
        }
    };

    private static final Executor DEFAULT_EXECUTOR;

    static {
        if (IOUtil.IS_PLATFORM_ANDROID) {
            DEFAULT_EXECUTOR = AndroidUtil.getThreadPoolExecutor();
        } else {
            // Daemon workers plus a 120-second delayed shutdown hook. A non-daemon pool pinned the JVM after
            // the first asynchronous delivery: the hook can only run once the JVM starts exiting, which a live
            // non-daemon worker prevents.
            //
            // Workers join the top-level thread group, not the initializing thread's: Executors.defaultThreadFactory()
            // captured that group for the life of the pool, and an application-defined ThreadGroup subclass would
            // have pinned its class loader. Like that factory's threads, a worker still inherits the context class
            // loader, inheritable thread-locals and (on JDK 21-23) access-control context of the posting thread that
            // created it, keeping them for its whole life - not just until it idles out, since a worker kept busy by
            // other posters never times out. Not inheriting would change the context class loader subscribers see;
            // getDefault() documents the limitation and the custom-executor alternative.
            final ThreadGroup workerGroup = topLevelThreadGroup();
            final AtomicInteger workerNumber = new AtomicInteger();
            final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(//
                    N.max(8, IOUtil.CPU_CORES), // coreThreadPoolSize
                    N.max(64, IOUtil.CPU_CORES), // maxThreadPoolSize
                    180L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> {
                        final Thread t = new Thread(workerGroup, r, "EventBus-worker-" + workerNumber.incrementAndGet(), 0);
                        t.setDaemon(true);

                        if (t.getPriority() != Thread.NORM_PRIORITY) {
                            t.setPriority(Thread.NORM_PRIORITY);
                        }

                        return t;
                    });

            // With an unbounded queue the pool never grows past its core size, so without this the
            // N.max(8, cores) core workers would live for the rest of the JVM once started.
            threadPoolExecutor.allowCoreThreadTimeOut(true);

            try {
                // The same drain as MoreExecutors.getExitingExecutorService(pool, 120, SECONDS), but its hook
                // thread was created by whichever thread initialized this class and so pinned that thread's
                // context class loader and inheritable thread-locals (e.g. a web application's) until JVM exit.
                Runtime.getRuntime().addShutdownHook(newShutdownHookThread(() -> {
                    threadPoolExecutor.shutdown();

                    try {
                        //noinspection ResultOfMethodCallIgnored
                        threadPoolExecutor.awaitTermination(120, TimeUnit.SECONDS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, "EventBus-default-executor-shutdown"));
            } catch (final IllegalStateException e) {
                // This class is being loaded while the JVM is already shutting down, so no hook can be
                // registered - and there is nothing left to wait for. Failing here would leave the class
                // permanently unusable with an ExceptionInInitializerError.
            }

            DEFAULT_EXECUTOR = Executors.unconfigurableExecutorService(threadPoolExecutor);
        }
    }

    private final Map<Object, List<SubIdentifier>> registeredSubMap = new IdentityHashMap<>();

    private final Map<String, Set<SubIdentifier>> registeredEventIdSubMap = new HashMap<>();

    private final Map<Object, String> stickyEventMap = new IdentityHashMap<>();

    private final String identifier;

    private final Executor executor;

    private final Map<String, List<SubIdentifier>> listOfEventIdSubMap = new ConcurrentHashMap<>();

    private volatile List<List<SubIdentifier>> listOfSubEventSubs = null;

    private volatile Map<Object, String> mapOfStickyEvent = null;

    /**
     * Serializes the decision points of {@link #register} (make a subscriber visible + snapshot the
     * sticky events to replay) against {@link #postSticky} (record the sticky event + resolve its
     * delivery list). Holding this single lock across those two short critical sections (never across
     * the actual {@code dispatch(...)}, which may run subscriber code) guarantees that a sticky event
     * is delivered to a concurrently-registering subscriber exactly once — either by {@code postSticky}'s
     * own delivery (if the subscriber was already visible) or by {@code register}'s replay (if the event
     * was already recorded), but never both. It also makes each {@code register}/{@code unregister} update
     * of {@code registeredSubMap} plus {@code registeredEventIdSubMap} one transaction: without it, an
     * unregister interleaved between register's two map updates would leave an event-ID entry for a
     * subscriber that is no longer registered. It is always the outermost lock, so it cannot deadlock with
     * {@code registeredSubMap} / {@code registeredEventIdSubMap} / {@code stickyEventMap}.
     */
    private final Object stickyDeliveryLock = new Object();

    /**
     * Caller-supplied {@link ExecutorService}s to shut down at JVM exit, compared by identity. Held weakly so a
     * discarded bus does not pin its executor. A pool such as {@link ThreadPoolExecutor} stays strongly
     * reachable through its live workers, but a decorator such as {@code Executors.newSingleThreadExecutor()}
     * does not, so it can be collected (and dropped from here) while the pool behind it still runs deliveries;
     * holding it strongly instead would defeat the JDK's collection-triggered shutdown of such decorators.
     * Guarded by its own monitor.
     */
    private static final List<WeakReference<ExecutorService>> HOOKED_EXECUTORS = new ArrayList<>();

    /** Whether the single process-wide shutdown hook is registered. Guarded by {@code HOOKED_EXECUTORS}. */
    private static boolean shutdownHookInstalled;

    /**
     * A never-started, never-registered thread whose only use is {@code Runtime.removeShutdownHook(..)}: that call
     * throws {@link IllegalStateException} once the JVM has begun shutting down, which makes it the only
     * side-effect-free public way to detect shutdown ({@code addShutdownHook} throws then too, but otherwise
     * registers the hook). Created once, so a registration constructs no thread (and so runs no application
     * {@link InheritableThreadLocal#childValue(Object)} callback) under {@code HOOKED_EXECUTORS}.
     */
    private static final Thread SHUTDOWN_PROBE = newShutdownHookThread(() -> {
    }, "EventBus-shutdown-probe");

    private static final EventBus INSTANCE = new EventBus("default");

    /**
     * Creates a new {@code EventBus} instance with a randomly generated identifier.
     * This {@code EventBus} will use the default executor for asynchronous event delivery; on a standard JVM its
     * worker threads are daemon threads, so queued asynchronous deliveries never keep the JVM alive (see
     * {@link #getDefault()} for shutdown, worker-thread and Android behavior).
     */
    private EventBus() {
        this(Strings.uuidWithoutHyphens());
    }

    /**
     * Creates a new {@code EventBus} instance with the specified identifier.
     * This {@code EventBus} will use the default executor for asynchronous event delivery; on a standard JVM its
     * worker threads are daemon threads, so queued asynchronous deliveries never keep the JVM alive (see
     * {@link #getDefault()} for shutdown, worker-thread and Android behavior).
     *
     * @param identifier a label for this {@code EventBus}, returned by {@link #identifier()}; it need not be unique and may be {@code null}
     */
    private EventBus(final String identifier) {
        this(identifier, DEFAULT_EXECUTOR);
    }

    /**
     * Creates a new {@code EventBus} instance with the specified identifier and executor.
     * The executor is used for asynchronous event delivery when {@link ThreadMode#THREAD_POOL_EXECUTOR} is specified.
     * If a caller-supplied executor (that is, one other than the shared default executor) is an
     * {@link ExecutorService}, it is added to a weakly-held, process-wide registry whose single JVM
     * shutdown hook (registered on first use) calls {@code shutdown()} on every registered executor and
     * then waits up to 60 seconds in total for them to terminate. Creating a bus while the JVM is already
     * shutting down registers nothing. No hook is registered for the shared default executor or for a
     * plain {@link Executor}.
     *
     * @param identifier a label for this {@code EventBus}, returned by {@link #identifier()}; it need not be unique and may be {@code null}
     * @param executor the executor to use for asynchronous event delivery, or {@code null} to use the default executor
     */
    private EventBus(final String identifier, final Executor executor) {
        this.identifier = identifier;
        this.executor = executor == null ? DEFAULT_EXECUTOR : executor;

        if (executor != DEFAULT_EXECUTOR && executor instanceof ExecutorService executorService) {
            trackForShutdown(executorService);
        }
    }

    /**
     * Registers a caller-supplied executor service for shutdown at JVM exit, installing the single
     * process-wide hook on first use. Replaces the former one-hook-per-bus scheme, which leaked a hook
     * thread (and pinned the executor) for every discarded bus.
     *
     * @param executorService the executor service to shut down at JVM exit
     */
    private static void trackForShutdown(final ExecutorService executorService) {
        synchronized (HOOKED_EXECUTORS) {
            // By identity, not equals() (as a WeakHashMap would): a distinct executor service that merely equals
            // a tracked one must be tracked too. No application code runs under this process-wide lock.
            for (final Iterator<WeakReference<ExecutorService>> iter = HOOKED_EXECUTORS.iterator(); iter.hasNext();) {
                final ExecutorService tracked = iter.next().get();

                if (tracked == null) {
                    iter.remove();
                } else if (tracked == executorService) {
                    return;
                }
            }

            try {
                if (shutdownHookInstalled) {
                    // Without this probe, an executor supplied during shutdown (say, by another shutdown hook) would
                    // be shut down underneath its bus by our running hook. The probe thread is pre-built: constructing
                    // a thread here would run inherited thread-local childValue(..) callbacks under this lock, and an
                    // IllegalStateException from one would be mistaken for shutdown, leaving the executor untracked.
                    Runtime.getRuntime().removeShutdownHook(SHUTDOWN_PROBE);
                } else {
                    Runtime.getRuntime().addShutdownHook(newShutdownHookThread(EventBus::shutdownTrackedExecutors, "EventBus-shutdown"));
                    shutdownHookInstalled = true;
                }
            } catch (final IllegalStateException e) {
                // The JVM is already shutting down: register nothing, and there is nothing left to wait for.
                return;
            }

            HOOKED_EXECUTORS.add(new WeakReference<>(executorService));
        }
    }

    /**
     * Creates a JVM shutdown-hook thread that does not inherit the creating thread's context class loader,
     * inheritable thread-local values, access-control context or thread group. A registered hook lives until JVM
     * exit, so inheriting them would pin, for example, the class loader of whichever web application first touched
     * this class. Not inheriting also means constructing the thread runs no application
     * {@link InheritableThreadLocal#childValue(Object)} callbacks. The JDK gives a non-inheriting thread the system
     * class loader as its context class loader, which the hook bodies never use; setting one explicitly would need
     * {@code RuntimePermission("setContextClassLoader")} under a security manager.
     *
     * @param task the hook body
     * @param name the hook thread's name
     * @return the unstarted hook thread
     */
    @SuppressWarnings("removal")
    private static Thread newShutdownHookThread(final Runnable task, final String name) {
        // On JDK 21-23 every thread also captures the access-control context of the creating stack, even without a
        // security manager. A hook built while application code is on the stack (say, a web application's first
        // call to getDefault()) would then pin that application's class loader through its protection domain.
        // doPrivileged stops the capture at this frame; from JDK 24 it simply runs the action.
        return AccessController.doPrivileged((PrivilegedAction<Thread>) () -> new Thread(topLevelThreadGroup(), task, name, 0, false));
    }

    /**
     * Returns the top-level thread group below the system group (normally {@code "main"}), for threads this class
     * keeps for the life of the JVM or of the default executor. Using the current thread's group instead would
     * retain it permanently, and with it the class loader of an application-defined {@code ThreadGroup} subclass.
     * The system group itself is avoided because a security manager guards thread creation in it.
     *
     * @return the top-level thread group, or under a security manager the highest group whose parent may be read
     */
    private static ThreadGroup topLevelThreadGroup() {
        ThreadGroup group = Thread.currentThread().getThreadGroup();

        // g.getParent() checks access to g's parent, and the default security manager guards only the system group.
        // So a denied g.getParent() means g sits directly below the system group: g is the top-level group. Testing
        // a parent before stepping up to it matters, otherwise the walk would stop one level short of "main".
        for (;;) {
            final ThreadGroup parent;

            try {
                parent = group.getParent();
            } catch (final SecurityException e) {
                return group;
            }

            if (parent == null) {
                return group; // the current thread is in the system group itself
            }

            try {
                if (parent.getParent() == null) {
                    return group; // parent is the system group
                }
            } catch (final SecurityException e) {
                return parent;
            }

            group = parent;
        }
    }

    /**
     * Shutdown-hook body: shuts down every still-reachable registered executor service, then waits up
     * to 60 seconds in total for them to terminate.
     */
    private static void shutdownTrackedExecutors() {
        // No logging here: the logging backend installs its own shutdown hook, so logging from a hook is undefined.
        final List<ExecutorService> snapshot = new ArrayList<>();

        synchronized (HOOKED_EXECUTORS) {
            for (final WeakReference<ExecutorService> ref : HOOKED_EXECUTORS) {
                final ExecutorService executorService = ref.get();

                if (executorService != null) {
                    snapshot.add(executorService);
                }
            }
        }

        for (final ExecutorService executorService : snapshot) {
            try {
                executorService.shutdown();
            } catch (final RuntimeException e) {
                // One misbehaving executor must not stop the others from being shut down.
            }
        }

        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        for (final ExecutorService executorService : snapshot) {
            try {
                //noinspection ResultOfMethodCallIgnored
                executorService.awaitTermination(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (final RuntimeException e) {
                // An executor whose lifecycle is managed elsewhere may refuse awaitTermination; the hook thread
                // must survive it to keep waiting for the others.
            }
        }
    }

    /**
     * Returns the default {@code EventBus} instance.
     * This is a singleton instance with identifier "default" that can be used throughout the application.
     *
     * <p>Asynchronous ({@link ThreadMode#THREAD_POOL_EXECUTOR}) deliveries run on the shared default executor.
     * On a standard JVM its worker threads are daemon threads: they never keep the JVM alive, and deliveries
     * still queued when the JVM exits may be abandoned. A shutdown hook gives them up to 120 seconds to drain;
     * no hook is installed if this class is first initialized while the JVM is already shutting down. Calling
     * {@link System#exit(int)} from an asynchronous subscriber therefore delays exit by that full timeout, because
     * the hook waits for the very task that is blocked in {@code System.exit}. On Android the shared default
     * executor is {@code AsyncTask.THREAD_POOL_EXECUTOR}; {@code EventBus} neither configures its threads nor
     * installs a shutdown hook for it.</p>
     *
     * <p>On a standard JVM each default worker is created by the posting thread that first needed it, and keeps
     * that thread's context class loader and inheritable thread-local values (on JDK 21-23 also its access-control
     * context) for as long as the worker lives. A subscriber running on a worker therefore sees the context class
     * loader of whichever thread created that worker, not necessarily the current poster's. A worker ends after
     * 180 seconds without work, but one kept busy by other posts never does, so code that posts to the shared
     * default executor can keep its class loader reachable indefinitely. When this library is shared by several
     * applications (a container-level library, for example), give each application's bus its own executor through
     * {@link #create(String, Executor)} and shut that executor down when the application stops.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * EventBus eventBus = EventBus.getDefault();
     * eventBus.post("Hello World");
     * }</pre>
     *
     * @return the default {@code EventBus} instance
     */
    @SuppressFBWarnings("MS_EXPOSE_REP")
    public static EventBus getDefault() {
        return INSTANCE;
    }

    /**
     * Creates a new {@code EventBus} instance with a randomly generated identifier.
     * This {@code EventBus} will use the default executor for asynchronous event delivery; on a standard JVM its
     * worker threads are daemon threads, so queued asynchronous deliveries never keep the JVM alive (see
     * {@link #getDefault()} for shutdown, worker-thread and Android behavior).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * EventBus eventBus = EventBus.create();
     * }</pre>
     *
     * @return a new {@code EventBus} instance
     */
    public static EventBus create() {
        return new EventBus();
    }

    /**
     * Creates a new {@code EventBus} instance with the specified identifier.
     * This {@code EventBus} will use the default executor for asynchronous event delivery; on a standard JVM its
     * worker threads are daemon threads, so queued asynchronous deliveries never keep the JVM alive (see
     * {@link #getDefault()} for shutdown, worker-thread and Android behavior).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * EventBus eventBus = EventBus.create("myEventBus");
     * }</pre>
     *
     * @param identifier a label for this {@code EventBus}, returned by {@link #identifier()}; it need not be unique and may be {@code null}
     * @return a new {@code EventBus} instance with the given identifier
     */
    public static EventBus create(final String identifier) {
        return new EventBus(identifier);
    }

    /**
     * Creates a new {@code EventBus} instance with the specified identifier and executor.
     * The executor is used for asynchronous event delivery when {@link ThreadMode#THREAD_POOL_EXECUTOR} is specified.
     * If the supplied executor is an {@link ExecutorService}, it is registered, by identity, with a single
     * process-wide JVM shutdown hook. At JVM exit that hook calls {@code shutdown()} on every registered executor
     * service that is still reachable and then waits up to 60 seconds in total for them to terminate. Creating
     * many buses over one executor service adds no further hooks, and creating a bus while the JVM is already
     * shutting down registers nothing. A plain {@link Executor} is never shut down by the bus.
     *
     * <p>Registration is weak, so a discarded bus does not pin its executor service. Once the object passed here
     * becomes unreachable, the hook neither shuts it down nor waits for it. That can happen to a decorator over a
     * pool, such as {@code Executors.newSingleThreadExecutor()}, while the pool behind it is still running queued
     * deliveries; keep a reference to such an executor service, or shut it down yourself, when those deliveries
     * must complete at exit. Calling {@link System#exit(int)} from a subscriber running on a registered executor
     * delays exit by the full 60 seconds, because the hook waits for the very task that is blocked in
     * {@code System.exit}.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ExecutorService executor = Executors.newFixedThreadPool(4);
     * EventBus eventBus = EventBus.create("myEventBus", executor);
     * }</pre>
     *
     * @param identifier a label for this {@code EventBus}, returned by {@link #identifier()}; it need not be unique and may be {@code null}
     * @param executor the executor to use for asynchronous event delivery; must not be {@code null}
     * @return a new {@code EventBus} instance with the given identifier and executor
     * @throws IllegalArgumentException if {@code executor} is {@code null}.
     */
    public static EventBus create(final String identifier, final Executor executor) throws IllegalArgumentException {
        N.checkArgNotNull(executor, cs.executor);

        return new EventBus(identifier, executor);
    }

    /**
     * Returns the identifier this {@code EventBus} was created with. Identifiers are labels and need not be
     * unique: buses created with the same identifier are independent. Returns {@code null} if the bus was
     * created with a {@code null} identifier.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * EventBus eventBus = EventBus.create("myBus");
     * String id = eventBus.identifier();   // returns "myBus"
     * }</pre>
     *
     * @return the identifier of this {@code EventBus}, possibly {@code null}
     */
    public String identifier() {
        return identifier;
    }

    /**
     * Returns the subscribers that have at least one handler with no effective event ID (neither from
     * registration nor from its {@link Subscribe} annotation) that would receive an event whose runtime class
     * is {@code eventType}. A handler matches when its parameter type is {@code eventType} or a supertype of it;
     * a {@code strictEventType} handler matches only its exact parameter type. Handlers declared for a subtype
     * of {@code eventType} do not match. A primitive {@code eventType} is treated as its wrapper class, and a
     * lambda or method reference typed as {@code Subscriber<E>} itself, whose handler parameter type is then
     * {@code Object}, matches every type. Each subscriber appears at most once, in unspecified order.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Object> subscribers = eventBus.subscribers(String.class);
     * }</pre>
     *
     * @param eventType the event type to search for
     * @return a snapshot list of subscribers registered for the specified event type
     * @throws IllegalArgumentException if {@code eventType} is {@code null}.
     */
    public List<Object> subscribers(final Class<?> eventType) throws IllegalArgumentException {
        return subscribers(null, eventType);
    }

    /**
     * Returns the subscribers that have at least one handler whose effective event ID (the registration
     * override, else the {@link Subscribe} annotation's) equals {@code eventId} and that would receive an event
     * whose runtime class is {@code eventType}. Type matching follows {@link #subscribers(Class)}: a handler
     * matches when its parameter type is {@code eventType} or a supertype of it (exactly {@code eventType} for a
     * {@code strictEventType} handler), a primitive {@code eventType} is treated as its wrapper class, and a
     * lambda or method reference typed as {@code Subscriber<E>} itself matches every type. Each subscriber
     * appears at most once, in unspecified order.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Object> subscribers = eventBus.subscribers("textEvents", String.class);
     * }</pre>
     *
     * @param eventId the event ID to match, or {@code null} or empty for subscribers without an event ID
     * @param eventType the event type to search for
     * @return a snapshot list of subscribers registered for the specified event type and ID
     * @throws IllegalArgumentException if {@code eventType} is {@code null}.
     */
    public List<Object> subscribers(final String eventId, final Class<?> eventType) throws IllegalArgumentException {
        N.checkArgNotNull(eventType, cs.eventType);

        final String normalizedEventId = Strings.isEmpty(eventId) ? null : eventId;
        final Class<?> matchType = wrapIfPrimitive(eventType);
        final List<Object> eventSubs = new ArrayList<>();

        synchronized (registeredSubMap) {
            for (final Map.Entry<Object, List<SubIdentifier>> entry : registeredSubMap.entrySet()) {
                for (final SubIdentifier sub : entry.getValue()) {
                    if (sub.isMyEvent(normalizedEventId, matchType)) {
                        eventSubs.add(entry.getKey());

                        break;
                    }
                }
            }
        }

        return eventSubs;
    }

    /**
     * Returns all currently registered subscribers in this {@code EventBus}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Object> allSubscribers = eventBus.allSubscribers();
     * }</pre>
     *
     * @return a snapshot list of all currently registered subscribers
     */
    public List<Object> allSubscribers() {
        synchronized (registeredSubMap) {
            return new ArrayList<>(registeredSubMap.keySet());
        }
    }

    /**
     * Returns the number of subscribers currently registered with this {@code EventBus}.
     *
     * <p>Counts distinct subscriber instances (matching {@code allSubscribers().size()}), not the
     * number of individual {@link Subscribe @Subscribe} handler methods they expose.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (eventBus.countOfSubscribers() == 0) {
     *     // no subscribers registered
     * }
     * }</pre>
     *
     * @return the count of currently registered subscribers
     * @see #allSubscribers()
     * @see #hasSubscribers(Class)
     */
    public int countOfSubscribers() {
        synchronized (registeredSubMap) {
            return registeredSubMap.size();
        }
    }

    /**
     * Checks whether any registered subscriber would receive an event of the given type.
     *
     * <p>A subscriber matches when its {@link Subscribe @Subscribe} handler parameter type is
     * compatible with {@code eventType}: for a normal subscriber the parameter type must be
     * assignable from {@code eventType}; for a {@code strictEventType} subscriber the two types must
     * be exactly equal. The event-ID filter is intentionally ignored by this query — it reports
     * purely on event-type compatibility, so a {@code true} result does not guarantee delivery for a
     * specific event ID. A primitive {@code eventType} is treated as its wrapper class, matching how
     * primitive handler parameters are boxed. A lambda or method reference typed as {@code Subscriber<E>}
     * itself has no reified event type; its handler parameter type is {@code Object}, so it matches every
     * {@code eventType} (one whose target is a sub-interface that fixes {@code E} keeps that type).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * record OrderPlacedEvent(long orderId) {}
     * if (eventBus.hasSubscribers(OrderPlacedEvent.class)) {
     *     eventBus.post(new OrderPlacedEvent(42L));
     * }
     * }</pre>
     *
     * @param eventType the event class to test for (must not be {@code null})
     * @return {@code true} if at least one registered subscriber accepts {@code eventType}
     * @throws IllegalArgumentException if {@code eventType} is {@code null}.
     * @see #countOfSubscribers()
     */
    public boolean hasSubscribers(final Class<?> eventType) throws IllegalArgumentException {
        N.checkArgNotNull(eventType, cs.eventType);

        final Class<?> matchType = wrapIfPrimitive(eventType);

        synchronized (registeredSubMap) {
            for (final List<SubIdentifier> subs : registeredSubMap.values()) {
                for (final SubIdentifier sub : subs) {
                    if (sub.strictEventType ? sub.parameterType.equals(matchType) : sub.parameterType.isAssignableFrom(matchType)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Registers a subscriber to receive events.
     * The subscriber must either implement the {@link Subscriber} interface or have methods annotated with {@link Subscribe}.
     * No registration-level thread mode override is applied, so each subscriber method is delivered events using the thread mode declared in its own {@link Subscribe} annotation (defaulting to {@link ThreadMode#DEFAULT}).
     *
     * <p>This {@code Object} overload accepts a subscriber that exposes its event handling either through
     * {@code public}, non-{@code static}, single-argument methods annotated with {@link Subscribe}, or by
     * implementing {@link Subscriber} with a reified {@code on(...)} parameter type (a named or anonymous
     * class, as in the example below). Any {@link Subscriber} whose event type resolves to {@code Object}
     * and whose {@code on(...)} method carries no {@link Subscribe} annotation (a <b>bare lambda</b>, but
     * equally a named {@code class X implements Subscriber<Object>} or an unbound generic
     * {@code class Y<T> implements Subscriber<T>}) cannot be registered here: it has no event ID, so this
     * method throws {@link IllegalStateException}. Register such instances through
     * {@link #register(Subscriber, String)} or {@link #register(Subscriber, String, ThreadMode)}, which
     * require an event ID, or annotate the {@code on(...)} method with {@link Subscribe} instead.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.register(new Subscriber<String>() {
     *     @Override
     *     public void on(String event) {
     *         System.out.println("Received: " + event);
     *     }
     * });
     * }</pre>
     *
     * @param subscriber the subscriber to register
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code subscriber} is {@code null}, if inherited {@code @Subscribe} declarations of a handler method
     *         conflict, or if no subscriber methods are found in the subscriber class.
     * @throws RuntimeException if a {@code @Subscribe} method is {@code static} (whatever its visibility), or is {@code public} and does not
     *         declare exactly one parameter, or ({@link java.lang.reflect.InaccessibleObjectException}) if a handler method cannot be made
     *         accessible, for example one in a non-public class whose package is in a named module not opened to this library
     * @throws IllegalStateException if the subscriber is identified as a lambda subscriber (an event ID is required for lambda subscribers)
     */
    public EventBus register(final Object subscriber) throws IllegalArgumentException, RuntimeException, IllegalStateException {
        return register(subscriber, (ThreadMode) null);
    }

    /**
     * Registers a subscriber with an optional event ID override.
     * When {@code eventId} is non-empty, every handler of the subscriber, including handlers whose {@link Subscribe}
     * annotation declares a different event ID, receives only events posted with that ID. When {@code eventId} is
     * {@code null} or empty, each handler keeps its annotation-level event ID, or no ID if none is declared, as with
     * {@link #register(Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.register(mySubscriber, "userEvents");
     * // This subscriber will only receive events posted with "userEvents" ID
     * }</pre>
     *
     * @param subscriber the subscriber to register
     * @param eventId the event ID override, or {@code null} or empty to keep each handler's annotation-level event ID;
     *                must be non-empty for lambda-based subscribers
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code subscriber} is {@code null}, if inherited {@code @Subscribe} declarations of a handler method
     *         conflict, or if no subscriber methods are found in the subscriber class.
     * @throws RuntimeException if a {@code @Subscribe} method is {@code static} (whatever its visibility), or is {@code public} and does not
     *         declare exactly one parameter, or ({@link java.lang.reflect.InaccessibleObjectException}) if a handler method cannot be made
     *         accessible, for example one in a non-public class whose package is in a named module not opened to this library
     * @throws IllegalStateException if the subscriber is identified as a lambda subscriber and {@code eventId} is empty or {@code null}
     */
    public EventBus register(final Object subscriber, final String eventId) throws IllegalArgumentException, RuntimeException, IllegalStateException {
        return register(subscriber, eventId, null);
    }

    /**
     * Registers a subscriber with a specific thread mode.
     * The thread mode determines on which thread the event will be delivered.
     * Lambda-based subscribers cannot be registered with this method; use
     * {@link #register(Subscriber, String, ThreadMode)} instead.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.register(mySubscriber, ThreadMode.THREAD_POOL_EXECUTOR);
     * // Events will be delivered on a background thread
     * }</pre>
     *
     * @param subscriber the subscriber to register
     * @param threadMode the thread mode override for event delivery, or {@code null} to use the thread mode declared in each subscriber method's {@link Subscribe} annotation
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code subscriber} is {@code null}, if {@code threadMode} is not supported, if inherited {@code @Subscribe}
     *         declarations of a handler method conflict, or if no subscriber methods are found in the subscriber class.
     * @throws RuntimeException if a {@code @Subscribe} method is {@code static} (whatever its visibility), or is {@code public} and does not
     *         declare exactly one parameter, or ({@link java.lang.reflect.InaccessibleObjectException}) if a handler method cannot be made
     *         accessible, for example one in a non-public class whose package is in a named module not opened to this library
     * @throws IllegalStateException if the subscriber is identified as a lambda subscriber (an event ID is required for lambda subscribers)
     */
    public EventBus register(final Object subscriber, final ThreadMode threadMode) throws IllegalArgumentException, RuntimeException, IllegalStateException {
        return register(subscriber, null, threadMode);
    }

    /**
     * Registers a subscriber with both event ID and thread mode specifications.
     * This is the most comprehensive registration method that allows full control over event filtering and delivery.
     * If the subscriber has sticky event handling enabled, it will immediately receive any matching sticky events;
     * delivery of those sticky events respects each handler's effective {@link ThreadMode} (the registration
     * override if given, otherwise the annotation's), so a {@code THREAD_POOL_EXECUTOR} sticky delivery is handed
     * to this bus's executor: with the default executor it runs on a background thread, but a caller-supplied
     * direct executor (such as {@code Runnable::run}) runs it on the registering thread before {@code register}
     * returns.
     *
     * <p>Subscriber identity is tracked by reference (an {@link IdentityHashMap}). Re-registering the
     * <em>same</em> subscriber instance <em>replaces</em> its previous registration rather than adding a second
     * one: the old handlers are removed and the new {@code eventId}/{@code threadMode} take effect. The
     * replacement starts with fresh interval-throttling and deduplication history, and its sticky handlers
     * replay every matching retained sticky event again, exactly as on a first registration. Deliveries already
     * selected or queued for the old registration may still run.
     *
     * <p><b>Registration overrides cover only {@code eventId} and {@code threadMode}.</b> The four
     * advanced delivery knobs &mdash; {@code sticky}, {@code strictEventType}, {@code intervalMillis}
     * (throttling) and {@code deduplicate} &mdash; are <b>annotation-only</b>: they can be configured
     * solely through a {@link Subscribe @Subscribe}-annotated method and there is intentionally no
     * registration-time override for them. Consequently a {@link Subscriber} registration whose
     * {@code on(...)} method carries no {@code @Subscribe} annotation (for example a plain lambda or method
     * reference) can never be sticky, strict, throttled, or deduplicated; to use any of those features,
     * register an object whose method is annotated with {@code @Subscribe} (a {@code Subscriber}
     * implementation may annotate its own {@code on(...)} method).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.register(mySubscriber, "userEvents", ThreadMode.THREAD_POOL_EXECUTOR);
     * }</pre>
     *
     * @param subscriber the subscriber to register
     * @param eventId the event ID override; must be non-empty for lambda-based subscribers; {@code null} or empty
     *                preserves each handler's annotation-level event ID (or no ID if none is declared)
     * @param threadMode the thread mode override for event delivery, or {@code null} to use the thread mode declared in each subscriber method's {@link Subscribe} annotation
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code subscriber} is {@code null}, if {@code threadMode} is not supported, if inherited {@code @Subscribe}
     *         declarations of a handler method conflict, or if no subscriber methods are found in the subscriber class.
     * @throws RuntimeException if a {@code @Subscribe} method is {@code static} (whatever its visibility), or is {@code public} and does not
     *         declare exactly one parameter, or ({@link java.lang.reflect.InaccessibleObjectException}) if a handler method cannot be made
     *         accessible, for example one in a non-public class whose package is in a named module not opened to this library
     * @throws IllegalStateException if the subscriber is identified as a lambda subscriber and {@code eventId} is empty or {@code null}
     */
    public EventBus register(final Object subscriber, final String eventId, final ThreadMode threadMode)
            throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgNotNull(subscriber, cs.subscriber);

        if (!isSupportedThreadMode(threadMode)) {
            throw new IllegalArgumentException("Unsupported thread mode: " + threadMode);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Registering subscriber: {} with eventId: {} and thread mode: {}", diagnosticString(subscriber), eventId, threadMode);
        }

        final Class<?> subscriberClass = subscriber.getClass();
        final List<SubIdentifier> subIdentifiers = getClassSubList(subscriberClass);

        if (N.isEmpty(subIdentifiers)) {
            throw new IllegalArgumentException("No subscriber method found in class: " + ClassUtil.getCanonicalClassName(subscriberClass));
        }

        final List<SubIdentifier> eventSubList = new ArrayList<>(subIdentifiers.size());

        for (final SubIdentifier sub : subIdentifiers) {
            if (sub.isPossibleLambdaSubscriber && Strings.isEmpty(eventId)) {
                throw new IllegalStateException("Lambda subscribers must be registered with an event ID");
            }

            eventSubList.add(new SubIdentifier(sub, subscriber, eventId, threadMode));
        }

        Set<SubIdentifier> oldSubEvents = null;
        Map<Object, String> stickySnapshot = null;
        boolean hasStickySub = false;

        // Make the subscriber visible (registeredSubMap / registeredEventIdSubMap) AND snapshot the sticky
        // events to replay, atomically under stickyDeliveryLock — but do NOT dispatch while holding it.
        // This is what guarantees a sticky event reaches this subscriber exactly once relative to a
        // concurrent postSticky(...) (see the stickyDeliveryLock field doc).
        synchronized (stickyDeliveryLock) {
            synchronized (registeredSubMap) {
                final List<SubIdentifier> removed = registeredSubMap.put(subscriber, eventSubList);
                oldSubEvents = removed == null ? null : N.newHashSet(removed);
                listOfSubEventSubs = null;
            }

            if (N.notEmpty(oldSubEvents)) {
                removeFromEventIdSubMap(oldSubEvents);
            }

            if (Strings.isEmpty(eventId)) {
                synchronized (registeredEventIdSubMap) {
                    for (final SubIdentifier sub : eventSubList) {
                        if (Strings.isEmpty(sub.eventId)) {
                            continue;
                        }

                        final Set<SubIdentifier> eventSubs = registeredEventIdSubMap.get(sub.eventId);

                        if (eventSubs == null) {
                            registeredEventIdSubMap.put(sub.eventId, N.toLinkedHashSet(sub));
                        } else {
                            eventSubs.add(sub);
                        }

                        listOfEventIdSubMap.remove(sub.eventId);
                    }
                }
            } else {
                synchronized (registeredEventIdSubMap) {
                    final Set<SubIdentifier> eventSubs = registeredEventIdSubMap.get(eventId);

                    if (eventSubs == null) {
                        registeredEventIdSubMap.put(eventId, N.newLinkedHashSet(eventSubList));
                    } else {
                        eventSubs.addAll(eventSubList);
                    }

                    listOfEventIdSubMap.remove(eventId);
                }
            }

            for (final SubIdentifier sub : eventSubList) {
                if (sub.sticky) {
                    hasStickySub = true;
                    break;
                }
            }

            if (hasStickySub) {
                stickySnapshot = mapOfStickyEvent;

                if (stickySnapshot == null) {
                    synchronized (stickyEventMap) {
                        stickySnapshot = new IdentityHashMap<>(stickyEventMap);
                        mapOfStickyEvent = stickySnapshot;
                    }
                }
            }
        }

        // Replay matching sticky events to the newly-registered sticky subscribers OUTSIDE the lock
        // (dispatch may run subscriber code synchronously on this thread).
        if (hasStickySub) {
            for (final SubIdentifier sub : eventSubList) {
                if (sub.sticky) {
                    for (final Map.Entry<Object, String> entry : stickySnapshot.entrySet()) {
                        if (sub.isMyEvent(entry.getValue(), entry.getKey().getClass())) {
                            try {
                                dispatch(sub, entry.getKey());
                            } catch (final Exception e) {
                                logger.error("Failed to post sticky event: " + diagnosticString(entry.getKey()) + " with eventId: "
                                        + N.toString(entry.getValue()) + " to subscriber: " + diagnosticString(sub), e); //NOSONAR
                            }
                        }
                    }
                }
            }
        }

        return this;
    }

    /**
     * Returns the cached list of prototype {@link SubIdentifier}s for the given subscriber class,
     * building it on first use. The class and all of its supertypes (superclasses and interfaces) are
     * scanned for {@code public}, non-{@code static}, single-argument methods annotated with
     * {@link Subscribe}; if the class also implements {@link Subscriber}, its {@code on(...)} method is
     * added when no annotated method with the same resolved event signature, and no annotated {@code on(Object)}
     * override, was already found. The result is shared across all {@code EventBus} instances and is never
     * {@code null}, but may be empty.
     *
     * @param subscriberClass the subscriber class to introspect
     * @return the prototype subscriber-method descriptors for {@code subscriberClass}, possibly empty
     * @throws RuntimeException if a {@code @Subscribe} method is {@code static}, or is {@code public} and does not
     *         declare exactly one parameter, or if a handler method cannot be made accessible
     * @throws IllegalArgumentException if equally specific inherited {@code @Subscribe} declarations of a handler method conflict
     */
    private List<SubIdentifier> getClassSubList(final Class<?> subscriberClass) throws RuntimeException, IllegalArgumentException {
        return classMetaSubMap.get(subscriberClass);
    }

    private static List<SubIdentifier> discoverSubscriberMethods(final Class<?> subscriberClass) {
        final Map<String, List<SubIdentifier>> annotatedMethods = new LinkedHashMap<>();
        // The JVM overrides by name and erased parameter types. Below a raw supertype reference an override keeps
        // the erased parameter type (handle(Object)) while the overridden declaration still resolves its type
        // argument (handle(T) with T = String), so their resolved signatures differ although both name one virtual
        // method - which was then registered, and invoked, twice. Every declaration is therefore filed under the
        // group that first claimed its erased signature (subtypes are scanned before their supertypes).
        final Map<String, String> groupByErasedSignature = new HashMap<>();
        final Map<Class<?>, Map<TypeVariable<?>, Class<?>>> typeBindings = new LinkedHashMap<>();
        collectTypeBindings(subscriberClass, Collections.emptyMap(), typeBindings);

        for (final Class<?> supertype : typeBindings.keySet()) {
            for (final Method method : supertype.getDeclaredMethods()) {
                if (method.isAnnotationPresent(Subscribe.class) && !method.isSynthetic()) {
                    if (Modifier.isStatic(method.getModifiers())) {
                        throw new RuntimeException(
                                "Subscriber method must not be static: " + method + " in class: " + ClassUtil.getCanonicalClassName(supertype));
                    }

                    if (!Modifier.isPublic(method.getModifiers())) {
                        continue;
                    }

                    final Class<?>[] parameterTypes = method.getParameterTypes();

                    if (parameterTypes.length != 1) {
                        throw new RuntimeException(
                                method.getName() + " has " + parameterTypes.length + " parameters. Subscriber method must have exactly 1 parameter.");
                    }

                    // Match on the specialized event type, while reflection still invokes
                    // the original declaration. This unifies generic overrides without
                    // registering compiler bridges as additional handlers.
                    final Class<?> eventType = eventType(method.getGenericParameterTypes()[0], typeBindings.get(supertype));
                    final String resolvedSignature = method.getName() + "#" + eventType.getName();
                    final String methodSignature = groupByErasedSignature.computeIfAbsent(method.getName() + "#" + parameterTypes[0].getName(),
                            key -> resolvedSignature);
                    annotatedMethods.computeIfAbsent(methodSignature, key -> new ArrayList<>()).add(new SubIdentifier(method, eventType));
                }
            }
        }

        final Map<String, SubIdentifier> subscriberMethodMap = new LinkedHashMap<>();
        for (final Map.Entry<String, List<SubIdentifier>> entry : annotatedMethods.entrySet()) {
            subscriberMethodMap.put(entry.getKey(), mostSpecificSubscriber(entry.getValue()));
        }

        // An annotated on(..) whose erased signature is on(Object) already is the Subscriber contract method.
        if (Subscriber.class.isAssignableFrom(subscriberClass) && !groupByErasedSignature.containsKey("on#" + Object.class.getName())) {
            try {
                // Invoke the interface contract, never an arbitrary overload named on.
                final Method method = Subscriber.class.getMethod("on", Object.class);
                final Class<?> eventType = eventType(method.getGenericParameterTypes()[0], typeBindings.get(Subscriber.class));
                subscriberMethodMap.putIfAbsent("on#" + eventType.getName(), new SubIdentifier(method, eventType));
            } catch (final NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }

        return new ArrayList<>(subscriberMethodMap.values());
    }

    /**
     * @throws IllegalArgumentException if equally specific subscriber declarations have conflicting subscription settings
     */
    private static SubIdentifier mostSpecificSubscriber(final List<SubIdentifier> candidates) throws IllegalArgumentException {
        // Decide after collecting the whole hierarchy: a concrete override can resolve
        // otherwise conflicting annotations inherited from two unrelated interfaces.
        SubIdentifier selected = null;
        for (final SubIdentifier candidate : candidates) {
            final Class<?> declaringClass = candidate.method.getDeclaringClass();
            boolean overridden = false;
            for (final SubIdentifier other : candidates) {
                final Class<?> otherClass = other.method.getDeclaringClass();
                if (declaringClass != otherClass
                        && (declaringClass.isAssignableFrom(otherClass) || declaringClass.isInterface() && !otherClass.isInterface())) {
                    overridden = true;
                    break;
                }
            }
            if (!overridden) {
                if (selected != null && !selected.method.getAnnotation(Subscribe.class).equals(candidate.method.getAnnotation(Subscribe.class))) {
                    throw new IllegalArgumentException("Conflicting @Subscribe declarations: " + selected.method + " and " + candidate.method);
                }
                selected = candidate;
            }
        }
        return selected;
    }

    private static void collectTypeBindings(final java.lang.reflect.Type type, final Map<TypeVariable<?>, Class<?>> inherited,
            final Map<Class<?>, Map<TypeVariable<?>, Class<?>>> result) {
        if (type == null) {
            return;
        }
        final Class<?> rawType = eventType(type, inherited);
        if (result.containsKey(rawType)) {
            return;
        }
        final Map<TypeVariable<?>, Class<?>> bindings = new HashMap<>(inherited);
        if (type instanceof ParameterizedType parameterizedType) {
            bindTypeParameters(parameterizedType, bindings);
        }
        result.put(rawType, bindings);
        collectTypeBindings(rawType.getGenericSuperclass(), bindings, result);
        for (final java.lang.reflect.Type interfaceType : rawType.getGenericInterfaces()) {
            collectTypeBindings(interfaceType, bindings, result);
        }
    }

    private static void bindTypeParameters(final ParameterizedType type, final Map<TypeVariable<?>, Class<?>> bindings) {
        final TypeVariable<?>[] variables = ((Class<?>) type.getRawType()).getTypeParameters();
        final java.lang.reflect.Type[] arguments = type.getActualTypeArguments();
        // Resolve in the incoming lexical context before rebinding an owner or superclass
        // that swaps the same variables (for example Outer<B, A>.Base<A>).
        final Map<TypeVariable<?>, Class<?>> additions = new HashMap<>();
        for (int i = 0; i < variables.length; i++) {
            additions.put(variables[i], eventType(arguments[i], bindings));
        }
        if (type.getOwnerType() instanceof ParameterizedType owner) {
            bindTypeParameters(owner, bindings);
        }
        bindings.putAll(additions);
    }

    private static Class<?> eventType(final java.lang.reflect.Type type, final Map<TypeVariable<?>, Class<?>> bindings) {
        return eventType(type, bindings, new HashSet<>());
    }

    private static Class<?> eventType(final java.lang.reflect.Type type, final Map<TypeVariable<?>, Class<?>> bindings, final Set<TypeVariable<?>> resolving) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType parameterizedType) {
            return (Class<?>) parameterizedType.getRawType();
        }
        if (type instanceof GenericArrayType arrayType) {
            return java.lang.reflect.Array.newInstance(eventType(arrayType.getGenericComponentType(), bindings, resolving), 0).getClass();
        }
        if (type instanceof TypeVariable<?> variable) {
            final Class<?> resolved = bindings.get(variable);
            if (resolved != null) {
                return resolved;
            }
            if (resolving.add(variable)) {
                return eventType(variable.getBounds()[0], bindings, resolving);
            }
        }
        if (type instanceof WildcardType wildcard) {
            return eventType(wildcard.getUpperBounds()[0], bindings, resolving);
        }
        return Object.class;
    }

    /**
     * Registers a {@link Subscriber} interface implementation with a specific event ID.
     * This method is specifically designed for lambda expressions and anonymous inner classes
     * that implement the {@link Subscriber} interface.
     *
     * <p>A lambda or method reference typed as {@code Subscriber<E>} itself has no runtime type argument, so it
     * is registered for event type {@code Object} and is invoked for every event posted with {@code eventId}; an
     * event it cannot accept fails inside the callback, and that failure is logged. Post only events it accepts
     * on that ID, or filter by type with a named or anonymous class, or a lambda targeting a sub-interface that
     * fixes {@code E}, each of which keeps its type argument.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.register((Subscriber<String>) event -> {
     *     System.out.println("Lambda received: " + event);
     * }, "stringEvents");
     * }</pre>
     *
     * @param subscriber the {@code Subscriber} implementation to register
     * @param eventId the event ID to filter events; must be non-empty when the subscriber is identified as a lambda subscriber
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code subscriber} is {@code null}, if inherited {@code @Subscribe} declarations of a handler method
     *         conflict, or if no subscriber methods are found.
     * @throws RuntimeException if a {@code @Subscribe} method is {@code static} (whatever its visibility), or is {@code public} and does not
     *         declare exactly one parameter, or ({@link java.lang.reflect.InaccessibleObjectException}) if a handler method cannot be made
     *         accessible, for example one in a non-public class whose package is in a named module not opened to this library
     * @throws IllegalStateException if the subscriber is identified as a lambda subscriber and {@code eventId} is empty or {@code null}
     */
    public EventBus register(final Subscriber<?> subscriber, final String eventId) throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgNotNull(subscriber, cs.subscriber);

        return register(subscriber, eventId, null);
    }

    /**
     * Registers a {@link Subscriber} interface implementation with both event ID and thread mode.
     * This method provides full control over how lambda-based subscribers receive events.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.register((Subscriber<User>) user -> {
     *     // Process user on background thread
     *     processUser(user);
     * }, "userUpdates", ThreadMode.THREAD_POOL_EXECUTOR);
     * }</pre>
     *
     * @param subscriber the {@code Subscriber} implementation to register
     * @param eventId the event ID to filter events; must be non-empty when the subscriber is identified as a lambda subscriber
     * @param threadMode the thread mode override for event delivery, or {@code null} to use the thread mode declared in each subscriber method's {@link Subscribe} annotation
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code subscriber} is {@code null}, if {@code threadMode} is not supported, if inherited {@code @Subscribe}
     *         declarations of a handler method conflict, or if no subscriber methods are found.
     * @throws RuntimeException if a {@code @Subscribe} method is {@code static} (whatever its visibility), or is {@code public} and does not
     *         declare exactly one parameter, or ({@link java.lang.reflect.InaccessibleObjectException}) if a handler method cannot be made
     *         accessible, for example one in a non-public class whose package is in a named module not opened to this library
     * @throws IllegalStateException if the subscriber is identified as a lambda subscriber and {@code eventId} is empty or {@code null}
     */
    public EventBus register(final Subscriber<?> subscriber, final String eventId, final ThreadMode threadMode)
            throws IllegalArgumentException, RuntimeException, IllegalStateException {
        N.checkArgNotNull(subscriber, cs.subscriber);

        final Object tmp = subscriber;
        return register(tmp, eventId, threadMode);
    }

    /**
     * Unregisters a previously registered subscriber.
     * All event subscriptions for this subscriber will be removed.
     * This method should be called when a subscriber is no longer needed to prevent memory leaks.
     * If the subscriber was not previously registered, this method does nothing.
     * Concurrent registration and unregistration update the subscriber registry and event-ID index together.
     * Events already selected for dispatch, including queued asynchronous deliveries, may still be delivered.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.unregister(mySubscriber);
     * // Future posts will not select mySubscriber; deliveries already queued may still run.
     * }</pre>
     *
     * @param subscriber the subscriber to unregister
     * @return this {@code EventBus} instance for method chaining
     */
    public EventBus unregister(final Object subscriber) {
        if (logger.isDebugEnabled()) {
            logger.debug("Unregistering subscriber: {}", diagnosticString(subscriber));
        }

        Set<SubIdentifier> subEvents = null;

        synchronized (stickyDeliveryLock) {
            synchronized (registeredSubMap) {
                final List<SubIdentifier> removed = registeredSubMap.remove(subscriber);
                subEvents = removed == null ? null : N.newHashSet(removed);
                listOfSubEventSubs = null;
            }

            if (N.notEmpty(subEvents)) {
                removeFromEventIdSubMap(subEvents);
            }
        }

        return this;
    }

    /**
     * Removes the given subscriber identifiers from the event-id index ({@code registeredEventIdSubMap})
     * and invalidates the corresponding cached snapshots in {@code listOfEventIdSubMap}. Any event ID
     * whose subscriber set becomes empty as a result is removed from the index entirely.
     *
     * @param subEventsToRemove the subscriber identifiers to remove; must be non-empty
     */
    private void removeFromEventIdSubMap(final Set<SubIdentifier> subEventsToRemove) {
        synchronized (registeredEventIdSubMap) {
            /*
             * Every identifier in the index is filed under its own (non-empty) eventId - see register(...) - so
             * only those keys can be affected. Looking them up directly keeps unregister O(identifiers removed)
             * instead of O(all event IDs), and leaves the cached delivery lists of unaffected event IDs intact
             * (they are only ever built from, and invalidated with, the index under this same lock).
             */
            for (final SubIdentifier sub : subEventsToRemove) {
                if (sub.eventId == null) {
                    continue;
                }

                final Set<SubIdentifier> eventSubs = registeredEventIdSubMap.get(sub.eventId);

                if (eventSubs != null && eventSubs.remove(sub)) {
                    if (eventSubs.isEmpty()) {
                        registeredEventIdSubMap.remove(sub.eventId);
                    }

                    listOfEventIdSubMap.remove(sub.eventId);
                }
            }
        }
    }

    /**
     * Posts an event to subscribers that have no event ID filter.
     * The event is delivered to every handler that has no effective event ID (its subscriber was registered
     * without an event ID and its {@link Subscribe} annotation declares none) and whose parameter type is
     * assignable from the event's runtime type (or, for a {@code strictEventType} handler, equal to it).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.post("Hello World");
     * eventBus.post(new UserLoginEvent(userId));
     * }</pre>
     *
     * @param event the event to post
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code event} is {@code null}.
     */
    public EventBus post(final Object event) throws IllegalArgumentException {
        return post((String) null, event);
    }

    /**
     * Posts an event with an optional event ID.
     * When {@code eventId} is non-{@code null} and non-empty, the event is delivered only to handlers whose
     * effective event ID equals it: handlers of subscribers registered with that event ID, and handlers whose
     * {@link Subscribe#eventId()} specifies it when their subscriber was registered without an event ID (a
     * registration-time event ID replaces the annotation's). The handler must also accept the event's runtime
     * type (exactly, for a {@code strictEventType} handler).
     * When {@code eventId} is {@code null} or empty, the event is delivered to all subscribers that
     * have no event ID filter (equivalent to calling {@link #post(Object)}).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.post("userEvents", new UserLoginEvent(userId));
     * // Only subscribers associated with "userEvents" will receive this
     * }</pre>
     *
     * @param eventId the event ID for filtering subscribers, or {@code null} (or empty) to deliver to subscribers without a specific event ID
     * @param event the event to post
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code event} is {@code null}.
     */
    public EventBus post(final String eventId, final Object event) throws IllegalArgumentException {
        N.checkArgNotNull(event, cs.event);

        final String normalizedEventId = Strings.isEmpty(eventId) ? null : eventId;

        dispatchToSubscribers(resolveSubscriberLists(normalizedEventId), normalizedEventId, event);

        return this;
    }

    /**
     * Resolves (and lazily caches) the list of candidate subscriber lists for the given normalized
     * event ID. Only snapshots for registered IDs are cached; absent IDs are not retained.
     * This is the snapshot/decision step of a post; it acquires the relevant map lock briefly
     * but performs no dispatch. {@link #postSticky} calls this while holding {@link #stickyDeliveryLock}
     * so that its delivery decision is ordered against a concurrent {@link #register}.
     */
    private List<List<SubIdentifier>> resolveSubscriberLists(final String normalizedEventId) {
        List<List<SubIdentifier>> subscriberLists = listOfSubEventSubs;

        if (normalizedEventId == null) {
            if (subscriberLists == null) {
                synchronized (registeredSubMap) {
                    subscriberLists = listOfSubEventSubs; // re-read inside lock to avoid double initialisation
                    if (subscriberLists == null) {
                        subscriberLists = new ArrayList<>(registeredSubMap.values()); // in case concurrent register/unregister.
                        listOfSubEventSubs = subscriberLists;
                    }
                }
            }
        } else {
            List<SubIdentifier> eventIdSubscribers = listOfEventIdSubMap.get(normalizedEventId);

            if (eventIdSubscribers == null) {
                synchronized (registeredEventIdSubMap) {
                    eventIdSubscribers = listOfEventIdSubMap.get(normalizedEventId); // re-read inside lock to avoid double initialisation
                    if (eventIdSubscribers == null) {
                        if (registeredEventIdSubMap.containsKey(normalizedEventId)) {
                            eventIdSubscribers = new ArrayList<>(registeredEventIdSubMap.get(normalizedEventId)); // in case concurrent register/unregister.
                            listOfEventIdSubMap.put(normalizedEventId, eventIdSubscribers);
                        } else {
                            // Transient routing IDs must not accumulate forever as negative cache entries.
                            eventIdSubscribers = N.emptyList();
                        }
                    }
                }
            }

            subscriberLists = Collections.singletonList(eventIdSubscribers);
        }

        return subscriberLists;
    }

    /**
     * Dispatches {@code event} to every matching subscriber in the previously-resolved lists. This is
     * always called OUTSIDE {@link #stickyDeliveryLock} because {@code dispatch(...)} may execute
     * subscriber code synchronously on the calling thread.
     */
    private void dispatchToSubscribers(final List<List<SubIdentifier>> subscriberLists, final String normalizedEventId, final Object event) {
        final Class<?> eventClass = event.getClass();

        for (final List<SubIdentifier> subscribers : subscriberLists) {
            for (final SubIdentifier subscriber : subscribers) {
                if (subscriber.isMyEvent(normalizedEventId, eventClass)) {
                    try {
                        dispatch(subscriber, event);
                    } catch (final Exception e) {
                        logger.error("Failed to post event: " + diagnosticString(event) + " to subscriber: " + diagnosticString(subscriber), e);
                    }
                }
            }
        }
    }

    /**
     * Posts a sticky event without an event ID. It is delivered immediately to current matching subscribers
     * that have no event ID filter (as {@link #post(Object)} does) and retained, by identity, for replay to
     * matching subscribers without an event ID that register later with {@code sticky=true} in their
     * {@link Subscribe} annotation. Posting an instance that is already retained (under any event ID) moves it
     * to "no event ID" and delivers it again; equal but distinct instances are retained separately.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * eventBus.postSticky(new AppConfiguration());
     * // Later registered subscribers with sticky=true will receive this event
     * }</pre>
     *
     * @param event the sticky event to post
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code event} is {@code null}.
     */
    public EventBus postSticky(final Object event) throws IllegalArgumentException {
        return postSticky(null, event);
    }

    /**
     * Posts a sticky event with a specific event ID.
     * The sticky event will be immediately delivered to current matching subscribers and retained for delivery to
     * matching subscribers that register later with {@code sticky=true} in their {@link Subscribe} annotation.
     * Sticky events are retained by identity ({@code ==}), not {@code equals()}: posting an instance that is
     * already retained replaces its event ID (and delivers it again to current matching subscribers), while
     * equal but distinct instances are each retained.
     * When several retained sticky events match a newly registering sticky subscriber, the order in which
     * they are replayed to it is unspecified.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Configuration config = new Configuration();
     * eventBus.postSticky("appConfig", config);
     * // Future subscribers for "appConfig" with sticky=true will receive this
     * }</pre>
     *
     * @param eventId the event ID to associate with the sticky event, or {@code null} (or empty) to retain it without a specific event ID
     * @param event the sticky event to post
     * @return this {@code EventBus} instance for method chaining
     * @throws IllegalArgumentException if {@code event} is {@code null}.
     */
    public EventBus postSticky(final String eventId, final Object event) throws IllegalArgumentException {
        N.checkArgNotNull(event, cs.event);

        final String normalizedEventId = Strings.isEmpty(eventId) ? null : eventId;

        final List<List<SubIdentifier>> subscriberLists;

        // Record the sticky event AND resolve its delivery list atomically under stickyDeliveryLock so
        // that, relative to a concurrent register(...), this event is delivered to a newly-registering
        // sticky subscriber exactly once: either here (subscriber already visible) or by register's
        // replay (event already recorded), never both. dispatch happens after the lock is released.
        synchronized (stickyDeliveryLock) {
            synchronized (stickyEventMap) {
                stickyEventMap.put(event, normalizedEventId);

                mapOfStickyEvent = null;
            }

            subscriberLists = resolveSubscriberLists(normalizedEventId);
        }

        dispatchToSubscribers(subscriberLists, normalizedEventId, event);

        return this;
    }

    /**
     * Removes {@code event} if that exact instance ({@code ==}, not {@code equals()}) is currently retained
     * without an event ID; it will then no longer be replayed to future subscribers. An instance retained under
     * an event ID is left in place; use {@link #removeStickyEvent(String, Object)}. A {@code null} event is never
     * retained, so it returns {@code false}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Configuration config = getConfiguration();
     * boolean removed = eventBus.removeStickyEvent(config);
     * }</pre>
     *
     * @param event the sticky event to remove
     * @return {@code true} if the event was removed, {@code false} otherwise
     */
    public boolean removeStickyEvent(final Object event) {
        return removeStickyEvent(null, event);
    }

    /**
     * Removes {@code event} if that exact instance ({@code ==}, not {@code equals()}) is currently retained under
     * {@code eventId} ({@code null} or empty meaning no event ID).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Configuration config = getConfiguration();
     * boolean removed = eventBus.removeStickyEvent("appConfig", config);
     * }</pre>
     *
     * @param eventId the event ID the sticky event was posted with, or {@code null} or empty for no event ID
     * @param event the sticky event to remove
     * @return {@code true} if the event was removed, {@code false} otherwise
     */
    public boolean removeStickyEvent(final String eventId, final Object event) {
        final String normalizedEventId = Strings.isEmpty(eventId) ? null : eventId;

        synchronized (stickyEventMap) {
            final String val = stickyEventMap.get(event);
            final String normalizedStoredEventId = Strings.isEmpty(val) ? null : val;

            if (N.equals(normalizedStoredEventId, normalizedEventId) && (val != null || stickyEventMap.containsKey(event))) {
                stickyEventMap.remove(event);

                mapOfStickyEvent = null;
                return true;
            }
        }

        return false;
    }

    /**
     * Removes all sticky events of a specific type that were posted without an event ID.
     * This is useful for clearing all sticky events of a particular class.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean removed = eventBus.removeStickyEvents(UserSession.class);
     * // All UserSession sticky events with null event ID are removed
     * }</pre>
     *
     * @param eventType the class type of sticky events to remove; a primitive class matches its wrapper type
     * @return {@code true} if one or more events were removed, {@code false} otherwise
     * @throws IllegalArgumentException if {@code eventType} is {@code null}.
     */
    public boolean removeStickyEvents(final Class<?> eventType) throws IllegalArgumentException {
        return removeStickyEvents(null, eventType);
    }

    /**
     * Removes all sticky events of a specific type that were posted with the specified event ID.
     * This allows targeted removal of sticky events by both type and ID.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean removed = eventBus.removeStickyEvents("userSessions", UserSession.class);
     * // All UserSession sticky events with "userSessions" ID are removed
     * }</pre>
     *
     * @param eventId the event ID to match, or {@code null} or empty for events retained without an event ID
     * @param eventType the class type of sticky events to remove; a primitive class matches its wrapper type
     * @return {@code true} if one or more events were removed, {@code false} otherwise
     * @throws IllegalArgumentException if {@code eventType} is {@code null}.
     */
    public boolean removeStickyEvents(final String eventId, final Class<?> eventType) throws IllegalArgumentException {
        N.checkArgNotNull(eventType, cs.eventType);

        final String normalizedEventId = Strings.isEmpty(eventId) ? null : eventId;
        final Class<?> matchType = wrapIfPrimitive(eventType);
        final List<Object> keyToRemove = new ArrayList<>();

        synchronized (stickyEventMap) {
            for (final Map.Entry<Object, String> entry : stickyEventMap.entrySet()) {
                if (N.equals(Strings.isEmpty(entry.getValue()) ? null : entry.getValue(), normalizedEventId)
                        && matchType.isAssignableFrom(entry.getKey().getClass())) {
                    keyToRemove.add(entry.getKey());
                }
            }

            if (N.notEmpty(keyToRemove)) {
                for (final Object event : keyToRemove) {
                    stickyEventMap.remove(event);
                }

                mapOfStickyEvent = null;

                return true;
            }
        }

        return false;
    }

    /**
     * Removes all sticky events from this {@code EventBus}.
     * After calling this method, no sticky events will be delivered to future subscribers.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean removed = eventBus.removeAllStickyEvents();
     * // All sticky events are cleared
     * }</pre>
     *
     * @return {@code true} if one or more sticky events were removed, {@code false} if there were
     *         no sticky events to remove. The return type mirrors {@link #removeStickyEvent(Object)}
     *         and {@link #removeStickyEvents(Class)} for family consistency.
     */
    public boolean removeAllStickyEvents() {
        synchronized (stickyEventMap) {
            if (stickyEventMap.isEmpty()) {
                return false;
            }

            stickyEventMap.clear();

            mapOfStickyEvent = null;

            return true;
        }
    }

    /**
     * Returns all sticky events of a specific type that were posted without an event ID.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Configuration> configs = eventBus.stickyEvents(Configuration.class);
     * for (Configuration config : configs) {
     *     processConfig(config);
     * }
     * }</pre>
     *
     * @param <T> the event type
     * @param eventType the class type to search for; a primitive class matches its wrapper type
     * @return a list of sticky events that can be assigned to the specified type
     * @throws IllegalArgumentException if {@code eventType} is {@code null}.
     */
    public <T> List<T> stickyEvents(final Class<T> eventType) throws IllegalArgumentException {
        return stickyEvents(null, eventType);
    }

    /**
     * Returns all sticky events of a specific type that were posted with the specified event ID.
     * This allows retrieval of sticky events filtered by both type and ID.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<Configuration> userConfigs = eventBus.stickyEvents("userConfig", Configuration.class);
     * }</pre>
     *
     * @param <T> the event type
     * @param eventId the event ID to match, or {@code null} or empty for events retained without an event ID
     * @param eventType the class type to search for; a primitive class matches its wrapper type
     * @return a list of sticky events matching both the type and event ID
     * @throws IllegalArgumentException if {@code eventType} is {@code null}.
     */
    public <T> List<T> stickyEvents(final String eventId, final Class<T> eventType) throws IllegalArgumentException {
        N.checkArgNotNull(eventType, cs.eventType);

        final String normalizedEventId = Strings.isEmpty(eventId) ? null : eventId;
        // int.class is a Class<Integer>, so matching and casting through its wrapper is type-safe.
        @SuppressWarnings("unchecked")
        final Class<T> matchType = (Class<T>) wrapIfPrimitive(eventType);
        final List<T> result = new ArrayList<>();

        synchronized (stickyEventMap) {
            for (final Map.Entry<Object, String> entry : stickyEventMap.entrySet()) {
                if (N.equals(Strings.isEmpty(entry.getValue()) ? null : entry.getValue(), normalizedEventId)
                        && matchType.isAssignableFrom(entry.getKey().getClass())) {
                    result.add(matchType.cast(entry.getKey()));
                }
            }
        }

        return result;
    }

    /**
     * Checks if the specified thread mode is supported by this {@code EventBus}.
     * Currently supports {@code DEFAULT} and {@code THREAD_POOL_EXECUTOR} modes.
     *
     * <p><b>Supported Thread Modes:</b></p>
     * <ul>
     *   <li>{@code null} or {@link ThreadMode#DEFAULT} - Events are delivered on the posting thread</li>
     *   <li>{@link ThreadMode#THREAD_POOL_EXECUTOR} - Events are submitted to this bus's executor (background threads
     *       for the default executor; a caller-supplied executor may run them on the posting thread)</li>
     * </ul>
     *
     * <p>It is consulted only for the thread-mode override passed to {@code register(...)}; thread modes declared
     * in {@link Subscribe} annotations are not checked against it.</p>
     *
     * @param threadMode the thread mode to check, may be {@code null}
     * @return {@code true} if the thread mode is supported, {@code false} otherwise
     */
    protected boolean isSupportedThreadMode(final ThreadMode threadMode) {
        return threadMode == null || threadMode == ThreadMode.DEFAULT || threadMode == ThreadMode.THREAD_POOL_EXECUTOR;
    }

    /**
     * Dispatches an event to a subscriber according to its thread mode configuration.
     * This method determines whether to deliver the event synchronously or asynchronously
     * based on the thread mode specified in the subscriber identifier.
     *
     * <p><b>Dispatch Behavior:</b></p>
     * <ul>
     *   <li>{@link ThreadMode#DEFAULT} - Event is delivered synchronously on the calling thread through
     *       {@link #post(SubIdentifier, Object)}</li>
     *   <li>{@link ThreadMode#THREAD_POOL_EXECUTOR} - Event is submitted to this bus's executor (a background thread for the
     *       default executor; a caller-supplied executor may run it on the calling thread)</li>
     * </ul>
     *
     * <p><b>Filtering:</b> Interval throttling and deduplication are decided here, on the calling thread
     * and for both thread modes, so they follow the order and spacing in which events are <i>posted</i> rather
     * than the order in which an executor happens to run them. A suppressed asynchronous event is never
     * handed to the executor. An accepted asynchronous event is enqueued as an invoke-only task (it does not
     * pass through {@link #post(SubIdentifier, Object)} again). Its reservation becomes permanent when
     * {@link Executor#execute(Runnable)} returns successfully or the task begins its callback, whichever
     * happens first. If any throwable escapes {@code execute} before either point, the reservation is released
     * so it cannot throttle or deduplicate later events; if the executor nevertheless runs that task later, the
     * task skips the callback (a subscriber without throttling or deduplication keeps no reservation, so its task
     * still runs). The original throwable propagates from this method; public posting methods log exceptions,
     * while errors propagate. Overlapping failed submissions are removed from the reservation history regardless
     * of failure order; newer accepted submissions keep their filter state. An attempted callback still counts
     * even if the callback fails or an inline executor subsequently throws. Events posted while a submission is
     * still inside {@code execute} are filtered against it; if that submission then fails, the events it
     * suppressed are not re-evaluated.</p>
     *
     * <p>For {@code THREAD_POOL_EXECUTOR}, an exception thrown by the event's {@code equals()} while
     * deduplicating also propagates from this method, before anything is reserved or submitted; for
     * {@code DEFAULT}, {@link #post(SubIdentifier, Object)} logs it.</p>
     *
     * @param identifier the subscriber identifier containing delivery configuration including thread mode,
     *                   subscriber instance, and method to invoke
     * @param event the event object to dispatch to the subscriber
     * @throws IllegalArgumentException if the thread mode of {@code identifier} is not one of the supported values.
     * @throws RejectedExecutionException if the thread mode is {@link ThreadMode#THREAD_POOL_EXECUTOR} and the executor
     *         rejects the delivery task
     */
    protected void dispatch(final SubIdentifier identifier, final Object event) throws IllegalArgumentException, RejectedExecutionException {
        switch (identifier.threadMode) {
            case DEFAULT:
                post(identifier, event);

                return;

            case THREAD_POOL_EXECUTOR: {
                // Reserve on the posting thread so the interval and the "previous event" follow post order and
                // spacing (as documented on @Subscribe) rather than executor scheduling, and so a suppressed
                // event is never enqueued. dispatch always runs outside every EventBus lock, as post(...) does.
                final Reservation reservation = reserve(identifier, event);

                if (reservation.accepted) {
                    try {
                        executor.execute(() -> {
                            // An executor may queue this task and still throw from execute (e.g. a
                            // ScheduledThreadPoolExecutor that fails to start a worker). Its reservation was then
                            // released, so delivering it now would bypass the throttling/deduplication state.
                            if (identifier.commit(reservation)) {
                                invoke(identifier, event);
                            }
                        });
                        identifier.commit(reservation);
                    } catch (final Throwable e) {
                        // Give the filter state back unless an inline executor already attempted the callback.
                        identifier.release(reservation);

                        throw e;
                    }
                }

                return;
            }

            default:
                throw new IllegalArgumentException("Unsupported thread mode");
        }
    }

    /**
     * Delivers an event to a specific subscriber method.
     * This method handles interval filtering, deduplication, and actual method invocation
     * by calling the subscriber's method via reflection.
     *
     * <p><b>Thread Safety:</b> Filtering atomically reserves each accepted delivery attempt before
     * logging or invoking the subscriber. Equality checks, logging and subscriber callbacks execute
     * outside the filtering monitor, so reentrant posts cannot invert handler locks. Distinct accepted
     * callbacks may execute concurrently. The reservation is committed before invoking the callback;
     * failed callbacks still count as accepted attempts for throttling and deduplication.</p>
     *
     * <p><b>Event Filtering:</b></p>
     * <ul>
     *   <li><b>Interval Filtering:</b> If {@code intervalMillis} is set on the subscriber, events posted
     *       within the specified interval (in milliseconds) will be ignored. This is useful for
     *       throttling high-frequency events.</li>
     *   <li><b>Deduplication:</b> If {@code deduplicate} is enabled, consecutive duplicate events
     *       (determined by {@code equals()}) will be ignored. This prevents redundant processing
     *       of unchanged data.</li>
     * </ul>
     *
     * <p><b>Error Handling:</b> Any exception thrown by the subscriber method, or by the reflective
     * invocation itself, is caught and logged; it is never propagated to the caller.</p>
     *
     * <p><b>Scope:</b> This method carries out {@link ThreadMode#DEFAULT} deliveries. For other direct callers
     * in this package, it reserves and commits each accepted attempt before invoking the callback.
     * Asynchronous ({@link ThreadMode#THREAD_POOL_EXECUTOR}) deliveries do <b>not</b> pass through it:
     * {@link #dispatch(SubIdentifier, Object)} makes the reservation on the posting thread and the
     * executor task invokes the subscriber method directly.</p>
     *
     * @param sub the subscriber identifier containing the method to invoke, subscriber instance,
     *            and filtering configuration (interval, deduplicate)
     * @param event the event object to deliver to the subscriber
     */
    protected void post(final SubIdentifier sub, final Object event) {
        try {
            final Reservation reservation = reserve(sub, event);
            if (reservation.accepted) {
                sub.commit(reservation);
                invoke(sub, event);
            }
        } catch (final Exception e) {
            logger.error("Failed to post event: " + diagnosticString(event) + " to subscriber: " + diagnosticString(sub), e);
        }
    }

    /**
     * Atomically reserves a delivery attempt for {@code event} (interval throttling and deduplication) and
     * logs a suppressed event at debug level. No application code runs under the filter monitor.
     *
     * @param sub the subscriber identifier holding the filter configuration and state
     * @param event the event being posted
     * @return the reservation: {@link Reservation#accepted} tells whether the event must now be delivered, and an
     *         accepted reservation can be handed back to {@link SubIdentifier#release(Reservation)} if the
     *         delivery it reserved is never attempted
     */
    private static Reservation reserve(final SubIdentifier sub, final Object event) {
        final Reservation reservation = sub.intervalMillis > 0 || sub.deduplicate ? sub.reservePost(event) : Reservation.UNFILTERED;

        // No application code (including diagnostic toString/logging callbacks) runs under the filter monitor.
        if (reservation == Reservation.INTERVAL) {
            if (logger.isDebugEnabled()) {
                logger.debug("Ignoring event: {} to subscriber: {} because it is within the interval: {}", diagnosticString(event), diagnosticString(sub),
                        sub.intervalMillis);
            }
        } else if (reservation == Reservation.DUPLICATE) {
            if (logger.isDebugEnabled()) {
                logger.debug("Ignoring event: {} to subscriber: {} (duplicate of previous event)", diagnosticString(event), diagnosticString(sub));
            }
        }

        return reservation;
    }

    /**
     * Invokes the subscriber method for an already-reserved delivery. Any exception from the reflective
     * invocation or from the subscriber itself is caught and logged, never propagated.
     *
     * @param sub the subscriber identifier whose method is invoked
     * @param event the accepted event
     */
    private static void invoke(final SubIdentifier sub, final Object event) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Posting event: {} to subscriber: {}", diagnosticString(event), diagnosticString(sub));
            }

            sub.method.invoke(sub.instance, event);
        } catch (final Exception e) {
            logger.error("Failed to post event: " + diagnosticString(event) + " to subscriber: " + diagnosticString(sub), e);
        }
    }

    /** Describes application objects without letting a secondary diagnostic exception abort dispatch. */
    private static String diagnosticString(final Object value) {
        try {
            return N.toString(value);
        } catch (final Exception | StackOverflowError e) {
            // StackOverflowError is caught deliberately: N.toString(..) recurses without a guard into a
            // self-containing collection or array, and a cyclic toString() (e.g. bidirectional entities) does the
            // same. Either would otherwise escape the logging catch blocks, abort the post, and skip every
            // remaining subscriber - or, with debug logging on, every delivery of that event.
            // The fallback must not call application code, including the failing exception's toString().
            return value == null ? "null" : value.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(value));
        }
    }

    /**
     * Returns the wrapper class for a primitive type, or {@code type} itself otherwise. Events are always
     * objects, so primitive handler parameters and primitive query types are matched by their wrapper.
     */
    private static Class<?> wrapIfPrimitive(final Class<?> type) {
        return ClassUtil.isPrimitiveType(type) ? ClassUtil.wrap(type) : type;
    }

    /**
     * The outcome of one filtering decision. A suppressed decision is the shared {@link #INTERVAL} or
     * {@link #DUPLICATE} constant. An accepted decision is {@link #UNFILTERED} when the subscriber filters
     * nothing and there is consequently no state to undo; otherwise it records the resulting filter state
     * and its predecessor. Pending submissions retain that history so rejecting them in either order can
     * restore the most recent non-rejected state. Successful submissions discard older history.
     */
    private static final class Reservation {

        /** Suppressed: the event arrived within the subscriber's minimum post interval. */
        static final Reservation INTERVAL = new Reservation(false);

        /** Suppressed: the event equals the subscriber's previously accepted event. */
        static final Reservation DUPLICATE = new Reservation(false);

        /** Accepted by a subscriber that filters nothing, so no filter state was written. */
        static final Reservation UNFILTERED = new Reservation(true);

        /** Whether the delivery attempt was accepted and must now be carried out. */
        final boolean accepted;

        /** The subscriber's {@code lastPostTimeNanos} after this reservation. */
        final long lastPostTimeNanos;

        /** The subscriber's {@code hasPosted} after this reservation. */
        final boolean hasPosted;

        /** The subscriber's {@code previousEvent} after this reservation. */
        final Object previousEvent;

        /** Earlier filter state, needed only while this submission can still be rejected. Guarded by the subscriber monitor. */
        Reservation previous;

        /**
         * Whether this submission was accepted or its callback began. Written under the subscriber monitor,
         * after clearing predecessor history; once true it never changes, allowing later operations to skip the monitor.
         */
        volatile boolean committed;

        /** Whether the executor rejected this submission. Guarded by the subscriber monitor. */
        boolean rejected;

        private Reservation(final boolean accepted) {
            this.accepted = accepted;
            lastPostTimeNanos = 0;
            hasPosted = false;
            previousEvent = null;
        }

        Reservation(final long lastPostTimeNanos, final boolean hasPosted, final Object previousEvent, final Reservation previous) {
            accepted = true;
            this.lastPostTimeNanos = lastPostTimeNanos;
            this.hasPosted = hasPosted;
            this.previousEvent = previousEvent;
            this.previous = previous;
        }
    }

    /**
     * Internal class representing a subscriber method and its configuration.
     * This class holds all the metadata needed to deliver events to a subscriber,
     * including the method reference, parameter type, thread mode, filtering options,
     * and runtime state such as the last post time and previous event for deduplication.
     */
    protected static final class SubIdentifier {

        /** The subscriber object instance on which the method will be invoked. {@code null} for prototype entries. */
        final Object instance;

        /** The subscriber method to invoke when a matching event is posted. */
        final Method method;

        /**
         * The event type this handler accepts: the method's parameter type with type variables resolved against the
         * subscriber class (parameterized types as their raw class; primitives boxed).
         */
        final Class<?> parameterType;

        /** The event ID filter; {@code null} means the subscriber accepts events posted without an ID. */
        final String eventId;

        /** The thread mode that controls on which thread the event is delivered. */
        final ThreadMode threadMode;

        /** If {@code true}, only events of the exact {@link #parameterType} are accepted; subtypes are excluded. */
        final boolean strictEventType;

        /**
         * If {@code true}, the subscriber should receive every retained matching sticky event
         * immediately upon registration.
         */
        final boolean sticky;

        /** Minimum interval in milliseconds between accepted delivery attempts; values {@code <= 0} disable throttling. */
        final long intervalMillis;

        /**
         * If {@code true}, consecutive duplicate events (as determined by {@code equals()}) are ignored
         * and not delivered to the subscriber.
         */
        final boolean deduplicate;

        /**
         * {@code true} for the unannotated {@link Subscriber#on(Object)} contract method when its resolved event type
         * is {@code Object} (a lambda, a method reference, or a raw, {@code Subscriber<Object>} or unbound generic
         * implementation); such a subscriber requires an explicit event ID.
         */
        final boolean isPossibleLambdaSubscriber;

        /**
         * The monotonic time when the last delivery attempt was accepted for this subscriber.
         */
        long lastPostTimeNanos;

        /** Whether an event-delivery attempt has already been recorded for interval throttling. */
        boolean hasPosted;

        /** The most recently accepted event, including failed callbacks, used when {@link #deduplicate} is {@code true}. */
        Object previousEvent = null;

        /**
         * Changes whenever a reservation updates or restores the filter state, including reentrant posts
         * made by equality callbacks. This generation is independent of reservation identity and never
         * rewinds when a rejected submission restores an earlier state.
         */
        private long reservationVersion;

        /** Latest filter state, including pending submissions; guarded by this subscriber's monitor. */
        private Reservation currentReservation;

        private Reservation reservePost(final Object event) {
            for (;;) {
                final Object previous;
                final long version;
                synchronized (this) {
                    final long now = intervalMillis > 0 ? System.nanoTime() : 0;
                    if (isWithinPostInterval(now)) {
                        return Reservation.INTERVAL;
                    }
                    if (!deduplicate) {
                        return recordReservation(now, event);
                    }
                    previous = previousEvent;
                    version = reservationVersion;
                }

                // equals() is application code: it may post to this or another handler. Revalidate
                // the reservation afterward so concurrent/reentrant changes cannot admit a duplicate.
                final boolean duplicate = previous != null && N.equals(previous, event);
                synchronized (this) {
                    if (version != reservationVersion) {
                        continue;
                    }
                    final long now = intervalMillis > 0 ? System.nanoTime() : 0;
                    if (isWithinPostInterval(now)) {
                        return Reservation.INTERVAL;
                    }
                    if (duplicate) {
                        return Reservation.DUPLICATE;
                    }
                    return recordReservation(now, event);
                }
            }
        }

        /** Records an accepted filtering decision while holding this subscriber's monitor. */
        private Reservation recordReservation(final long now, final Object event) {
            if (intervalMillis > 0) {
                recordPostTime(now);
            }
            if (deduplicate) {
                previousEvent = event;
            }
            currentReservation = new Reservation(lastPostTimeNanos, hasPosted, previousEvent, currentReservation);
            reservationVersion++;
            return currentReservation;
        }

        /**
         * Makes a submitted or attempted delivery permanent, discarding history that can no longer affect filtering.
         *
         * @param reservation a reservation for this subscriber
         * @return {@code false} if the reservation was already released, in which case its delivery must not be
         *         attempted; {@code true} otherwise, including for shared decisions that wrote no filter state
         */
        boolean commit(final Reservation reservation) {
            if (!reservation.accepted || reservation == Reservation.UNFILTERED || reservation.committed) {
                return true;
            }

            synchronized (this) {
                if (reservation.rejected) {
                    return false;
                }

                if (!reservation.committed) {
                    reservation.previous = null;
                    reservation.committed = true;
                }

                return true;
            }
        }

        /**
         * Removes a rejected submission from the filter history. If it is the latest reservation, restores
         * the nearest non-rejected predecessor; otherwise it is unlinked from the newer pending submission
         * that recorded it as its predecessor, so it no longer retains its event. Committed deliveries are
         * never undone.
         *
         * @param reservation a reservation for this subscriber; shared decisions that wrote no filter state
         *                    (suppressed events or an unfiltered subscriber) are ignored
         */
        void release(final Reservation reservation) {
            if (!reservation.accepted || reservation == Reservation.UNFILTERED || reservation.committed) {
                // Shared decisions wrote no state, and permanent reservations can never be undone.
                return;
            }

            synchronized (this) {
                if (reservation.committed || reservation.rejected) {
                    return;
                }

                reservation.rejected = true;
                if (currentReservation != reservation) {
                    // Splice it out of the pending chain. Merely flagging it kept it (and its event) linked
                    // behind every newer pending submission, so overlapping failures grew the chain without
                    // bound until the newest submission completed. If a newer commit already cut the chain,
                    // the reservation is unreachable and the loop finds nothing.
                    for (Reservation newer = currentReservation; newer != null; newer = newer.previous) {
                        if (newer.previous == reservation) {
                            newer.previous = reservation.previous;
                            break;
                        }
                    }

                    reservation.previous = null;
                    return;
                }

                // Rejected predecessors were spliced out when they failed, so the predecessor is the nearest
                // non-rejected state.
                currentReservation = reservation.previous;
                reservation.previous = null;

                lastPostTimeNanos = currentReservation == null ? 0 : currentReservation.lastPostTimeNanos;
                hasPosted = currentReservation != null && currentReservation.hasPosted;
                previousEvent = currentReservation == null ? null : currentReservation.previousEvent;

                // Invalidate the deduplication snapshot of any equals() call still running against the
                // released reservation, so it re-reads the restored previous event instead of the released one.
                reservationVersion++;
            }
        }

        boolean isWithinPostInterval(final long currentTimeNanos) {
            if (intervalMillis <= 0 || !hasPosted) {
                return false;
            }

            final long elapsedNanos = currentTimeNanos - lastPostTimeNanos;

            // Fail open for an elapsed value outside nanoTime's reliable half-range (for example,
            // an interval spanning at least 2^63 nanoseconds) instead of suppressing indefinitely.
            return elapsedNanos >= 0 && elapsedNanos < TimeUnit.MILLISECONDS.toNanos(intervalMillis);
        }

        void recordPostTime(final long currentTimeNanos) {
            lastPostTimeNanos = currentTimeNanos;
            hasPosted = true;
        }

        /**
         * Constructs a prototype {@code SubIdentifier} from a subscriber method, using the method's erased
         * parameter type as the event type. The resulting instance has no subscriber object ({@link #instance} is {@code null}) and
         * is used as a template to create bound instances via
         * {@link #SubIdentifier(SubIdentifier, Object, String, ThreadMode)}.
         *
         * @param method the subscriber method annotated with {@link Subscribe}, or the {@code on} method
         *               of a {@link Subscriber} implementation
         */
        SubIdentifier(final Method method) {
            this(method, method.getParameterTypes()[0]);
        }

        /** Uses a resolved event type independently of the method's erased invocation signature. */
        SubIdentifier(final Method method, final Class<?> eventType) {
            final Subscribe subscribe = method.getAnnotation(Subscribe.class);
            instance = null;
            this.method = method;
            parameterType = wrapIfPrimitive(eventType);
            eventId = subscribe == null || Strings.isEmpty(subscribe.eventId()) ? null : subscribe.eventId();
            threadMode = subscribe == null ? ThreadMode.DEFAULT : subscribe.threadMode();
            strictEventType = subscribe != null && subscribe.strictEventType();
            sticky = subscribe != null && subscribe.sticky();
            intervalMillis = subscribe == null ? 0 : subscribe.intervalMillis();
            deduplicate = subscribe != null && subscribe.deduplicate();

            isPossibleLambdaSubscriber = Subscriber.class.isAssignableFrom(method.getDeclaringClass()) && method.getName().equals("on")
                    && parameterType.equals(Object.class) && subscribe == null;

            // setAccessible is caller-sensitive: under JPMS it checks the calling module. Calling it here honors a
            // package opened to this event bus module, which ClassUtil.setAccessible (whose module is abacus-common)
            // did not; the fallback keeps packages opened only to abacus-common working.
            try {
                method.setAccessible(true);
            } catch (final RuntimeException e) {
                ClassUtil.setAccessible(method, true);
            }
        }

        /**
         * Constructs a bound {@code SubIdentifier} by combining the metadata from a prototype entry
         * with a concrete subscriber instance and registration-time overrides for the event ID and thread mode.
         *
         * @param sub       the prototype {@code SubIdentifier} carrying method-level defaults
         * @param obj       the subscriber instance that will receive events
         * @param eventId   the registration-level event ID override; if empty or {@code null}, the method-level ID is used
         * @param threadMode the registration-level thread mode override; if {@code null}, the method-level mode is used
         */
        SubIdentifier(final SubIdentifier sub, final Object obj, final String eventId, final ThreadMode threadMode) {
            instance = obj;
            method = sub.method;
            parameterType = sub.parameterType;
            this.eventId = Strings.isEmpty(eventId) ? sub.eventId : eventId;
            this.threadMode = threadMode == null ? sub.threadMode : threadMode;
            strictEventType = sub.strictEventType;
            sticky = sub.sticky;
            intervalMillis = sub.intervalMillis;
            deduplicate = sub.deduplicate;
            isPossibleLambdaSubscriber = sub.isPossibleLambdaSubscriber;
        }

        /**
         * Determines whether this subscriber should receive an event of the specified type and event ID.
         * The event ID must match exactly (both {@code null} or both equal), and the event type must be
         * compatible with this subscriber's {@link #parameterType}, respecting the {@link #strictEventType} flag.
         *
         * @param eventId   the event ID of the posted event, or {@code null} if posted without an ID
         * @param eventType the runtime type of the posted event
         * @return {@code true} if this subscriber is eligible to receive the event
         */
        boolean isMyEvent(final String eventId, final Class<?> eventType) {
            if (!N.equals(this.eventId, eventId)) {
                return false;
            }

            return strictEventType ? parameterType.equals(eventType) : parameterType.isAssignableFrom(eventType);
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 31 * h + System.identityHashCode(instance);
            h = 31 * h + N.hashCode(method);
            h = 31 * h + N.hashCode(parameterType);
            h = 31 * h + N.hashCode(eventId);
            h = 31 * h + N.hashCode(threadMode);
            h = 31 * h + N.hashCode(strictEventType);
            h = 31 * h + N.hashCode(sticky);
            h = 31 * h + N.hashCode(intervalMillis);
            h = 31 * h + N.hashCode(deduplicate);
            return 31 * h + N.hashCode(isPossibleLambdaSubscriber);
        }

        @SuppressFBWarnings
        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj instanceof SubIdentifier other) {
                return instance == other.instance && N.equals(method, other.method) && N.equals(parameterType, other.parameterType)
                        && N.equals(eventId, other.eventId) && N.equals(threadMode, other.threadMode) && N.equals(strictEventType, other.strictEventType)
                        && N.equals(sticky, other.sticky) && N.equals(intervalMillis, other.intervalMillis) && N.equals(deduplicate, other.deduplicate)
                        && N.equals(isPossibleLambdaSubscriber, other.isPossibleLambdaSubscriber);
            }

            return false;
        }

        @Override
        public String toString() {
            return "{obj=" + diagnosticString(instance) + ", method=" + N.toString(method) + ", parameterType=" + N.toString(parameterType) + ", eventId="
                    + N.toString(eventId) + ", threadMode=" + N.toString(threadMode) + ", strictEventType=" + N.toString(strictEventType) + ", sticky="
                    + N.toString(sticky) + ", interval=" + N.toString(intervalMillis) + ", deduplicate=" + N.toString(deduplicate)
                    + ", isPossibleLambdaSubscriber=" + N.toString(isPossibleLambdaSubscriber) + "}";
        }

    }
}
