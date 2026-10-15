package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
@Timeout(30)
public class EventBusReservationTest extends TestBase {

    @Test
    void dispatch_whenQueuedTasksAreAccepted_discardsHistoryAndAvoidsFilterMonitor() throws Exception {
        final List<Runnable> queued = new ArrayList<>();
        final EventBus bus = EventBus.create("accepted-queued-tasks", queued::add);
        final DeduplicatedHandler subscriber = new DeduplicatedHandler();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);
        final java.lang.reflect.Field current = EventBus.SubIdentifier.class.getDeclaredField("currentReservation");
        current.setAccessible(true);
        final java.lang.reflect.Field previous = current.getType().getDeclaredField("previous");
        previous.setAccessible(true);

        for (final String event : List.of("first", "second", "third")) {
            bus.dispatch(identifier, event);
            assertNull(previous.get(current.get(identifier)), "successful submission must not retain older event history");
        }
        bus.dispatch(identifier, "third");
        assertEquals(3, queued.size(), "successful submission must commit deduplication before task execution");
        assertTrue(subscriber.received.isEmpty());

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch completed = new CountDownLatch(1);
        final Thread worker = new Thread(() -> {
            try {
                queued.forEach(Runnable::run);
            } catch (Throwable e) {
                failure.set(e);
            } finally {
                completed.countDown();
            }
        }, "committed-eventbus-tasks");
        worker.setDaemon(true);
        try {
            synchronized (identifier) {
                worker.start();
                assertTrue(completed.await(5, TimeUnit.SECONDS), "permanently committed tasks must not reacquire the filter monitor");
            }
        } finally {
            worker.join(5000);
        }

        assertFalse(worker.isAlive());
        assertNull(failure.get());
        assertEquals(List.of("first", "second", "third"), subscriber.received);
    }

    @ParameterizedTest
    @MethodSource("executorFailures")
    void dispatch_whenExecutorFails_releasesReservationAndPreservesThrowable(Throwable expected) throws Exception {
        verifyExecutorFailureCleanup(expected);
    }

    @ParameterizedTest
    @MethodSource("allExecutorFailures")
    void dispatch_whenExecutorFailsRepeatedly_discardsReservationHistory(Throwable expected) throws Exception {
        final java.lang.reflect.Field current = EventBus.SubIdentifier.class.getDeclaredField("currentReservation");
        current.setAccessible(true);

        final AtomicInteger offered = new AtomicInteger();
        final EventBus bus = EventBus.create("repeated-executor-failures", task -> {
            offered.incrementAndGet();
            throwExecutorFailure(expected);
        });
        final DeduplicatedHandler subscriber = new DeduplicatedHandler();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);

        for (int i = 0; i < 128; i++) {
            assertSame(expected, assertThrows(Throwable.class, () -> bus.dispatch(identifier, "retry")));
            assertNull(current.get(identifier), "failed submissions must release their entire reservation history without a successful retry");
            assertNull(identifier.previousEvent, "a failed submission must not retain its event");
        }

        assertEquals(128, offered.get());
        assertTrue(subscriber.received.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("executorFailures")
    void dispatch_whenExecutorFailsAfterCallback_keepsReservationCommitted(Throwable expected) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final EventBus bus = EventBus.create("attempted-before-executor-failure", task -> {
            offered.incrementAndGet();
            task.run();
            throwExecutorFailure(expected);
        });
        final DeduplicatedHandler subscriber = new DeduplicatedHandler();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);

        assertSame(expected, assertThrows(Throwable.class, () -> bus.dispatch(identifier, "same")));
        bus.dispatch(identifier, "same");

        assertEquals(1, offered.get());
        assertEquals(List.of("same"), subscriber.received);
    }

    @ParameterizedTest
    @MethodSource("executorFailures")
    void dispatch_whenOlderSubmissionFails_preservesNewerAcceptedEvent(Throwable expected) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final AtomicReference<EventBus> busReference = new AtomicReference<>();
        final DeduplicatedHandler subscriber = new DeduplicatedHandler();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);
        final EventBus bus = EventBus.create("newer-accepted-before-failure", task -> {
            if (offered.getAndIncrement() == 0) {
                busReference.get().dispatch(identifier, "newer");
                throwExecutorFailure(expected);
            }
            task.run();
        });
        busReference.set(bus);

        assertSame(expected, assertThrows(Throwable.class, () -> bus.dispatch(identifier, "older")));
        bus.dispatch(identifier, "newer");
        assertEquals(2, offered.get());
        bus.dispatch(identifier, "older");
        assertEquals(List.of("newer", "older"), subscriber.received);
    }

    @ParameterizedTest(name = "rejectSubmission={0}")
    @ValueSource(booleans = { false, true })
    void dispatch_whenCallbackCommitsBeforeSubmissionFinishes_preservesAcceptedDelivery(boolean rejectSubmission) throws Exception {
        CountDownLatch queued = new CountDownLatch(1);
        CountDownLatch finishSubmission = new CountDownLatch(1);
        AtomicReference<Runnable> callback = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger offered = new AtomicInteger();
        RejectedExecutionException rejection = new RejectedExecutionException("submission failed after scheduling");
        EventBus bus = EventBus.create("completion-race", task -> {
            offered.incrementAndGet();
            callback.set(task);
            queued.countDown();
            try {
                assertTrue(finishSubmission.await(5, TimeUnit.SECONDS), "Submission was not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
            if (rejectSubmission) {
                throw rejection;
            }
        });
        DeduplicatedHandler subscriber = new DeduplicatedHandler();
        EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);
        Thread posting = new Thread(() -> {
            try {
                bus.dispatch(identifier, "accepted");
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "eventbus-submission-completion");
        posting.setDaemon(true);
        try {
            posting.start();
            assertTrue(queued.await(5, TimeUnit.SECONDS), "Executor did not receive the callback");
            synchronized (identifier) {
                finishSubmission.countDown();
                // Pause commit/release after its fast check; the callback commits before it acquires the monitor.
                awaitBlocked(posting);
                callback.get().run();
            }
        } finally {
            finishSubmission.countDown();
            posting.join(5000);
        }

        assertFalse(posting.isAlive());
        assertSame(rejectSubmission ? rejection : null, failure.get());
        bus.dispatch(identifier, "accepted");
        assertEquals(1, offered.get(), "An attempted callback remains committed even if submission subsequently fails");
        assertEquals(List.of("accepted"), subscriber.received);
    }

    @ParameterizedTest(name = "rejectNewerFirst={0}")
    @ValueSource(booleans = { false, true })
    void dispatch_whenOverlappingIntervalSubmissionsFail_restoresAcceptedTimestamp(boolean rejectNewerFirst) throws Exception {
        verifyOverlappingIntervalRejections(rejectNewerFirst);
    }

    @Test
    void reservation_whenDecisionIsShared_preservesConstantState() throws Exception {
        final EventBus.SubIdentifier identifier = asyncIdentifier(new DeduplicatedHandler());
        final Class<?> reservationClass = Class.forName(EventBus.class.getName() + "$Reservation");
        final Method commit = EventBus.SubIdentifier.class.getDeclaredMethod("commit", reservationClass);
        final Method release = EventBus.SubIdentifier.class.getDeclaredMethod("release", reservationClass);
        final java.lang.reflect.Field committed = reservationClass.getDeclaredField("committed");
        final java.lang.reflect.Field rejected = reservationClass.getDeclaredField("rejected");
        committed.setAccessible(true);
        rejected.setAccessible(true);

        for (final String name : List.of("INTERVAL", "DUPLICATE", "UNFILTERED")) {
            final java.lang.reflect.Field field = reservationClass.getDeclaredField(name);
            field.setAccessible(true);
            final Object reservation = field.get(null);
            final boolean wasCommitted = committed.getBoolean(reservation);
            final boolean wasRejected = rejected.getBoolean(reservation);
            try {
                commit.invoke(identifier, reservation);
                assertEquals(wasCommitted, committed.getBoolean(reservation), name + " is shared by all subscribers");
                release.invoke(identifier, reservation);
                assertEquals(wasRejected, rejected.getBoolean(reservation), name + " is shared by all subscribers");
            } finally {
                // Keep a failing baseline run from contaminating other tests through a shared singleton.
                committed.setBoolean(reservation, wasCommitted);
                rejected.setBoolean(reservation, wasRejected);
            }
        }
    }

    @Test
    void reservation_whenRejected_repeatedCompletionPreservesPreviousDelivery() throws Exception {
        EventBus.SubIdentifier identifier = asyncIdentifier(new DeduplicatedHandler());
        Method reserve = EventBus.SubIdentifier.class.getDeclaredMethod("reservePost", Object.class);
        reserve.setAccessible(true);
        Class<?> reservationType = reserve.getReturnType();
        Method commit = EventBus.SubIdentifier.class.getDeclaredMethod("commit", reservationType);
        Method release = EventBus.SubIdentifier.class.getDeclaredMethod("release", reservationType);
        java.lang.reflect.Field current = EventBus.SubIdentifier.class.getDeclaredField("currentReservation");
        java.lang.reflect.Field committed = reservationType.getDeclaredField("committed");
        java.lang.reflect.Field rejected = reservationType.getDeclaredField("rejected");
        current.setAccessible(true);
        committed.setAccessible(true);
        rejected.setAccessible(true);
        Object accepted = reserve.invoke(identifier, "accepted");
        commit.invoke(identifier, accepted);
        Object failed = reserve.invoke(identifier, "rejected");

        release.invoke(identifier, failed);
        release.invoke(identifier, failed);
        commit.invoke(identifier, failed);

        assertSame(accepted, current.get(identifier));
        assertEquals("accepted", identifier.previousEvent);
        assertTrue(committed.getBoolean(accepted));
        assertTrue(rejected.getBoolean(failed));
        assertFalse(committed.getBoolean(failed), "A late commit must not revive a rejected reservation");

        Object retry = reserve.invoke(identifier, "rejected");
        commit.invoke(identifier, retry);
        assertSame(retry, current.get(identifier));
        assertEquals("rejected", identifier.previousEvent);
        assertTrue(committed.getBoolean(retry));
    }

    public static class ThrottledHandler {
        final List<String> received = Collections.synchronizedList(new ArrayList<>());

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, intervalMillis = 200)
        public void onEvent(String event) {
            received.add(event);
        }
    }

    public static class DeduplicatedHandler {
        final List<String> received = Collections.synchronizedList(new ArrayList<>());

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, deduplicate = true)
        public void onEvent(String event) {
            received.add(event);
        }
    }

    private static Stream<Throwable> executorFailures() {
        return Stream.of(new IllegalStateException("executor failure"), new AssertionError("executor error"),
                new Exception("executor escaped its declared throws contract"));
    }

    private static Stream<Throwable> allExecutorFailures() {
        return Stream.concat(Stream.of(new RejectedExecutionException("executor rejection")), executorFailures());
    }

    private static void verifyExecutorFailureCleanup(final Throwable expected) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final EventBus bus = EventBus.create("executor-failure-cleanup", task -> {
            if (offered.getAndIncrement() == 0) {
                throwExecutorFailure(expected);
            }
            task.run();
        });
        final DeduplicatedHandler subscriber = new DeduplicatedHandler();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);

        assertSame(expected, assertThrows(Throwable.class, () -> bus.dispatch(identifier, "retry")));
        bus.dispatch(identifier, "retry");

        assertEquals(2, offered.get(), "an executor failure before acceptance must not deduplicate the retry");
        assertEquals(List.of("retry"), subscriber.received);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwExecutorFailure(final Throwable failure) throws E {
        throw (E) failure;
    }

    private static EventBus.SubIdentifier asyncIdentifier(final Object subscriber) throws Exception {
        final EventBus.SubIdentifier prototype = new EventBus.SubIdentifier(subscriber.getClass().getMethod("onEvent", String.class));
        return new EventBus.SubIdentifier(prototype, subscriber, null, ThreadMode.THREAD_POOL_EXECUTOR);
    }

    private static void verifyOverlappingIntervalRejections(final boolean rejectNewerFirst) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final CountDownLatch[] entered = { new CountDownLatch(1), new CountDownLatch(1) };
        final CountDownLatch[] release = { new CountDownLatch(1), new CountDownLatch(1) };
        final RejectedExecutionException[] rejections = { new RejectedExecutionException("first rejection"),
                new RejectedExecutionException("second rejection") };
        final EventBus bus = EventBus.create("overlapping-interval-rejections", task -> {
            final int index = offered.getAndIncrement() - 1;
            if (index >= 0 && index < 2) {
                entered[index].countDown();
                try {
                    assertTrue(release[index].await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                throw rejections[index];
            }
            task.run();
        });
        final ThrottledHandler subscriber = new ThrottledHandler();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);
        bus.dispatch(identifier, "accepted");

        // Use a deterministic past accepted time, avoiding real sleeps between throttled submissions.
        final long priorAcceptedTime = System.nanoTime() - TimeUnit.SECONDS.toNanos(10);
        final Method record = EventBus.SubIdentifier.class.getDeclaredMethod("recordReservation", long.class, Object.class);
        record.setAccessible(true);
        final Method commit = EventBus.SubIdentifier.class.getDeclaredMethod("commit", record.getReturnType());
        synchronized (identifier) {
            commit.invoke(identifier, record.invoke(identifier, priorAcceptedTime, "accepted"));
        }

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] posting = new Thread[2];
        for (int i = 0; i < posting.length; i++) {
            final int index = i;
            posting[i] = new Thread(() -> {
                try {
                    assertSame(rejections[index], assertThrows(RejectedExecutionException.class, () -> bus.dispatch(identifier, "rejected-" + index)));
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            }, "interval-rejection-" + index);
            posting[i].setDaemon(true);
        }
        try {
            posting[0].start();
            assertTrue(entered[0].await(5, TimeUnit.SECONDS));
            synchronized (identifier) {
                identifier.recordPostTime(priorAcceptedTime);
            }
            posting[1].start();
            assertTrue(entered[1].await(5, TimeUnit.SECONDS));
            final int firstRejected = rejectNewerFirst ? 1 : 0;
            release[firstRejected].countDown();
            posting[firstRejected].join(5000);
            assertFalse(posting[firstRejected].isAlive());
            release[1 - firstRejected].countDown();
        } finally {
            release[0].countDown();
            release[1].countDown();
            posting[0].join(5000);
            posting[1].join(5000);
        }

        assertFalse(posting[0].isAlive());
        assertFalse(posting[1].isAlive());
        assertNull(failure.get());
        assertEquals(priorAcceptedTime, identifier.lastPostTimeNanos, "both rejections must restore the prior accepted timestamp exactly");
        assertTrue(identifier.hasPosted, "rejection must not erase the earlier accepted delivery");
        assertTrue(identifier.isWithinPostInterval(priorAcceptedTime + TimeUnit.MILLISECONDS.toNanos(199)));
        assertFalse(identifier.isWithinPostInterval(priorAcceptedTime + TimeUnit.MILLISECONDS.toNanos(200)));
        bus.dispatch(identifier, "retry");
        assertEquals(4, offered.get(), "the rejected submissions must not throttle a later eligible retry");
        assertEquals(List.of("accepted", "retry"), subscriber.received);
    }
}
