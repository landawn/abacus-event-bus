package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
import java.util.concurrent.Executor;
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
public class EventBusAsyncDispatchTest extends TestBase {

    public static class AsyncThrottledSubscriber {
        final List<String> received = Collections.synchronizedList(new ArrayList<>());

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, intervalMillis = 200)
        public void onEvent(String event) {
            received.add(event);
        }
    }

    public static class LongIntervalSubscriber extends AsyncThrottledSubscriber {
        @Override
        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, intervalMillis = Long.MAX_VALUE)
        public void onEvent(String event) {
            super.onEvent(event);
        }
    }

    public static class AsyncDedupSubscriber {
        final List<String> received = Collections.synchronizedList(new ArrayList<>());

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, deduplicate = true)
        public void onEvent(String event) {
            received.add(event);
        }
    }

    public static class AsyncUnfilteredSubscriber {
        final List<String> received = Collections.synchronizedList(new ArrayList<>());

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, intervalMillis = -1)
        public void onEvent(String event) {
            received.add(event);
        }
    }

    private static Executor countingInlineExecutor(final AtomicInteger tasks) {
        return task -> {
            tasks.incrementAndGet();
            task.run();
        };
    }

    @Test
    public void testAsyncIntervalIsMeasuredAtPostTimeNotAtExecutionTime() throws InterruptedException {
        List<Runnable> queued = new ArrayList<>();
        EventBus bus = EventBus.create("async-interval", queued::add);
        AsyncThrottledSubscriber subscriber = new AsyncThrottledSubscriber();
        bus.register(subscriber);

        bus.post("first");
        TimeUnit.MILLISECONDS.sleep(250);
        bus.post("second");
        TimeUnit.MILLISECONDS.sleep(250);
        bus.post("third");

        assertEquals(3, queued.size(), "Posts beyond the interval must be accepted before callbacks execute");
        assertTrue(subscriber.received.isEmpty());
        queued.forEach(Runnable::run);
        assertEquals(List.of("first", "second", "third"), subscriber.received);
    }

    @Test
    public void testAsyncIntervalSuppressedEventIsNeverHandedToTheExecutor() {
        final AtomicInteger tasks = new AtomicInteger();
        final EventBus bus = EventBus.create("async-suppressed", countingInlineExecutor(tasks));
        final AsyncThrottledSubscriber subscriber = new LongIntervalSubscriber();
        bus.register(subscriber);

        bus.post("e1");
        bus.post("e2");

        assertEquals(1, tasks.get(), "a throttled event must not be enqueued");
        assertEquals(List.of("e1"), subscriber.received);
    }

    @Test
    public void testAsyncDeduplicateIsDecidedAtPostTime() {
        final AtomicInteger tasks = new AtomicInteger();
        final EventBus bus = EventBus.create("async-dedup", countingInlineExecutor(tasks));
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);

        bus.post("same");
        bus.post("same");

        assertEquals(1, tasks.get(), "a consecutive duplicate must not be enqueued");
        assertEquals(List.of("same"), subscriber.received);

        // Regression guard: a non-consecutive repeat is still delivered.
        bus.post("other");
        bus.post("same");
        assertEquals(3, tasks.get());
        assertEquals(List.of("same", "other", "same"), subscriber.received);

        // Unicode / empty events participate in equals() like any other.
        bus.post("\u03bb");
        bus.post("\u03bb");
        bus.post("");
        bus.post("");
        assertEquals(List.of("same", "other", "same", "\u03bb", ""), subscriber.received);
    }

    @Test
    public void testAsyncWithoutIntervalOrDeduplicateEnqueuesEveryPost() {
        final AtomicInteger tasks = new AtomicInteger();
        final EventBus bus = EventBus.create("async-unfiltered", countingInlineExecutor(tasks));
        final AsyncUnfilteredSubscriber subscriber = new AsyncUnfilteredSubscriber();
        bus.register(subscriber);

        bus.post("same");
        bus.post("same");
        bus.post("same");

        assertEquals(3, tasks.get());
        assertEquals(List.of("same", "same", "same"), subscriber.received);
    }

    @Test
    public void testRejectedAsyncSubmissionReleasesItsThrottleReservation() {
        final AtomicInteger offered = new AtomicInteger();
        final Executor rejecting = task -> {
            offered.incrementAndGet();
            throw new RejectedExecutionException("expected rejection");
        };
        final EventBus bus = EventBus.create("async-rejected", rejecting);
        final AsyncThrottledSubscriber subscriber = new AsyncThrottledSubscriber();
        bus.register(subscriber);

        assertDoesNotThrow(() -> bus.post("e1"));
        assertDoesNotThrow(() -> bus.post("e2"));

        assertEquals(2, offered.get(), "e1 was never delivered, so the interval slot it reserved must be given back to e2");
        assertEquals(0, subscriber.received.size());
    }

    @Test
    public void testRejectedAsyncSubmissionReleasesItsDeduplicationReservation() {
        final AtomicInteger offered = new AtomicInteger();
        final Executor rejectingFirstTask = task -> {
            if (offered.getAndIncrement() == 0) {
                throw new RejectedExecutionException("expected rejection");
            }

            task.run();
        };
        final EventBus bus = EventBus.create("async-rejected-dedup", rejectingFirstTask);
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);

        assertDoesNotThrow(() -> bus.post("same"));
        bus.post("same");

        assertEquals(2, offered.get(), "the rejected event was never delivered, so it must not become the previous event");
        assertEquals(List.of("same"), subscriber.received);
    }

    @ParameterizedTest(name = "rejectNewerFirst={0}")
    @ValueSource(booleans = { false, true })
    public void testOverlappingRejectedSubmissionsRestorePreviousEvent(boolean rejectNewerFirst) throws Exception {
        verifyOverlappingRejectedSubmissions(rejectNewerFirst);
    }

    private static void verifyOverlappingRejectedSubmissions(final boolean rejectNewerFirst) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final CountDownLatch[] entered = { new CountDownLatch(1), new CountDownLatch(1) };
        final CountDownLatch[] release = { new CountDownLatch(1), new CountDownLatch(1) };
        final EventBus bus = EventBus.create("overlapping-rejections", task -> {
            final int index = offered.getAndIncrement() - 1;
            if (index >= 0 && index < 2) {
                entered[index].countDown();
                try {
                    assertTrue(release[index].await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                throw new RejectedExecutionException("expected rejection " + index);
            }
            task.run();
        });
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);
        bus.post("accepted");

        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Thread[] posting = { new Thread(() -> postAndRecordFailure(bus, "first", failure)),
                new Thread(() -> postAndRecordFailure(bus, "second", failure)) };
        posting[0].setDaemon(true);
        posting[1].setDaemon(true);
        try {
            posting[0].start();
            assertTrue(entered[0].await(5, TimeUnit.SECONDS));
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
        bus.post("accepted");
        assertEquals(3, offered.get(), "both rejected submissions must leave the last accepted event in place");
        bus.post("first");
        assertEquals(4, offered.get(), "a rejected event must be eligible for a later retry");
        assertEquals(List.of("accepted", "first"), subscriber.received);
    }

    private static void postAndRecordFailure(final EventBus bus, final String event, final AtomicReference<Throwable> failure) {
        try {
            bus.post(event);
        } catch (Throwable e) {
            failure.compareAndSet(null, e);
        }
    }

    @Test
    public void testNestedRejectedSubmissionsDoNotSuppressRetry() {
        final AtomicInteger offered = new AtomicInteger();
        final AtomicReference<EventBus> reference = new AtomicReference<>();
        final EventBus bus = EventBus.create("nested-rejections", task -> {
            final int index = offered.incrementAndGet();
            if (index == 1) {
                reference.get().post("second");
            }
            if (index <= 2) {
                throw new RejectedExecutionException("expected rejection " + index);
            }
            task.run();
        });
        reference.set(bus);
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);

        bus.post("first");
        bus.post("first");

        assertEquals(3, offered.get());
        assertEquals(List.of("first"), subscriber.received);
    }

    @Test
    public void testRejectedOuterSubmissionPreservesSuccessfulNewerSubmission() {
        final AtomicInteger offered = new AtomicInteger();
        final AtomicReference<EventBus> reference = new AtomicReference<>();
        final EventBus bus = EventBus.create("successful-newer-submission", task -> {
            if (offered.incrementAndGet() == 1) {
                reference.get().post("second");
                throw new RejectedExecutionException("expected outer rejection");
            }
            task.run();
        });
        reference.set(bus);
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);

        bus.post("first");
        bus.post("second");
        assertEquals(2, offered.get(), "rejecting an older submission must preserve newer successful delivery");
        bus.post("first");
        assertEquals(List.of("second", "first"), subscriber.received);
    }

    @Test
    public void testRejectedNestedSubmissionPreservesSuccessfulOuterSubmission() {
        final AtomicInteger offered = new AtomicInteger();
        final AtomicReference<EventBus> reference = new AtomicReference<>();
        final EventBus bus = EventBus.create("successful-outer-submission", task -> {
            final int index = offered.incrementAndGet();
            if (index == 1) {
                reference.get().post("second");
            } else if (index == 2) {
                throw new RejectedExecutionException("expected nested rejection");
            }
            task.run();
        });
        reference.set(bus);
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);

        bus.post("first");
        bus.post("first");
        assertEquals(2, offered.get(), "rejecting a newer submission must preserve an older successful delivery");
        bus.post("second");
        assertEquals(List.of("first", "second"), subscriber.received);
    }

    @Test
    public void testInlineAttemptBeforeExecutorRejectionRemainsDeduplicated() {
        final AtomicInteger offered = new AtomicInteger();
        final EventBus bus = EventBus.create("attempted-before-rejection", task -> {
            offered.incrementAndGet();
            task.run();
            throw new RejectedExecutionException("reported after callback attempt");
        });
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);

        bus.post("same");
        bus.post("same");

        assertEquals(1, offered.get(), "an attempted callback still counts when execute subsequently throws");
        assertEquals(List.of("same"), subscriber.received);
    }

    @ParameterizedTest
    @MethodSource("executorFailures")
    public void testExecutorFailureReleasesReservationAndPreservesThrowable(Throwable expected) throws Exception {
        verifyExecutorFailureCleanup(expected);
    }

    private static Stream<Throwable> executorFailures() {
        return Stream.of(new IllegalStateException("executor failure"), new AssertionError("executor error"),
                new Exception("executor escaped its declared throws contract"));
    }

    private static Stream<Throwable> allExecutorFailures() {
        return Stream.concat(Stream.of(new RejectedExecutionException("executor rejection")), executorFailures());
    }

    @ParameterizedTest
    @MethodSource("allExecutorFailures")
    public void testRepeatedExecutorFailuresDoNotRetainReservationHistory(Throwable expected) throws Exception {
        final java.lang.reflect.Field current = EventBus.SubIdentifier.class.getDeclaredField("currentReservation");
        current.setAccessible(true);

        final AtomicInteger offered = new AtomicInteger();
        final EventBus bus = EventBus.create("repeated-executor-failures", task -> {
            offered.incrementAndGet();
            throwExecutorFailure(expected);
        });
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);

        for (int i = 0; i < 128; i++) {
            assertSame(expected, assertThrows(Throwable.class, () -> bus.dispatch(identifier, "retry")));
            assertNull(current.get(identifier), "failed submissions must release their entire reservation history without a successful retry");
            assertNull(identifier.previousEvent, "a failed submission must not retain its event");
        }

        assertEquals(128, offered.get());
        assertTrue(subscriber.received.isEmpty());
    }

    private static void verifyExecutorFailureCleanup(final Throwable expected) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final EventBus bus = EventBus.create("executor-failure-cleanup", task -> {
            if (offered.getAndIncrement() == 0) {
                throwExecutorFailure(expected);
            }
            task.run();
        });
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);

        assertSame(expected, assertThrows(Throwable.class, () -> bus.dispatch(identifier, "retry")));
        bus.dispatch(identifier, "retry");

        assertEquals(2, offered.get(), "an executor failure before acceptance must not deduplicate the retry");
        assertEquals(List.of("retry"), subscriber.received);
    }

    @ParameterizedTest
    @MethodSource("executorFailures")
    public void testAttemptedCallbackRemainsCommittedWhenExecutorThrowsAnyFailure(Throwable expected) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final EventBus bus = EventBus.create("attempted-before-executor-failure", task -> {
            offered.incrementAndGet();
            task.run();
            throwExecutorFailure(expected);
        });
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        final EventBus.SubIdentifier identifier = asyncIdentifier(subscriber);

        assertSame(expected, assertThrows(Throwable.class, () -> bus.dispatch(identifier, "same")));
        bus.dispatch(identifier, "same");

        assertEquals(1, offered.get());
        assertEquals(List.of("same"), subscriber.received);
    }

    @ParameterizedTest
    @MethodSource("executorFailures")
    public void testFailedExecutorSubmissionCannotUndoNewerAcceptedSubmission(Throwable expected) throws Exception {
        final AtomicInteger offered = new AtomicInteger();
        final AtomicReference<EventBus> busReference = new AtomicReference<>();
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
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

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwExecutorFailure(final Throwable failure) throws E {
        throw (E) failure;
    }

    private static EventBus.SubIdentifier asyncIdentifier(final Object subscriber) throws Exception {
        final EventBus.SubIdentifier prototype = new EventBus.SubIdentifier(subscriber.getClass().getMethod("onEvent", String.class));
        return new EventBus.SubIdentifier(prototype, subscriber, null, ThreadMode.THREAD_POOL_EXECUTOR);
    }

    @Test
    public void testCommitAndReleaseLeaveSharedReservationConstantsUnchanged() throws Exception {
        final EventBus.SubIdentifier identifier = asyncIdentifier(new AsyncDedupSubscriber());
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
    public void testAcceptedQueuedTasksDiscardHistoryAndDoNotWaitForFilterMonitor() throws Exception {
        final List<Runnable> queued = new ArrayList<>();
        final EventBus bus = EventBus.create("accepted-queued-tasks", queued::add);
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
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

    @ParameterizedTest(name = "rejectNewerFirst={0}")
    @ValueSource(booleans = { false, true })
    public void testOverlappingIntervalRejectionsRestoreAcceptedTimestamp(boolean rejectNewerFirst) throws Exception {
        verifyOverlappingIntervalRejections(rejectNewerFirst);
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
        final AsyncThrottledSubscriber subscriber = new AsyncThrottledSubscriber();
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
                    assertSame(rejections[index],
                            assertThrows(RejectedExecutionException.class, () -> bus.dispatch(identifier, "rejected-" + index)));
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
