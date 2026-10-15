package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
@Timeout(30)
public class EventBusAsyncDispatchTest extends TestBase {

    @Test
    void post_whenAsyncIntervalExpires_acceptsEventsBeforeCallbacksRun() throws InterruptedException {
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
    void post_whenAsyncIntervalSuppressesEvent_doesNotEnqueueCallback() {
        final List<Runnable> queued = new ArrayList<>();
        final EventBus bus = EventBus.create("async-suppressed", queued::add);
        final AsyncThrottledSubscriber subscriber = new LongIntervalSubscriber();
        bus.register(subscriber);

        bus.post("e1");
        bus.post("e2");

        assertEquals(1, queued.size(), "a throttled event must not be enqueued");
        assertTrue(subscriber.received.isEmpty());
        queued.forEach(Runnable::run);
        assertEquals(List.of("e1"), subscriber.received);
    }

    @Test
    void post_whenAsyncEventsAreDuplicates_deduplicatesBeforeCallbacksRun() {
        final List<Runnable> queued = new ArrayList<>();
        final EventBus bus = EventBus.create("async-dedup", queued::add);
        final AsyncDedupSubscriber subscriber = new AsyncDedupSubscriber();
        bus.register(subscriber);

        bus.post("same");
        bus.post("same");

        assertEquals(1, queued.size(), "a consecutive duplicate must not be enqueued");
        assertTrue(subscriber.received.isEmpty());

        // Regression guard: a non-consecutive repeat is still delivered.
        bus.post("other");
        bus.post("same");
        assertEquals(3, queued.size());
        assertTrue(subscriber.received.isEmpty());

        // Unicode / empty events participate in equals() like any other.
        bus.post("\u03bb");
        bus.post("\u03bb");
        bus.post("");
        bus.post("");
        assertEquals(5, queued.size());
        assertTrue(subscriber.received.isEmpty());
        queued.forEach(Runnable::run);
        assertEquals(List.of("same", "other", "same", "\u03bb", ""), subscriber.received);
    }

    @Test
    void post_whenAsyncFilteringIsDisabled_enqueuesEveryEvent() {
        final List<Runnable> queued = new ArrayList<>();
        final EventBus bus = EventBus.create("async-unfiltered", queued::add);
        final AsyncUnfilteredSubscriber subscriber = new AsyncUnfilteredSubscriber();
        bus.register(subscriber);

        bus.post("same");
        bus.post("same");
        bus.post("same");

        assertEquals(3, queued.size());
        assertTrue(subscriber.received.isEmpty());
        queued.forEach(Runnable::run);
        assertEquals(List.of("same", "same", "same"), subscriber.received);
    }

    @Test
    void post_whenExecutorRejects_releasesThrottleReservation() {
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
    void post_whenExecutorRejects_releasesDeduplicationReservation() {
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
    void post_whenOverlappingSubmissionsAreRejected_restoresAcceptedEvent(boolean rejectNewerFirst) throws Exception {
        verifyOverlappingRejectedSubmissions(rejectNewerFirst);
    }

    @Test
    void post_whenNestedSubmissionsAreRejected_allowsRetry() {
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
    void post_whenOuterSubmissionIsRejected_preservesNewerAcceptedEvent() {
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
    void post_whenNestedSubmissionIsRejected_preservesOuterAcceptedEvent() {
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
    void post_whenInlineExecutorRejectsAfterCallback_keepsDeduplicationCommitted() {
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

    @Test
    void post_whenAsyncStickyRoutingAndDeduplicationCombine_preservesAcceptedEvents() {
        List<Runnable> queued = new ArrayList<>();
        EventBus eventBus = EventBus.create("combined-attributes", queued::add);
        List<String> received = new ArrayList<>();
        class Handler {
            @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, eventId = "combined", sticky = true, strictEventType = true, deduplicate = true)
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
    void unregister_whenCallbackIsAlreadyQueued_preservesAcceptedDeliveryAndStopsFuturePosts() {
        List<Runnable> queued = new ArrayList<>();
        EventBus bus = EventBus.create("unregister-queued", queued::add);
        AsyncUnfilteredSubscriber subscriber = new AsyncUnfilteredSubscriber();
        bus.register(subscriber);
        bus.post("accepted");
        assertEquals(1, queued.size());
        assertTrue(subscriber.received.isEmpty());

        bus.unregister(subscriber);
        bus.post("after removal");

        assertEquals(1, queued.size(), "Unregistration must prevent selection for future posts");
        assertEquals(0, bus.countOfSubscribers());
        queued.forEach(Runnable::run);
        assertEquals(List.of("accepted"), subscriber.received);
    }

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
}
