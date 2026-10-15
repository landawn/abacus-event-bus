package com.landawn.abacus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;

import com.landawn.abacus.util.N;

@Tag("base-test")
public abstract class TestBase {

    /** Runs the same operation simultaneously on the requested number of workers. */
    protected static void runConcurrently(int threadCount, Runnable action) throws InterruptedException {
        runConcurrently(Collections.nCopies(threadCount, action).toArray(Runnable[]::new));
    }

    /** Runs a simultaneous batch and reports worker failures on the JUnit thread. */
    protected static void runConcurrently(Runnable... actions) throws InterruptedException {
        int threadCount = actions.length;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> workers = new ArrayList<>();
        try {
            for (int i = 0; i < threadCount; i++) {
                Runnable action = actions[i];
                Thread worker = new Thread(() -> {
                    ready.countDown();
                    try {
                        assertTrue(start.await(5, TimeUnit.SECONDS), "Concurrent start was not released");
                        action.run();
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        done.countDown();
                    }
                }, "eventbus-test-worker-" + i);
                // A monitor deadlock must fail the test without trapping the test JVM.
                worker.setDaemon(true);
                workers.add(worker);
                worker.start();
            }
            assertTrue(ready.await(5, TimeUnit.SECONDS), "Workers did not reach the start barrier");
            start.countDown();
            assertTrue(done.await(10, TimeUnit.SECONDS), "Concurrent operations did not finish");
            if (failure.get() != null) {
                fail("Concurrent worker failed", failure.get());
            }
        } finally {
            start.countDown();
            workers.forEach(Thread::interrupt);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            for (Thread worker : workers) {
                long remaining = deadline - System.nanoTime();
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.timedJoin(worker, remaining);
                }
            }
        }
    }

    /** Waits for a worker to block on a monitor, with a bounded failure instead of a scheduling sleep. */
    protected static void awaitBlocked(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != Thread.State.BLOCKED && thread.isAlive() && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(1);
        }
        assertEquals(Thread.State.BLOCKED, thread.getState(), "Thread did not reach the expected monitor: " + thread.getName());
    }

    /** GC regressions necessarily request collection, but wait only to a fixed deadline. */
    protected static void awaitCollected(WeakReference<?> reference) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (reference.get() != null && System.nanoTime() < deadline) {
            System.gc();
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertNull(reference.get(), "Discarded object is still strongly retained");
    }

    public static <T> void assertHaveSameElements(Collection<? extends T> expected, Collection<? extends T> actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    /** Shut down test-owned workers and verify that none remain after the test. */
    protected static void shutdownAndAwaitTermination(ExecutorService executor) throws InterruptedException {
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Test executor did not terminate");
    }
}
