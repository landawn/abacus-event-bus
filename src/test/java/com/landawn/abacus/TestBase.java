package com.landawn.abacus;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;

import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.BiIterator;
import com.landawn.abacus.util.Iterators;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Pair;
import com.landawn.abacus.util.TriIterator;
import com.landawn.abacus.util.Triple;
import com.landawn.abacus.util.TypeReference;

@Tag("base-test")
public abstract class TestBase {

    public static final char[] NULL_CHAR_ARRAY = "null".toCharArray();

    /** Runs a simultaneous batch and reports worker failures on the JUnit thread. */
    protected static void runConcurrently(int threadCount, Runnable action) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> workers = new ArrayList<>();
        try {
            for (int i = 0; i < threadCount; i++) {
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

    /** GC regressions necessarily request collection, but wait only to a fixed deadline. */
    protected static void awaitCollected(WeakReference<?> reference) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (reference.get() != null && System.nanoTime() < deadline) {
            System.gc();
            TimeUnit.MILLISECONDS.sleep(20);
        }
        assertNull(reference.get(), "Discarded object is still strongly retained");
    }

    public static void assertHaveSameElements(boolean[] expected, boolean[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static void assertHaveSameElements(char[] expected, char[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static void assertHaveSameElements(byte[] expected, byte[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static void assertHaveSameElements(short[] expected, short[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static void assertHaveSameElements(int[] expected, int[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static void assertHaveSameElements(long[] expected, long[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static void assertHaveSameElements(float[] expected, float[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static void assertHaveSameElements(double[] expected, double[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static <T> void assertHaveSameElements(T[] expected, T[] actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static <T> void assertHaveSameElements(Collection<? extends T> expected, T[] actual) {
        assertHaveSameElements(expected, N.toList(actual));
    }

    public static <T> void assertHaveSameElements(Collection<? extends T> expected, Collection<? extends T> actual) {
        assertTrue(N.containsSameElements(expected, actual), "Expected: " + N.toString(expected) + ", Actual: " + N.toString(actual));
    }

    public static <T> Iterable<T> createIterable(final T... a) {
        return new Iterable<>() {
            @Override
            public java.util.Iterator<T> iterator() {
                return N.toList(a).iterator();
            }
        };
    }

    public static <T> Iterable<T> createIterable(final Collection<? extends T> c) {
        return new Iterable<>() {
            @Override
            public java.util.Iterator<T> iterator() {
                return (Iterator<T>) c.iterator();
            }
        };
    }

    public static <A, B> BiIterator<A, B> createBiIterator(final Iterable<Pair<A, B>> iterable) {
        if (iterable == null) {
            return BiIterator.empty();
        }

        final Iterator<Pair<A, B>> iter = iterable.iterator();

        return BiIterator.of(Iterators.map(iter, e -> N.newEntry(e.left(), e.right())));
    }

    public static <A, B, C> TriIterator<A, B, C> createTriIterator(final Iterable<Triple<A, B, C>> iterable) {
        if (iterable == null) {
            return TriIterator.empty();
        }

        final Iterator<Triple<A, B, C>> iter = iterable.iterator();

        return TriIterator.generate(() -> iter.hasNext(), t -> {
            final Triple<A, B, C> e = iter.next();
            t.setLeft(e.left());
            t.setMiddle(e.middle());
            t.setRight(e.right());
        });
    }

    protected static <T> Type<T> createType(Class<T> typeClass) {
        return Type.of(typeClass);
    }

    protected static <T> Type<T> createType(TypeReference<T> typeRef) {
        return typeRef.type();
    }

    @SuppressWarnings("rawtypes")
    protected static <T extends Type> T createType(String typeName) {
        return (T) Type.of(typeName);
    }

}
