package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class EventBusSubIdentifierTest extends TestBase {

    public static class Handler {
        public void on(String event) {
        }

        public void other(String event) {
        }
    }

    @Test
    public void testSubIdentifierEqualsAndHashCode() throws Exception {
        Handler handler = new Handler();
        Method on = Handler.class.getMethod("on", String.class);
        Method other = Handler.class.getMethod("other", String.class);

        EventBus.SubIdentifier protoA = new EventBus.SubIdentifier(on);
        EventBus.SubIdentifier protoB = new EventBus.SubIdentifier(on);
        EventBus.SubIdentifier protoOther = new EventBus.SubIdentifier(other);

        assertEquals(protoA, protoA);
        assertEquals(protoA, protoB);
        assertEquals(protoA.hashCode(), protoB.hashCode());
        assertNotEquals(protoA, protoOther);
        assertNotEquals(protoA, "not-an-identifier");
        assertNotEquals(protoA, null);

        EventBus.SubIdentifier bound = new EventBus.SubIdentifier(protoA, handler, "id", ThreadMode.DEFAULT);
        EventBus.SubIdentifier boundSame = new EventBus.SubIdentifier(protoA, handler, "id", ThreadMode.DEFAULT);
        EventBus.SubIdentifier boundOtherId = new EventBus.SubIdentifier(protoA, handler, "other", ThreadMode.DEFAULT);
        EventBus.SubIdentifier boundOtherInstance = new EventBus.SubIdentifier(protoA, new Handler(), "id", ThreadMode.DEFAULT);
        EventBus.SubIdentifier boundOtherMode = new EventBus.SubIdentifier(protoA, handler, "id", ThreadMode.THREAD_POOL_EXECUTOR);

        assertEquals(bound, boundSame);
        assertEquals(bound.hashCode(), boundSame.hashCode());
        assertNotEquals(bound, boundOtherId);
        assertNotEquals(bound, boundOtherInstance);
        assertNotEquals(bound, boundOtherMode);
        assertTrue(bound.toString().contains("on"));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1_000_000_000L, 1_000_000_000L, Long.MAX_VALUE - 1, Long.MIN_VALUE + 1 })
    public void testEventIntervalUsesMonotonicElapsedTimeAndHonorsBoundary(long firstPostTime) throws NoSuchMethodException {
        class SubscriberWithInterval {
            @Subscribe(intervalMillis = 100)
            public void onEvent(String event) {
                // No-op: this test exercises the interval state carried by the subscriber metadata.
            }
        }

        final Method method = SubscriberWithInterval.class.getDeclaredMethod("onEvent", String.class);
        final EventBus.SubIdentifier sub = new EventBus.SubIdentifier(method);
        final long intervalNanos = TimeUnit.MILLISECONDS.toNanos(100);

        assertFalse(sub.isWithinPostInterval(firstPostTime));

        sub.recordPostTime(firstPostTime);

        assertTrue(sub.isWithinPostInterval(firstPostTime));
        assertTrue(sub.isWithinPostInterval(firstPostTime + intervalNanos - 1));
        assertFalse(sub.isWithinPostInterval(firstPostTime + intervalNanos));
        assertFalse(sub.isWithinPostInterval(firstPostTime - 1));
    }
}
