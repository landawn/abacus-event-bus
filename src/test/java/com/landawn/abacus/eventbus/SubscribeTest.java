package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ThreadMode;

@Tag("unit")
public class SubscribeTest extends TestBase {

    @Test
    void annotation_whenAttributesAreOmitted_exposesDocumentedDefaults() throws NoSuchMethodException {
        Method method = AnnotationFixture.class.getDeclaredMethod("defaults", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertNotNull(annotation);
        assertEquals(Subscribe.class, annotation.annotationType());
        assertEquals(ThreadMode.DEFAULT, annotation.threadMode());
        assertFalse(annotation.strictEventType());
        assertFalse(annotation.sticky());
        assertEquals("", annotation.eventId());
        assertEquals(0, annotation.intervalMillis());
        assertFalse(annotation.deduplicate());
    }

    @Test
    void annotation_whenAttributesAreConfigured_preservesValues() throws NoSuchMethodException {
        Method method = AnnotationFixture.class.getDeclaredMethod("configured", String.class);
        Subscribe annotation = method.getAnnotation(Subscribe.class);
        assertNotNull(annotation);
        assertEquals(ThreadMode.THREAD_POOL_EXECUTOR, annotation.threadMode());
        assertEquals("testId", annotation.eventId());
        assertTrue(annotation.sticky());
        assertTrue(annotation.strictEventType());
        assertEquals(5000, annotation.intervalMillis());
        assertTrue(annotation.deduplicate());
    }

    @Test
    void annotation_whenInspected_hasRuntimeRetention() {
        Retention retention = Subscribe.class.getAnnotation(Retention.class);
        assertNotNull(retention);
        assertEquals(RetentionPolicy.RUNTIME, retention.value());
    }

    @Test
    void annotation_whenInspected_targetsMethodsOnly() {
        Target target = Subscribe.class.getAnnotation(Target.class);
        assertNotNull(target);
        assertArrayEquals(new ElementType[] { ElementType.METHOD }, target.value());
    }

    private static class AnnotationFixture {
        @Subscribe
        public void defaults(String event) {
        }

        @Subscribe(threadMode = ThreadMode.THREAD_POOL_EXECUTOR, eventId = "testId", sticky = true, strictEventType = true, intervalMillis = 5000, deduplicate = true)
        public void configured(String event) {
        }
    }
}
