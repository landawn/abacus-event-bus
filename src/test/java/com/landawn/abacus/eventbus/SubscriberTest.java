package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.landawn.abacus.TestBase;

@Tag("unit")
public class SubscriberTest extends TestBase {

    @Test
    void interface_whenInspected_declaresOneGenericCallback() throws NoSuchMethodException {
        assertTrue(Subscriber.class.isInterface());
        assertTrue(Subscriber.class.isAnnotationPresent(FunctionalInterface.class));
        Method method = Subscriber.class.getDeclaredMethod("on", Object.class);
        assertEquals(void.class, method.getReturnType());
        assertEquals(1, method.getParameterCount());
        assertEquals(Subscriber.class.getTypeParameters()[0], method.getGenericParameterTypes()[0]);
        assertEquals(1, Arrays.stream(Subscriber.class.getDeclaredMethods()).filter(m -> Modifier.isAbstract(m.getModifiers())).count());
    }

    @ParameterizedTest(name = "implementation={0}")
    @MethodSource("implementations")
    void on_whenInvokedDirectly_forwardsEveryValueIncludingNull(String name, Function<List<String>, Subscriber<String>> factory) {
        List<String> received = new ArrayList<>();
        Subscriber<String> subscriber = factory.apply(received);

        subscriber.on("first");
        subscriber.on(null);
        subscriber.on("third");

        assertEquals(Arrays.asList("first", null, "third"), received);
    }

    private static Stream<Arguments> implementations() {
        return Stream.of(Arguments.of("lambda", (Function<List<String>, Subscriber<String>>) events -> value -> events.add(value)),
                Arguments.of("method reference", (Function<List<String>, Subscriber<String>>) events -> events::add),
                Arguments.of("anonymous class", (Function<List<String>, Subscriber<String>>) events -> new Subscriber<String>() {
                    @Override
                    public void on(String event) {
                        events.add(event);
                    }
                }));
    }
}
