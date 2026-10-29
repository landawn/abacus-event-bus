package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Stream;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ClassUtil;

@Tag("unit")
public class EventBusGenericTest extends TestBase {

    @Test
    void register_whenSubscriberHasOverloads_discoversOnlyGenericContract() {
        final EventBus bus = EventBus.create();
        final Overloaded handler = new Overloaded();
        bus.register(handler, "events");
        bus.post("events", "");
        bus.post("events", "\u03bb\ud83d\ude00");
        bus.post("events", Integer.MAX_VALUE);
        assertEquals(2, handler.strings);
        assertEquals(0, handler.integers);
        assertFalse(bus.hasSubscribers(Integer.class));
    }

    @Test
    void register_whenObjectSubscriberIsAnnotated_discoversContractAndAdditionalHandler() {
        EventBus bus = EventBus.create();
        AnnotatedObjectSubscriber handler = new AnnotatedObjectSubscriber();
        bus.register(handler);

        bus.post("text");
        bus.post(42);

        assertEquals(List.of("text", 42), handler.objects);
        assertEquals(List.of(42), handler.numbers);
        assertEquals(1, bus.countOfSubscribers());
    }

    @Test
    void register_whenPublicHandlerIsInherited_deliversMatchingEvent() {
        EventBus eventBus = EventBus.create();
        InheritedSubscriberChild subscriber = new InheritedSubscriberChild();

        eventBus.register(subscriber, "inherited");
        eventBus.post("inherited", "inherited-event");

        assertEquals(List.of("inherited-event"), subscriber.receivedEvents);
    }

    @Test
    void register_whenSubclassOverridesHandler_usesSubclassMetadata() {
        EventBus eventBus = EventBus.create();
        ChildOverrideSubscriber subscriber = new ChildOverrideSubscriber();

        eventBus.register(subscriber);
        eventBus.post("base", "ignored");
        eventBus.post("child", "handled");

        assertEquals(List.of("handled"), subscriber.events);
    }

    @Test
    void register_whenClassAndInterfaceAnnotationsDiffer_prefersClassMetadata() {
        final EventBus bus = EventBus.create();
        final ClassWins handler = new ClassWins();
        bus.register(handler);
        bus.post("class", "correct");
        bus.post("interface", "wrong");
        assertEquals(List.of("correct"), handler.values);
    }

    @Test
    void register_whenDiamondInterfacesAgree_deliversSpecializedHandlerOnce() {
        EventBus bus = EventBus.create();
        DiamondHandler handler = new DiamondHandler();
        bus.register(handler);

        bus.post("diamond", "matching");
        bus.post("diamond", 42);
        bus.post("other", "wrong channel");

        assertEquals(List.of("matching"), handler.values);
        assertEquals(List.of(handler), bus.subscribers("diamond", String.class));
        assertFalse(bus.hasSubscribers(Integer.class));
    }

    @Test
    void register_whenInheritedAnnotationsConflict_leavesRegistryEmpty() {
        final EventBus bus = EventBus.create();
        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> bus.register(new Conflicting()));
        assertTrue(error.getMessage().contains("Conflicting @Subscribe"));
        assertEquals(0, bus.countOfSubscribers());
        final ResolvedConflict handler = new ResolvedConflict();
        bus.register(handler);
        bus.post("resolved", "");
        bus.post("other", "wrong");
        assertEquals(1, handler.calls);
    }

    @Test
    void register_whenGenericTypeIsResolvedThroughHierarchy_allowsUnqualifiedDelivery() {
        final EventBus bus = EventBus.create();
        final Strings handler = new Strings();
        bus.register(handler);
        bus.post("");
        bus.post("\u03bb\ud83d\ude00");
        bus.post(1);
        assertEquals(List.of("", "\u03bb\ud83d\ude00"), handler.values);
        assertFalse(bus.hasSubscribers(Integer.class));
        assertEquals(List.of(handler), bus.subscribers(String.class));
        bus.unregister(handler);
        assertFalse(bus.hasSubscribers(String.class));
        assertThrows(IllegalArgumentException.class, () -> bus.post(null));
    }

    @Test
    void register_whenGenericOverrideIsAnnotated_usesChildConfigurationOnce() {
        final EventBus bus = EventBus.create();
        final ConcreteHandler handler = new ConcreteHandler();
        bus.postSticky("parent", "wrong");
        bus.postSticky("child", "retained");
        bus.register(handler);
        bus.post("parent", "wrong");
        bus.post("child", "live");
        assertEquals(List.of("retained", "live"), handler.values);
        assertFalse(bus.hasSubscribers(Integer.class));
        assertTrue(bus.subscribers("parent", String.class).isEmpty());
    }

    @Test
    void register_whenGenericOverrideIsUnannotated_inheritsMetadataAndType() {
        final EventBus bus = EventBus.create();
        final InheritedAnnotation handler = new InheritedAnnotation();
        bus.register(handler);
        bus.post("parent", "\u03bb");
        assertEquals(List.of("\u03bb"), handler.values);
        assertFalse(bus.hasSubscribers(Integer.class));
    }

    @Test
    void register_whenInheritedGenericHandlerIsStrict_preservesExactType() {
        final EventBus bus = EventBus.create();
        final AnnotatedStrings handler = new AnnotatedStrings();
        bus.register(handler);
        assertFalse(bus.hasSubscribers(String.class));
        assertTrue(bus.hasSubscribers(CharSequence.class));
        assertEquals(List.of(handler), bus.subscribers("inherited", CharSequence.class));
    }

    @Test
    void register_whenPrimitiveAndBoxedOverloadsExist_keepsBothHandlers() {
        final EventBus bus = EventBus.create();
        final PrimitiveOverloads handler = new PrimitiveOverloads();
        bus.register(handler);
        bus.post(Integer.MIN_VALUE);
        assertEquals(1, handler.primitives);
        assertEquals(1, handler.boxes);
    }

    @Test
    void register_whenParameterIsGenericArray_resolvesComponentType() {
        final EventBus bus = EventBus.create();
        final StringArrays handler = new StringArrays();
        final String[] empty = new String[0];
        final String[] values = { "\u03bb", null };
        bus.register(handler);
        bus.post(empty);
        bus.post(values);
        bus.post(new Integer[] { 1 });
        assertEquals(2, handler.values.size());
        assertSame(empty, handler.values.get(0));
        assertSame(values, handler.values.get(1));
        assertFalse(bus.hasSubscribers(Integer[].class));
    }

    @Test
    void register_whenGenericBoundIsUnresolved_filtersByBound() {
        final EventBus bus = EventBus.create();
        final Bounded<Number> handler = new Bounded<>();
        bus.register(handler);
        bus.post(1);
        bus.post(Double.NaN);
        bus.post("wrong");
        assertEquals(List.of(1, Double.NaN), handler.values);
    }

    @Test
    void register_whenNestedSubscriberUsesOwnerType_resolvesGenericArgument() {
        final EventBus bus = EventBus.create();
        final Owner<String>.Handler handler = new Owner<String>().new Handler() {
        };
        bus.register(handler);
        bus.post("\u03bb");
        bus.post(1);
        assertEquals(List.of("\u03bb"), handler.values);
        assertFalse(bus.hasSubscribers(Integer.class));
    }

    @ParameterizedTest
    @MethodSource("wildcardOwnerSubscribers")
    void register_whenOwnerTypeIsWildcard_routesUsingUpperBound(Owner<?>.Handler handler, List<Object> expected) {
        EventBus bus = EventBus.create();
        bus.register(handler, "wildcard");

        bus.post("wildcard", "text");
        bus.post("wildcard", 42);
        bus.post("other", "wrong channel");

        assertEquals(expected, handler.values);
        assertEquals(List.of(handler), bus.subscribers("wildcard", String.class));
    }

    @Test
    void register_whenOwnerTypeIsRebound_preservesLexicalArguments() {
        final EventBus bus = EventBus.create();
        final SwappingOwner<String, Integer>.Swapped handler = new SwappingOwner<String, Integer>().new Swapped(new SwappingOwner<>()) {
        };
        bus.register(handler);
        bus.post("\u03bb");
        bus.post(1);
        assertEquals(List.of("\u03bb"), handler.values);
    }

    @Test
    void register_whenRawSubclassOverridesGenericHandler_deliversOnce() {
        final EventBus bus = EventBus.create();
        final RawOverride handler = new RawOverride();
        bus.register(handler);
        bus.post("text");
        assertEquals(List.of("override:text"), handler.values);
    }

    @Test
    void register_whenRawSubclassOverrideIsAnnotated_usesOverrideMetadata() {
        final EventBus bus = EventBus.create();
        final RawAnnotatedOverride handler = new RawAnnotatedOverride();
        bus.register(handler);
        bus.post("unqualified");
        bus.post("override", "qualified");
        assertEquals(List.of("override:qualified"), handler.values);
        assertTrue(bus.subscribers(String.class).isEmpty());
    }

    @Test
    void register_whenRawSubclassAnnotatesSubscriberContract_usesOverrideMetadata() {
        final EventBus bus = EventBus.create();
        final RawSubscriberOverride handler = new RawSubscriberOverride();
        bus.register(handler);
        bus.post("unqualified");
        bus.post("override", "qualified");
        assertEquals(List.of("override:qualified"), handler.values);
        assertTrue(bus.subscribers(String.class).isEmpty());
    }

    @Test
    void register_whenLambdaTargetsSubinterfaceFixingType_filtersByThatType() {
        final EventBus bus = EventBus.create();
        final List<String> received = new ArrayList<>();
        final StringSubscriber subscriber = received::add;

        bus.register(subscriber); // the fixed type argument makes an event ID unnecessary
        bus.post("text");
        bus.post(42);

        assertEquals(List.of("text"), received);
        assertEquals(List.of(subscriber), bus.subscribers(String.class));
        assertTrue(bus.subscribers(Integer.class).isEmpty());
    }

    @ParameterizedTest(name = "opensTo={0}")
    @ValueSource(strings = { "abacus.event.bus", "abacus.common" })
    void register_whenNamedModuleOpensHandlerPackageToLibrary_deliversEvent(String opensTo, @TempDir Path directory) throws Exception {
        // EventBus and abacus-common become separate automatic modules; the app opens its package to one of them only.
        // Opening it to abacus.common is what the ClassUtil.setAccessible fallback keeps working.
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "This test requires a JDK");
        final Path modules = Files.createDirectories(directory.resolve("modules"));
        final Path eventBusClasses = Path.of(EventBus.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        try (OutputStream out = Files.newOutputStream(modules.resolve("abacus-event-bus.jar"));
             JarOutputStream jar = new JarOutputStream(out);
             Stream<Path> files = Files.walk(eventBusClasses)) {
            for (final Path file : (Iterable<Path>) files.filter(p -> p.toString().endsWith(".class"))::iterator) {
                jar.putNextEntry(new JarEntry(eventBusClasses.relativize(file).toString().replace('\\', '/')));
                jar.write(Files.readAllBytes(file));
                jar.closeEntry();
            }
        }
        Files.copy(Path.of(ClassUtil.class.getProtectionDomain().getCodeSource().getLocation().toURI()), modules.resolve("abacus-common.jar"));

        final Path source = Files.createDirectories(directory.resolve("src/app/handlers"));
        Files.writeString(directory.resolve("src/module-info.java"), "module app { requires abacus.event.bus; opens app.handlers to " + opensTo + "; }");
        Files.writeString(source.resolve("Main.java"), """
                package app.handlers;
                public class Main {
                    public static void main(String[] args) {
                        StringBuilder seen = new StringBuilder();
                        Object handler = new Object() {
                            @com.landawn.abacus.eventbus.Subscribe
                            public void on(String event) { seen.append(event); }
                        };
                        com.landawn.abacus.eventbus.EventBus.create().register(handler).post("delivered");
                        System.out.print("RESULT=" + seen);
                    }
                }
                """);
        final Path classes = directory.resolve("classes");
        assertEquals(0, compiler.run(null, null, null, "-nowarn", "-d", classes.toString(), "--module-path", modules.toString(),
                directory.resolve("src/module-info.java").toString(), source.resolve("Main.java").toString()));

        final Path output = directory.resolve("output.txt");
        // ALL-SYSTEM: abacus-common, as an automatic module, needs modules such as java.sql resolved.
        final Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "--add-modules", "ALL-SYSTEM",
                "--module-path", modules + File.pathSeparator + classes, "-m", "app/app.handlers.Main").redirectErrorStream(true)
                        .redirectOutput(output.toFile())
                        .start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Child JVM did not finish");
        } finally {
            process.destroyForcibly();
        }
        final String text = Files.readString(output, StandardCharsets.UTF_8);
        assertTrue(text.contains("RESULT=delivered"), text);
        assertEquals(0, process.exitValue(), text);
    }

    public interface DiamondRoot<T> {
        @Subscribe(eventId = "diamond")
        void handle(T event);
    }

    public interface DiamondLeft extends DiamondRoot<String> {
        @Override
        @Subscribe(eventId = "diamond")
        void handle(String event);
    }

    public interface DiamondRight extends DiamondRoot<String> {
        @Override
        @Subscribe(eventId = "diamond")
        void handle(String event);
    }

    public static class DiamondHandler implements DiamondLeft, DiamondRight {
        final List<String> values = new ArrayList<>();

        @Override
        public void handle(String event) {
            values.add(event);
        }
    }

    public static class AnnotatedObjectSubscriber implements Subscriber<Object> {
        final List<Object> objects = new ArrayList<>();
        final List<Number> numbers = new ArrayList<>();

        @Override
        @Subscribe
        public void on(Object event) {
            objects.add(event);
        }

        @Subscribe
        public void onNumber(Number event) {
            numbers.add(event);
        }
    }

    public static class Overloaded implements Subscriber<String> {
        int strings;
        int integers;

        @Override
        public void on(String event) {
            strings++;
        }

        public void on(Integer event) {
            integers++;
        }
    }

    public interface GenericHandler<T> {
        @Subscribe(eventId = "parent", sticky = true)
        void handle(T event);
    }

    public static class ConcreteHandler implements GenericHandler<String> {
        final List<String> values = new ArrayList<>();

        @Override
        @Subscribe(eventId = "child", sticky = true)
        public void handle(String event) {
            values.add(event);
        }
    }

    public static class InheritedAnnotation implements GenericHandler<String> {
        final List<String> values = new ArrayList<>();

        @Override
        public void handle(String event) {
            values.add(event);
        }
    }

    public static class GenericBase<T> implements Subscriber<T> {
        final List<T> values = new ArrayList<>();

        @Override
        public void on(T event) {
            values.add(event);
        }
    }

    public static class Middle<T> extends GenericBase<T> {
    }

    public static class Strings extends Middle<String> {
    }

    public static class ArrayBase<T> implements Subscriber<T[]> {
        final List<T[]> values = new ArrayList<>();

        @Override
        public void on(T[] event) {
            values.add(event);
        }
    }

    public static class StringArrays extends ArrayBase<String> {
    }

    public static class Bounded<T extends Number> implements Subscriber<T> {
        final List<T> values = new ArrayList<>();

        @Override
        public void on(T event) {
            values.add(event);
        }
    }

    public static class PrimitiveOverloads {
        int primitives;
        int boxes;

        @Subscribe
        public void handle(int value) {
            primitives++;
        }

        @Subscribe
        public void handle(Integer value) {
            boxes++;
        }
    }

    public interface ConfiguredInterface {
        @Subscribe(eventId = "interface")
        void handle(String event);
    }

    public static class ConfiguredBase {
        final List<String> values = new ArrayList<>();

        @Subscribe(eventId = "class")
        public void handle(String event) {
            values.add(event);
        }
    }

    public static class ClassWins extends ConfiguredBase implements ConfiguredInterface {
    }

    public static class AnnotatedBase<T> implements Subscriber<T> {
        final List<T> values = new ArrayList<>();

        @Override
        @Subscribe(eventId = "inherited", strictEventType = true)
        public void on(T event) {
            values.add(event);
        }
    }

    public static class AnnotatedStrings extends AnnotatedBase<CharSequence> {
    }

    public interface OtherConfiguredInterface {
        @Subscribe(eventId = "other")
        void handle(String event);
    }

    public static class Conflicting implements ConfiguredInterface, OtherConfiguredInterface {
        @Override
        public void handle(String event) {
        }
    }

    public static class ResolvedConflict implements ConfiguredInterface, OtherConfiguredInterface {
        int calls;

        @Override
        @Subscribe(eventId = "resolved")
        public void handle(String event) {
            calls++;
        }
    }

    public static class Owner<T> {
        public class Handler implements Subscriber<T> {
            final List<T> values = new ArrayList<>();

            @Override
            public void on(T event) {
                values.add(event);
            }
        }
    }

    public static class UpperBoundOwnerHandler extends Owner<? extends CharSequence>.Handler {
        UpperBoundOwnerHandler(Owner<? extends CharSequence> owner) {
            owner.super();
        }
    }

    public static class LowerBoundOwnerHandler extends Owner<? super String>.Handler {
        LowerBoundOwnerHandler(Owner<? super String> owner) {
            owner.super();
        }
    }

    public static class UnboundedOwnerHandler extends Owner<?>.Handler {
        UnboundedOwnerHandler(Owner<?> owner) {
            owner.super();
        }
    }

    public static class SwappingOwner<A, B> {
        public class Base<T> implements Subscriber<T> {
            final List<T> values = new ArrayList<>();

            @Override
            public void on(T event) {
                values.add(event);
            }
        }

        public class Swapped extends SwappingOwner<B, A>.Base<A> {
            public Swapped(final SwappingOwner<B, A> owner) {
                owner.super();
            }
        }
    }

    static final class ChildOverrideSubscriber extends BaseOverrideSubscriber {
        final List<String> events = new ArrayList<>();

        @Override
        @Subscribe(eventId = "child")
        public void onEvent(String event) {
            events.add(event);
        }
    }

    public static class InheritedSubscriberChild extends InheritedSubscriberParent {
    }

    public static class BaseOverrideSubscriber {
        @Subscribe(eventId = "base")
        public void onEvent(String event) {
            throw new AssertionError("Base subscriber metadata should be overridden");
        }
    }

    public static class InheritedSubscriberParent {
        final List<String> receivedEvents = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            receivedEvents.add(event);
        }
    }

    public interface StringSubscriber extends Subscriber<String> {
    }

    public static class GenericHandlerBase<T> {
        final List<String> values = new ArrayList<>();

        @Subscribe
        public void handle(T event) {
            values.add("base:" + event);
        }
    }

    public static class StringHandlerMiddle<X> extends GenericHandlerBase<String> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawOverride extends StringHandlerMiddle {
        @Override
        @Subscribe
        public void handle(Object event) {
            values.add("override:" + event);
        }
    }

    @SuppressWarnings("rawtypes")
    public static class RawAnnotatedOverride extends StringHandlerMiddle {
        @Override
        @Subscribe(eventId = "override")
        public void handle(Object event) {
            values.add("override:" + event);
        }
    }

    public static class GenericSubscriberBase<T> implements Subscriber<T> {
        final List<String> values = new ArrayList<>();

        @Override
        public void on(T event) {
            values.add("base:" + event);
        }
    }

    public static class StringSubscriberMiddle<X> extends GenericSubscriberBase<String> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawSubscriberOverride extends StringSubscriberMiddle {
        @Override
        @Subscribe(eventId = "override")
        public void on(Object event) {
            values.add("override:" + event);
        }
    }

    private static Stream<Arguments> wildcardOwnerSubscribers() {
        return Stream.of(Arguments.of(new UpperBoundOwnerHandler(new Owner<>()), List.of("text")),
                Arguments.of(new LowerBoundOwnerHandler(new Owner<>()), List.of("text", 42)),
                Arguments.of(new UnboundedOwnerHandler(new Owner<>()), List.of("text", 42)));
    }
}
