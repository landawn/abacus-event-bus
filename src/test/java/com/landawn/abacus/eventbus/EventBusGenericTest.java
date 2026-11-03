package com.landawn.abacus.eventbus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
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

    @ParameterizedTest
    @MethodSource("boundedWildcardOwnerSubscribers")
    void register_whenWildcardOwnerVariableIsBounded_routesUsingDeclaredBound(NumberOwner<?>.Handler handler, List<Object> expected) {
        EventBus bus = EventBus.create();
        bus.register(handler); // the declared bound reifies the event type, so no event ID is needed

        bus.post(7);
        bus.post(7.5);
        bus.post("text");

        assertEquals(expected, handler.values);
        assertFalse(bus.hasSubscribers(String.class));
    }

    @Test
    void register_whenWildcardOwnerVariableIsBounded_annotatedHandlerMatchesOnlyDeclaredBound() {
        EventBus bus = EventBus.create();
        UnboundedNumberOwnerAnnotatedHandler handler = new UnboundedNumberOwnerAnnotatedHandler(new NumberOwner<>());
        bus.register(handler);

        bus.post(7);
        bus.post("text");

        assertEquals(List.of(7), handler.values);
        assertEquals(List.of(handler), bus.subscribers(Integer.class));
        assertTrue(bus.subscribers(String.class).isEmpty());
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
    void register_whenRawSubclassOverloadsInheritedHandler_registersBoth() {
        // Below the raw supertype there is no bridge, so handle(Number) overloads (not overrides) handle(T).
        final EventBus bus = EventBus.create();
        final RawNumberOverload handler = new RawNumberOverload();
        bus.register(handler);
        bus.post(7);
        bus.post("text");
        assertEquals(2, handler.values.size());
        assertTrue(handler.values.containsAll(List.of("base:7", "overload:7")));
    }

    @Test
    void register_whenRawSubclassOverloadsHandlerOfNonPublicBase_registersBoth() {
        // The public middle class re-declares the inherited handle(Object) as a visibility bridge, which redirects to
        // no other declaration, so handle(Number) still only overloads it.
        final EventBus bus = EventBus.create();
        final RawNumberOverloadOfHiddenBase handler = new RawNumberOverloadOfHiddenBase();
        bus.register(handler);
        bus.post(7);
        bus.post("text");
        assertEquals(2, handler.values.size());
        assertTrue(handler.values.containsAll(List.of("base:7", "overload:7")));
    }

    @Test
    void register_whenSubclassOfNonPublicBaseOverridesHandler_deliversOnce() {
        final EventBus bus = EventBus.create();
        final HiddenBaseOverride handler = new HiddenBaseOverride();
        bus.register(handler);
        bus.post("text");
        assertEquals(List.of("override:text"), handler.values);
    }

    @Test
    void register_whenBoundedSubclassOfNonPublicBaseOverridesHandlerByErasure_deliversOnce() {
        // handle(Number) overrides the inherited handle(U), whose U erases to Number, so the handle(Object) bridge
        // leads to it; registering the inherited declaration too would invoke the override twice per event.
        final EventBus bus = EventBus.create();
        final BoundedHiddenBaseOverride<Integer> handler = new BoundedHiddenBaseOverride<>();
        bus.register(handler);
        bus.post(7);
        bus.post("text");
        assertEquals(List.of("override:7"), handler.values);
    }

    @Test
    void register_whenBoundedSubclassOfNonPublicBaseOverridesSubscriberContractByErasure_deliversOnce() {
        final EventBus bus = EventBus.create();
        final BoundedHiddenContractOverride<Integer> handler = new BoundedHiddenContractOverride<>();
        bus.register(handler);
        bus.post(7);
        bus.post("text");
        assertEquals(List.of("override:7"), handler.values);
    }

    @Test
    void register_whenRawSubclassOfNonPublicMiddleOverloadsHandler_registersBoth() {
        // As the first public class, the raw subclass holds the visibility bridge handle(Object) beside its own
        // handle(Number); below the raw supertype that cannot override the inherited handle(T), so both are handlers.
        final EventBus bus = EventBus.create();
        final RawOverloadBesideVisibilityBridge handler = new RawOverloadBesideVisibilityBridge();
        bus.register(handler);
        bus.post(7);
        bus.post("text");
        assertEquals(2, handler.values.size());
        assertTrue(handler.values.containsAll(List.of("base:7", "overload:7")));
    }

    @Test
    void register_whenRawSubclassOfNonPublicMiddleOverloadsSubscriberContract_registersBoth() {
        final EventBus bus = EventBus.create();
        final RawContractOverloadBesideVisibilityBridge handler = new RawContractOverloadBesideVisibilityBridge();
        bus.register(handler);
        bus.post(7);
        bus.post("text");
        assertEquals(2, handler.values.size());
        assertTrue(handler.values.containsAll(List.of("contract:7", "overload:7")));
    }

    @Test
    void register_whenRawSubclassOverloadsSubscriberContract_registersBoth() {
        // Below the raw supertype on(Object) is not bridged to on(Number), so the annotated method only overloads the contract.
        final EventBus bus = EventBus.create();
        final RawSubscriberOverload handler = new RawSubscriberOverload();
        bus.register(handler);
        bus.post(7);
        bus.post("text");
        assertEquals(2, handler.values.size());
        assertTrue(handler.values.containsAll(List.of("base:7", "overload:7")));
    }

    @Test
    void register_whenSubscriberContractIsOverriddenWithAnnotation_usesOverrideOnly() {
        // on(Object) is bridged to the annotated on(String), so the contract is that override, not a second handler.
        final EventBus bus = EventBus.create();
        final AnnotatedContractOverride handler = new AnnotatedContractOverride();
        bus.register(handler);
        bus.post("unqualified");
        bus.post("override", "qualified");
        assertEquals(List.of("qualified"), handler.values);
        assertTrue(bus.subscribers(String.class).isEmpty());
    }

    @Test
    void register_whenSuperinterfaceDeclaresAnnotatedStaticMethod_ignoresIt() {
        final EventBus bus = EventBus.create();
        final StaticHelperImplementor handler = new StaticHelperImplementor();
        bus.register(handler);
        bus.post("text");
        assertEquals(List.of("text"), handler.values);
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
            // Skip unnamed-package classes (e.g. IDE-compiled tools in the same output folder): a module can't contain them.
            for (final Path file : (Iterable<Path>) files
                    .filter(p -> p.toString().endsWith(".class") && eventBusClasses.relativize(p).getNameCount() > 1)::iterator) {
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
        final List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        // The fallback setAccessible path runs in this child. Inherit the agent so that execution is in the report.
        ManagementFactory.getRuntimeMXBean()
                .getInputArguments()
                .stream()
                .filter(argument -> argument.startsWith("-javaagent:") && argument.contains("jacoco"))
                .map(argument -> argument.contains("append=") ? argument : argument + ",append=true")
                .forEach(command::add);
        // ALL-SYSTEM: abacus-common, as an automatic module, needs modules such as java.sql resolved.
        command.add("--add-modules");
        command.add("ALL-SYSTEM");
        command.add("--module-path");
        command.add(modules + File.pathSeparator + classes);
        command.add("-m");
        command.add("app/app.handlers.Main");
        final Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile()).start();
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

    public static class NumberOwner<T extends Number> {
        public class Handler implements Subscriber<T> {
            final List<Object> values = new ArrayList<>();

            @Override
            public void on(T event) {
                values.add(event);
            }
        }

        public class AnnotatedHandler {
            final List<Object> values = new ArrayList<>();

            @Subscribe
            public void handle(T event) {
                values.add(event);
            }
        }
    }

    public static class UnboundedNumberOwnerHandler extends NumberOwner<?>.Handler {
        UnboundedNumberOwnerHandler(NumberOwner<?> owner) {
            owner.super();
        }
    }

    public static class LowerBoundNumberOwnerHandler extends NumberOwner<? super Integer>.Handler {
        LowerBoundNumberOwnerHandler(NumberOwner<? super Integer> owner) {
            owner.super();
        }
    }

    public static class UpperBoundNumberOwnerHandler extends NumberOwner<? extends Integer>.Handler {
        UpperBoundNumberOwnerHandler(NumberOwner<? extends Integer> owner) {
            owner.super();
        }
    }

    public static class UnboundedNumberOwnerAnnotatedHandler extends NumberOwner<?>.AnnotatedHandler {
        UnboundedNumberOwnerAnnotatedHandler(NumberOwner<?> owner) {
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

    public static class NumberHandlerMiddle<U extends Number> extends GenericHandlerBase<U> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawNumberOverload extends NumberHandlerMiddle {
        @Subscribe
        public void handle(Number event) {
            values.add("overload:" + event);
        }
    }

    static class HiddenGenericHandlerBase<T> {
        final List<String> values = new ArrayList<>();

        @Subscribe
        public void handle(T event) {
            values.add("base:" + event);
        }
    }

    public static class PublicNumberHandlerMiddle<U extends Number> extends HiddenGenericHandlerBase<U> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawNumberOverloadOfHiddenBase extends PublicNumberHandlerMiddle {
        @Subscribe
        public void handle(Number event) {
            values.add("overload:" + event);
        }
    }

    public static class HiddenBaseOverride extends HiddenGenericHandlerBase<String> {
        @Override
        @Subscribe
        public void handle(String event) {
            values.add("override:" + event);
        }
    }

    public static class BoundedHiddenBaseOverride<U extends Number> extends HiddenGenericHandlerBase<U> {
        @Override
        @Subscribe
        public void handle(Number event) {
            values.add("override:" + event);
        }
    }

    static class HiddenGenericSubscriberBase<T> implements Subscriber<T> {
        final List<String> values = new ArrayList<>();

        @Override
        public void on(T event) {
            values.add("contract:" + event);
        }
    }

    public static class BoundedHiddenContractOverride<U extends Number> extends HiddenGenericSubscriberBase<U> {
        @Override
        @Subscribe
        public void on(Number event) {
            values.add("override:" + event);
        }
    }

    static class HiddenNumberHandlerMiddle<U extends Number> extends HiddenGenericHandlerBase<U> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawOverloadBesideVisibilityBridge extends HiddenNumberHandlerMiddle {
        @Subscribe
        public void handle(Number event) {
            values.add("overload:" + event);
        }
    }

    static class HiddenNumberSubscriberMiddle<U extends Number> extends HiddenGenericSubscriberBase<U> {
    }

    @SuppressWarnings("rawtypes")
    public static class RawContractOverloadBesideVisibilityBridge extends HiddenNumberSubscriberMiddle {
        @Subscribe
        public void on(Number event) {
            values.add("overload:" + event);
        }
    }

    public interface StaticAnnotatedHelper {
        @Subscribe
        static void helper(String event) {
            throw new AssertionError("A static interface method is not a handler");
        }
    }

    public static class StaticHelperImplementor implements StaticAnnotatedHelper {
        final List<String> values = new ArrayList<>();

        @Subscribe
        public void onEvent(String event) {
            values.add(event);
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

    public static class NumberSubscriberMiddle<U extends Number> extends GenericSubscriberBase<U> {
    }

    public static class AnnotatedContractOverride implements Subscriber<String> {
        final List<String> values = new ArrayList<>();

        @Override
        @Subscribe(eventId = "override")
        public void on(String event) {
            values.add(event);
        }
    }

    @SuppressWarnings("rawtypes")
    public static class RawSubscriberOverload extends NumberSubscriberMiddle {
        @Subscribe
        public void on(Number event) {
            values.add("overload:" + event);
        }
    }

    private static Stream<Arguments> wildcardOwnerSubscribers() {
        return Stream.of(Arguments.of(new UpperBoundOwnerHandler(new Owner<>()), List.of("text")),
                Arguments.of(new LowerBoundOwnerHandler(new Owner<>()), List.of("text", 42)),
                Arguments.of(new UnboundedOwnerHandler(new Owner<>()), List.of("text", 42)));
    }

    private static Stream<Arguments> boundedWildcardOwnerSubscribers() {
        return Stream.of(Arguments.argumentSet("NumberOwner<?>", new UnboundedNumberOwnerHandler(new NumberOwner<>()), List.of(7, 7.5)),
                Arguments.argumentSet("NumberOwner<? super Integer>", new LowerBoundNumberOwnerHandler(new NumberOwner<>()), List.of(7, 7.5)),
                Arguments.argumentSet("NumberOwner<? extends Integer>", new UpperBoundNumberOwnerHandler(new NumberOwner<>()), List.of(7)));
    }

    interface GenericHandle<T> {
        @Subscribe
        void handle(T event);
    }

    interface StringHandle extends GenericHandle<String> {
        List<String> events();

        @Subscribe
        @Override
        default void handle(final String event) {
            events().add(event);
        }
    }

    // GenericHandle is a direct superinterface declared before StringHandle, so it is scanned first.
    static class SuperInterfaceDeclaredFirst implements GenericHandle<String>, StringHandle {
        final List<String> events = new ArrayList<>();

        @Override
        public List<String> events() {
            return events;
        }
    }

    static class SubInterfaceDeclaredFirst implements StringHandle, GenericHandle<String> {
        final List<String> events = new ArrayList<>();

        @Override
        public List<String> events() {
            return events;
        }
    }

    abstract static class AbstractGenericHandle implements GenericHandle<String> {
    }

    // GenericHandle is reached through the superclass, which is scanned before the declared StringHandle.
    static class SuperInterfaceViaSuperclass extends AbstractGenericHandle implements StringHandle {
        final List<String> events = new ArrayList<>();

        @Override
        public List<String> events() {
            return events;
        }
    }

    static Stream<StringHandle> overriddenSuperInterfaceSubscribers() {
        return Stream.of(new SuperInterfaceDeclaredFirst(), new SubInterfaceDeclaredFirst(), new SuperInterfaceViaSuperclass());
    }

    @ParameterizedTest
    @MethodSource("overriddenSuperInterfaceSubscribers")
    void register_whenSuperInterfaceIsScannedBeforeOverridingSubInterface_registersHandlerOnce(final StringHandle subscriber) {
        EventBus bus = EventBus.create();
        bus.register(subscriber);
        bus.post("event");

        assertEquals(List.of("event"), subscriber.events());
        assertEquals(List.of(subscriber), bus.subscribers(String.class));
    }

    interface IdStringHandle extends GenericHandle<String> {
        List<String> events();

        @Subscribe(eventId = "strings")
        @Override
        default void handle(final String event) {
            events().add(event);
        }
    }

    static class IdOverrideAfterSuperInterface implements GenericHandle<String>, IdStringHandle {
        final List<String> events = new ArrayList<>();

        @Override
        public List<String> events() {
            return events;
        }
    }

    @Test
    void register_whenOverridingSubInterfaceChangesEventId_usesOnlyTheMostSpecificDeclaration() {
        EventBus bus = EventBus.create();
        IdOverrideAfterSuperInterface subscriber = new IdOverrideAfterSuperInterface();
        bus.register(subscriber);
        bus.post("unscoped");
        bus.post("strings", "scoped");

        assertEquals(List.of("scoped"), subscriber.events());
    }
}
