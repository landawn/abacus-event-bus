# abacus-event-bus

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn.abacus/abacus-event-bus.svg)](https://central.sonatype.com/artifact/com.landawn.abacus/abacus-event-bus)
[![Javadocs](https://img.shields.io/badge/javadoc-reference-brightgreen.svg)](https://www.javadoc.io/doc/com.landawn.abacus/abacus-event-bus)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A lightweight, thread-safe Java event bus for communication between loosely coupled components in the same JVM. Publishers post ordinary Java objects; subscribers receive matching events through annotated methods or the `Subscriber<E>` functional interface.

The library supports synchronous and executor-based delivery, event IDs, type hierarchy matching, sticky events, interval throttling, and consecutive-event deduplication. It builds on [abacus-common](https://github.com/landawn/abacus-common) and requires **Java 21 or later**.

This guide describes the source in this checkout. Dependency versions below match [pom.xml](pom.xml): **abacus-event-bus 1.1** and **abacus-common 8.3.0**. Published Javadoc may describe a different release.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Creating and choosing a bus](#creating-and-choosing-a-bus)
- [Defining subscribers](#defining-subscribers)
- [Event IDs and type matching](#event-ids-and-type-matching)
- [Registration and listener lifecycle](#registration-and-listener-lifecycle)
- [Threading and executors](#threading-and-executors)
- [Sticky events](#sticky-events)
- [Throttling and deduplication](#throttling-and-deduplication)
- [Inspecting a bus](#inspecting-a-bus)
- [API reference](#api-reference)
- [Failures and troubleshooting](#failures-and-troubleshooting)
- [Building and testing](#building-and-testing)
- [Further documentation](#further-documentation)
- [License](#license)

## Installation

`abacus-common` is declared with Maven's `provided` scope in this project. Applications must supply it at runtime; add both dependencies explicitly.

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>com.landawn.abacus</groupId>
        <artifactId>abacus-event-bus</artifactId>
        <version>1.1</version>
    </dependency>
    <dependency>
        <groupId>com.landawn.abacus</groupId>
        <artifactId>abacus-common</artifactId>
        <version>8.3.0</version>
    </dependency>
</dependencies>
```

### Gradle (Groovy DSL)

```groovy
repositories {
    mavenCentral()
}

dependencies {
    implementation 'com.landawn.abacus:abacus-event-bus:1.1'
    implementation 'com.landawn.abacus:abacus-common:8.3.0'
}
```

Use a Java 21+ compiler and runtime. `ThreadMode` comes from `com.landawn.abacus.util`; `EventBus`, `Subscribe`, and `Subscriber` come from `com.landawn.abacus.eventbus`.

## Quick start

Save this complete example as `OrderExample.java` in an application with the dependencies above:

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscribe;

public class OrderExample {
    public record OrderPlaced(long orderId) {}

    public static class OrderListener {
        @Subscribe
        public void onOrder(OrderPlaced event) {
            System.out.println("Order received: " + event.orderId());
        }
    }

    public static void main(String[] args) {
        EventBus bus = EventBus.create("orders");
        OrderListener listener = new OrderListener();
        bus.register(listener);
        try {
            bus.post(new OrderPlaced(42));
            // Prints "Order received: 42" before post() returns.
        } finally {
            bus.unregister(listener);
        }
    }
}
```

The method name is arbitrary. Its parameter declares the event type, and `@Subscribe` makes it discoverable. An unqualified `@Subscribe` uses the posting thread and receives events posted without an event ID.

An event is delivered to every matching handler. Posting with no matching subscribers is allowed; an ordinary `post()` does not retain the event for later subscribers. Payloads are passed by reference, without copying or serialization. Immutable event objects, such as records whose components are also immutable, simplify sharing across subscribers.

The shorter examples below are independent snippets intended for a method body, with imports shown at the top of each snippet.

## Creating and choosing a bus

| Factory or accessor | Behavior |
| --- | --- |
| `EventBus.getDefault()` | Returns the shared singleton with identifier `"default"`. |
| `EventBus.create()` | Creates an independent bus with a generated identifier. |
| `EventBus.create(String identifier)` | Creates an independent bus with the supplied identifier. |
| `EventBus.create(String identifier, Executor executor)` | Creates an independent bus using the supplied executor for executor-mode handlers. The executor must be non-null. |
| `bus.identifier()` | Returns the bus identifier. |

Registrations and sticky events belong to one bus. Two calls to `create("orders")` create separate buses even though their names match. A bus identifier is a label; an **event ID** is a routing value within that bus.

Use a dedicated bus when a component, subsystem, or test needs its own registrations and retained events. Use `getDefault()` when participants intentionally share a bus, and clean up their registrations when their lifecycle ends.

`EventBus` has no public constructor, `close()`, or `shutdown()` method. Manage listener and executor lifetimes explicitly.

## Defining subscribers

### Annotated methods

A subscriber object can expose multiple handlers with different event types, IDs, and delivery options:

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscribe;

class Notifications {
    @Subscribe
    public void onMessage(String message) {
        System.out.println("Message: " + message);
    }

    @Subscribe(eventId = "progress")
    public void onProgress(Integer percent) {
        System.out.println("Progress: " + percent);
    }
}

EventBus bus = EventBus.create();
Notifications listener = new Notifications();
bus.register(listener);
bus.post("Started");
bus.post("progress", 50);
bus.unregister(listener);
```

Subscriber methods must be public, non-static, and accept exactly one parameter. Use `void` methods; return values are not used. Primitive parameters are matched through their boxed types, so an `int` handler can receive an `Integer` event.

Non-public annotated instance methods are ignored. Annotated static methods are rejected even when non-public. A public annotated method with zero or multiple parameters also causes registration to fail. If discovery finds no usable handlers, registration fails.

### Lambdas and method references

`Subscriber<E>` declares one method: `void on(E event)`. Keep the subscriber instance so it can be unregistered later:

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscriber;

EventBus bus = EventBus.create();
Subscriber<String> listener = System.out::println;
bus.register(listener, "messages");
try {
    bus.post("messages", "Hello channel");  // Delivered.
    bus.post("other", "Different channel"); // Not delivered.
    bus.post("No channel");                 // Not delivered.
} finally {
    bus.unregister(listener);
}
```

**Lambdas and method references require a non-empty event ID.** Their generic payload type is generally erased to `Object` at runtime, so the bus cannot use `Subscriber<String>` alone to enforce a String-only channel. Post a consistent payload type to that ID; an incompatible payload can cause a cast failure inside the callback, which is logged.

A concrete or anonymous implementation can preserve its generic event type and register without an ID:

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscriber;

Subscriber<Integer> numbers = new Subscriber<Integer>() {
    @Override
    public void on(Integer value) {
        System.out.println(value);
    }
};

EventBus bus = EventBus.create();
bus.register(numbers);
bus.post(42);       // Delivered.
bus.post("text");   // Different type; not delivered.
bus.unregister(numbers);
```

An unannotated `Subscriber` whose resolved event type is `Object` also requires a non-empty event ID, even if it is a named class. Annotate its `on` method if you need an explicit catch-all subscriber without an ID.

The options `sticky`, `strictEventType`, `intervalMillis`, and `deduplicate` are available only through `@Subscribe`. A plain lambda cannot configure them. An implementation of `Subscriber<E>` can annotate its `on(E)` method to use them.

### Inheritance and generics

Discovery includes annotated methods declared in superclasses and interfaces. Generic event types are resolved through the concrete subscriber's class and interface hierarchy. For example, a subclass specializing a handler's `T` to `String` receives String events.

An overridden handler is registered once using the most-specific annotation declaration. An unannotated override can inherit subscription metadata from an annotated ancestor. Class declarations take precedence over interface declarations. Conflicting annotations on unrelated interfaces cause `IllegalArgumentException`; add an annotated override to resolve the conflict.

Unannotated overloads named `on` do not become handlers just because the class implements `Subscriber`. Only the interface callback and explicitly annotated methods are discovered.

## Event IDs and type matching

Delivery requires **both** a matching event ID and a matching runtime event type.

### Event ID rules

IDs are exact, case-sensitive strings. There is no wildcard or prefix matching. Null and empty IDs represent the unqualified channel when posting, querying, or removing sticky events.

| Effective handler ID | `post(event)` | `post("orders", event)` | `post("audit", event)` |
| --- | --- | --- | --- |
| No ID | Eligible | No | No |
| `"orders"` | No | Eligible | No |
| `"audit"` | No | No | Eligible |

“Eligible” still requires a compatible event type and acceptance by any delivery filters. An `Object` handler without an ID receives all unqualified event types; it does not receive every named channel.

`post("hello")` posts the String payload `"hello"` without an ID. To target a channel, supply two arguments: `post("messages", "hello")`.

### Runtime type rules

By default, a handler accepts an event when its parameter type is assignable from the event's runtime class. A `Number` handler receives `Integer` and `Double`; an interface handler receives compatible implementations.

With `strictEventType = true`, the parameter type must equal the runtime class. A strict `Number` handler does not receive `Integer` or `Double` objects. A strict interface handler cannot match an implementing object's concrete runtime class.

Parameterized payloads match on their raw class: `List<String>` and `List<Integer>` are not separate event types. Use distinct event classes or event IDs to distinguish them. Strict matching also uses the raw class, so a strict `List` handler will not match an `ArrayList` instance.

## Registration and listener lifecycle

Registration overrides apply to **every handler on that subscriber object**:

| Registration argument | Effective configuration |
| --- | --- |
| Non-empty `eventId` | Replaces each handler's annotated event ID. |
| Null or empty `eventId` | Preserves each handler's annotated ID, or no ID when none is declared. |
| Non-null `threadMode` | Replaces each handler's annotated delivery mode. |
| Null `threadMode` | Preserves each handler's annotation mode, defaulting to `DEFAULT`. |

Passing `""` at registration does **not** clear an annotated event ID. Calling `register(listener)` preserves annotated thread modes; it does not force synchronous delivery.

Subscribers are tracked by object identity (`==`). Distinct instances remain separate even if their `equals()` methods say they are equal. Re-registering the same instance replaces its previous registration, resets its delivery-filter history, and performs sticky replay again for sticky handlers. It does not add a second simultaneous registration for that instance.

To subscribe one component to multiple IDs, declare separate annotated methods or use separate subscriber objects. Repeatedly registering the same object under different IDs retains only the latest registration.

`unregister(listener)` removes all handlers for that exact object and is harmless when the object is not registered. Registrations strongly retain listeners, so unregister listeners when their owning component stops. For a lambda, retain the original lambda instance; creating a new equivalent lambda does not unregister the old one.

Unregistration prevents selection by future posts. A delivery already selected or queued can still run. Re-registration likewise does not cancel tasks created by the old registration.

## Threading and executors

Thread mode is selected per handler, through `@Subscribe` or a registration override. Providing an executor when creating a bus does not automatically make all handlers asynchronous.

| Mode | Callback execution | What the posting call waits for |
| --- | --- | --- |
| `ThreadMode.DEFAULT` | On the thread calling `post()` or `postSticky()`. Sticky replay runs on the registering thread. | The selected synchronous callbacks finish before the call returns. |
| `ThreadMode.THREAD_POOL_EXECUTOR` | Submitted to the bus's executor. | Submission, not callback completion, unless the executor itself executes inline or blocks. |

### Supplying an executor

This complete example owns its executor and waits for submitted callbacks when the try-with-resources block closes:

```java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscriber;
import com.landawn.abacus.util.ThreadMode;

public class BackgroundExample {
    public static void main(String[] args) {
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            EventBus bus = EventBus.create("background", executor);
            Subscriber<String> listener = System.out::println;
            bus.register(listener, "jobs", ThreadMode.THREAD_POOL_EXECUTOR);
            try {
                bus.post("jobs", "Process this on the worker");
            } finally {
                bus.unregister(listener);
            }
            // Unregistering does not cancel the task already submitted above.
        }
    }
}
```

An executor determines where and how callbacks run. A direct executor such as `Runnable::run` executes executor-mode callbacks on the posting thread. A single-thread executor serializes tasks submitted to it; with concurrent publishers, do not assume a deterministic submission order.

### Default executor and shutdown

On a standard JVM, buses created without an explicit executor share a pool with daemon workers and an unbounded queue. Queued work does not keep the JVM alive. Its shutdown hook allows up to 120 seconds for draining, but application correctness should not depend on completion during JVM exit. The Android platform branch uses the executor supplied by `AndroidUtil`.

A caller-supplied `ExecutorService` is tracked weakly by a shared JVM shutdown hook. At JVM exit, that hook calls `shutdown()` on tracked services and waits up to 60 seconds in total. A plain `Executor` is not shut down by the bus. For predictable application shutdown, stop publishers and close or shut down your owned executor explicitly; the bus has no flush or await-completion API.

For workloads that can outpace consumers, choose an executor with suitable queue capacity and rejection behavior. Executor rejection is logged by public posting methods; they do not return a delivery result or retry rejected work. A policy that silently discards tasks can lose events without informing the bus and can still advance throttling or deduplication history.

### Concurrency and ordering

The bus's registry and delivery filtering are thread-safe. This does not serialize subscriber callbacks. Multiple posting threads can enter the same synchronous handler concurrently, and a pool can run accepted callbacks concurrently. Protect mutable listener state as required by your application's threading model.

Cross-subscriber callback order is unspecified. Asynchronous completion order depends on the executor and callback duration. Sticky replay order is also unspecified. Callbacks can post events or change registrations; a synchronous nested post can invoke another callback before the outer callback returns.

## Sticky events

`postSticky()` both posts an event to current matching subscribers and retains it in memory for future replay. Current subscribers do not need `sticky = true` to receive that live post. The flag controls replay when a subscriber registers.

```java
import java.util.List;

import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscribe;

record Status(String state) {}
class StatusListener {
    @Subscribe(eventId = "status", sticky = true)
    public void onStatus(Status status) {
        System.out.println(status.state());
    }
}

EventBus bus = EventBus.create();
Status ready = new Status("ready");
bus.postSticky("status", ready);

StatusListener listener = new StatusListener();
bus.register(listener); // Replays ready on this thread and prints "ready".
List<Status> retained = bus.stickyEvents("status", Status.class);
System.out.println(retained.size()); // 1

boolean removed = bus.removeStickyEvent("status", ready); // true
bus.unregister(listener);
```

### Retention and replay rules

- Retention uses **event object identity**, not `equals()` or event class. Multiple distinct objects of the same class and ID are retained, including objects that compare equal.
- Posting the same object again updates its retained ID and performs another live post. That object has only one retained entry, so reposting it under another ID moves its retained channel.
- Every retained event matching a sticky handler's ID and type is considered for replay. Replay uses that handler's thread mode, throttling, and deduplication settings.
- Replay order is unspecified. With throttling enabled, an initial replay can suppress other retained events considered immediately afterward.
- Registration and a concurrent sticky post coordinate their delivery decisions to avoid duplicate delivery from both live posting and replay for that registration/post race. This is not a retry or durable-delivery guarantee.
- Events remain retained until explicitly removed. There is no automatic expiration or latest-value-per-channel replacement. Unregistering a subscriber does not remove sticky events.

Retention stores the original object reference. Mutating that object changes what future subscribers observe. Use immutable payloads and remove obsolete retained events to control memory use.

### Inspecting and removing retained events

| Operation | Scope |
| --- | --- |
| `stickyEvents(Type.class)` | Snapshot of compatible retained events without an ID. |
| `stickyEvents("status", Type.class)` | Snapshot of compatible retained events on `"status"`. |
| `removeStickyEvent(event)` | Removes that exact retained object only if it has no ID. |
| `removeStickyEvent("status", event)` | Removes that exact object only if its retained ID is `"status"`. |
| `removeStickyEvents(Type.class)` | Removes compatible retained events without an ID. |
| `removeStickyEvents("status", Type.class)` | Removes compatible retained events on `"status"`. |
| `removeAllStickyEvents()` | Clears all retained events across all IDs and types. |

Type-based queries and removals include subtypes. Null and empty IDs select the unqualified channel; they do not mean all channels. `stickyEvents(Object.class)` therefore lists only unqualified events, while `stickyEvents("status", Object.class)` lists all types on `"status"`.

All removal methods return `true` if at least one entry was removed, otherwise `false`. Removing by object requires the original instance, not a newly created equal value. Removal affects future replay; it does not cancel callbacks already selected or submitted.

For a single retained configuration per channel, remove prior matching events before posting the replacement. Those two public calls are separate operations; applications needing atomic replacement must coordinate their publishers and readers.

## Throttling and deduplication

These options are configured on individual annotated handlers. Their state belongs to each handler registration, rather than being shared by all listeners on a bus.

### Interval throttling

`@Subscribe(intervalMillis = 1000)` accepts at most one delivery attempt per 1,000-millisecond interval for that handler. Values at or below zero disable throttling.

The interval is measured when the posting thread considers a delivery, using a monotonic clock. It is not measured from callback completion or from the time a queued task begins running. Events within the interval are dropped; they are not delayed, coalesced, or delivered at the end of the interval. A callback that takes longer than the interval can overlap a later accepted callback.

### Consecutive-event deduplication

`@Subscribe(deduplicate = true)` compares the incoming event with the most recently accepted event using `equals()`. For `A, A, B, B, A`, the handler receives `A, B, A` when no other filter suppresses those events. This is consecutive deduplication, not a set of every previously seen value.

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscribe;

class ChangeListener {
    @Subscribe(deduplicate = true)
    public void onChange(String state) {
        System.out.println(state);
    }
}

EventBus bus = EventBus.create();
ChangeListener listener = new ChangeListener();
bus.register(listener);
bus.post("A").post("A").post("B").post("B").post("A");
// Prints A, B, A on separate lines.
bus.unregister(listener);
```

Use immutable events with meaningful equality when deduplicating. The previous event is retained by reference, so mutating it can change later equality comparisons.

### Combining filters and handling failed attempts

Both filters can be enabled on the same method. An event must pass both. Suppressed events do not become the previous accepted event or restart the interval. For executor delivery, decisions happen before submission, so suppressed events never enter the executor queue.

A callback attempt counts even if the callback throws. If the executor rejects a submission before the callback begins, that submission's filter reservation is released so it does not suppress later posts. If an inline executor runs the callback and then throws, the attempt still counts.

Filtering decisions are atomic under concurrency, but accepted callbacks can overlap and complete out of order. Deduplication follows accepted posting decisions, not the order in which an executor finishes callbacks.

## Inspecting a bus

| Query | Meaning |
| --- | --- |
| `subscribers(eventType)` | Subscriber instances with at least one handler matching that event type on the unqualified channel. |
| `subscribers(eventId, eventType)` | Subscriber instances with at least one handler matching both that ID and type. |
| `allSubscribers()` | All registered subscriber instances. |
| `countOfSubscribers()` | Number of registered instances, not number of handler methods. |
| `hasSubscribers(eventType)` | Whether any handler accepts that event type, **ignoring event IDs**. |

For example, `subscribers("orders", Integer.class)` includes a non-strict `Number` handler on `"orders"`, but excludes a strict `Number` handler. Each subscriber object appears at most once in a query result even if several of its methods match.

Query lists are snapshots with unspecified order. Changing a returned list does not change the bus, and later registry changes do not update an earlier list. The objects inside the lists remain the original instances.

These queries inspect routing eligibility, not whether throttling, deduplication, or executor availability will allow a particular delivery. They are also snapshots under concurrency. To check a channel, use `subscribers(eventId, eventType)`; `hasSubscribers(eventType)` alone does not establish that a post to that channel has a recipient.

## API reference

### `EventBus`

All signatures below are public. Factories are static; the remaining methods operate on a bus instance. `register`, `unregister`, `post`, and `postSticky` return the same bus for chaining. A fluent return value does not report delivery success.

| Signature | Return type | Purpose |
| --- | --- | --- |
| `getDefault()` | `EventBus` | Shared default bus. |
| `create()` | `EventBus` | New bus with generated identifier. |
| `create(String identifier)` | `EventBus` | New named bus. |
| `create(String identifier, Executor executor)` | `EventBus` | New named bus with explicit executor. |
| `identifier()` | `String` | Bus identifier. |
| `register(Object subscriber)` | `EventBus` | Discover handlers and preserve their annotation settings. |
| `register(Object subscriber, String eventId)` | `EventBus` | Register with an optional ID override. |
| `register(Object subscriber, ThreadMode threadMode)` | `EventBus` | Register with an optional delivery-mode override. |
| `register(Object subscriber, String eventId, ThreadMode threadMode)` | `EventBus` | Register with both optional overrides. |
| `register(Subscriber<?> subscriber, String eventId)` | `EventBus` | Functional-interface registration with an ID. |
| `register(Subscriber<?> subscriber, String eventId, ThreadMode threadMode)` | `EventBus` | Functional-interface registration with an ID and mode. |
| `unregister(Object subscriber)` | `EventBus` | Remove every handler for that subscriber instance. |
| `post(Object event)` | `EventBus` | Deliver on the unqualified channel. |
| `post(String eventId, Object event)` | `EventBus` | Deliver on the specified channel. |
| `postSticky(Object event)` | `EventBus` | Retain and deliver on the unqualified channel. |
| `postSticky(String eventId, Object event)` | `EventBus` | Retain and deliver on the specified channel. |
| `subscribers(Class<?> eventType)` | `List<Object>` | Matching unqualified subscriber instances. |
| `subscribers(String eventId, Class<?> eventType)` | `List<Object>` | Matching subscriber instances on one channel. |
| `allSubscribers()` | `List<Object>` | All registered instances. |
| `countOfSubscribers()` | `int` | Count registered instances. |
| `hasSubscribers(Class<?> eventType)` | `boolean` | Check type compatibility across all channels. |
| `<T> stickyEvents(Class<T> eventType)` | `List<T>` | Compatible retained events without an ID. |
| `<T> stickyEvents(String eventId, Class<T> eventType)` | `List<T>` | Compatible retained events on one channel. |
| `removeStickyEvent(Object event)` | `boolean` | Remove an unqualified retained object by identity. |
| `removeStickyEvent(String eventId, Object event)` | `boolean` | Remove a retained object by identity and ID. |
| `removeStickyEvents(Class<?> eventType)` | `boolean` | Remove compatible unqualified retained events. |
| `removeStickyEvents(String eventId, Class<?> eventType)` | `boolean` | Remove compatible retained events on one channel. |
| `removeAllStickyEvents()` | `boolean` | Clear retained events on every channel. |

Because the Object overloads accept either `String` or `ThreadMode` as their second argument, `register(object, null)` can be ambiguous. Use `register(object)` to preserve defaults, or the three-argument overload to express optional overrides explicitly.

### `@Subscribe`

The annotation is retained at runtime and targets methods.

| Attribute | Type | Default | Effect |
| --- | --- | --- | --- |
| `eventId` | `String` | `""` | Unqualified channel, unless a non-empty ID is declared or supplied at registration. |
| `threadMode` | `ThreadMode` | `ThreadMode.DEFAULT` | Synchronous delivery; `THREAD_POOL_EXECUTOR` delegates to the bus's executor. |
| `strictEventType` | `boolean` | `false` | Require exact runtime class when true; otherwise accept compatible subtypes. |
| `sticky` | `boolean` | `false` | Replay retained matching events at registration. |
| `intervalMillis` | `long` | `0` | Minimum interval between accepted attempts; values <= 0 disable throttling. |
| `deduplicate` | `boolean` | `false` | Suppress events equal to the last accepted event. |

### `Subscriber<E>` and `ThreadMode`

`Subscriber<E>` is a functional interface with the single abstract method `void on(E event)`. The bus passes non-null events to it. Calling `on()` directly is an ordinary Java call and bypasses routing, filtering, executor selection, and exception handling supplied by the bus.

The supported delivery modes are `ThreadMode.DEFAULT` and `ThreadMode.THREAD_POOL_EXECUTOR`. A null registration override preserves annotation defaults; it is not a third delivery mode.

## Failures and troubleshooting

### Validation and delivery failures

| Condition | Behavior |
| --- | --- |
| Null event passed to `post` or `postSticky` | `IllegalArgumentException`. |
| Null subscriber passed to `register` | `IllegalArgumentException`. |
| Null executor passed to `create(identifier, executor)` | `IllegalArgumentException`. |
| Null event type passed to a subscriber query, sticky query, or type-based sticky removal | `IllegalArgumentException`. |
| No usable subscriber method, conflicting inherited annotations, or unsupported registration thread mode | `IllegalArgumentException`. |
| Annotated static method or public annotated method with invalid parameter count | `RuntimeException` during registration. |
| Lambda or other unannotated Object-typed `Subscriber` registered without a non-empty ID | `IllegalStateException`. |
| Subscriber callback throws | Caught and logged; other matching handlers can still receive the event. |
| Executor submission throws an exception such as `RejectedExecutionException` | Public posting and sticky replay log it and continue to other deliveries. |
| Executor submission throws an `Error` | The error propagates; posting is not an all-throwable isolation boundary. |
| No matching subscriber | Ordinary posting completes without retaining or retrying the event. |

Delivery failures are reported through the logging facade in abacus-common. `post()` returns the bus, not a `Future`, acknowledgement, or subscriber result. There is no built-in retry, persistence, transaction, or cross-process transport. If a publisher needs an explicit processing result, use an application-level completion mechanism or a direct service call.

### Common symptoms

| Symptom | What to check |
| --- | --- |
| Handler is never called | Same bus instance, registered listener, public one-argument instance method, matching ID, and compatible runtime type. |
| Named posts miss an unqualified listener | IDs must match; a listener without an ID is not a wildcard. |
| Registration reports that a lambda needs an ID | Supply a non-empty ID, or use a concrete typed/annotated subscriber. |
| Lambda callback logs `ClassCastException` | Keep a consistent payload type on its ID; lambda generic types may be erased at runtime. |
| A second registration stops the first channel | Re-registering the same object replaces its registration. Use separate objects or separately annotated handlers. |
| Handler still runs after `unregister` | It may already have been selected or queued. Coordinate executor completion if teardown requires waiting. |
| Executor was supplied but handler runs synchronously | Set `THREAD_POOL_EXECUTOR` on the handler or registration. |
| Executor-mode callback runs on the caller | Inspect the executor; an inline executor or caller-runs rejection policy can do this. |
| Some rapid or repeated posts disappear | Check `intervalMillis`, `deduplicate`, and executor rejection or discard policy. |
| Sticky replay returns several old values | Retention is by object identity; remove obsolete entries explicitly. |
| Removing an equal sticky value returns false | Pass the original object instance and its currently retained ID. |
| `hasSubscribers()` is true but posting finds no recipient | It ignores IDs. Inspect `subscribers(eventId, eventType)`. |
| `NoClassDefFoundError` for abacus utility classes | Include abacus-common on the runtime classpath. |

## Building and testing

From the repository root with Java 21+ and Maven available:

```shell
mvn clean test
mvn clean test jacoco:report
mvn -Dtest=EventBusRoutingTest test
mvn -Dtest=AbacusEventBusTestSuite test
```

These are separate commands: run the one appropriate to your task. The first runs normal test discovery, the second also generates coverage, the third selects one test class, and the fourth uses the explicit suite. Do not select the suite together with all individual test classes in the same invocation.

Test reports are written to `target/surefire-reports`; the coverage report is `target/site/jacoco/index.html`. The POM supplies JVM module options used by lifecycle tests. See the [test guide](src/test/README.md) for scenario ownership and concurrency-testing conventions.

To build the JAR, sources, and Javadoc locally, run `mvn package`. To run verification without signing release artifacts, use `mvn verify -Dgpg.skip=true`; the POM otherwise binds GPG signing to the `verify` phase.

## Further documentation

- [Published API Javadoc](https://www.javadoc.io/doc/com.landawn.abacus/abacus-event-bus)
- [EventBus source and API contracts](src/main/java/com/landawn/abacus/eventbus/EventBus.java)
- [Subscribe annotation](src/main/java/com/landawn/abacus/eventbus/Subscribe.java)
- [Subscriber interface](src/main/java/com/landawn/abacus/eventbus/Subscriber.java)
- [Package overview](src/main/java/com/landawn/abacus/eventbus/package-info.java)
- [Test guide](src/test/README.md)
- [abacus-common](https://github.com/landawn/abacus-common): utility library and runtime dependency.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
