# abacus-event-bus

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn.abacus/abacus-event-bus.svg)](https://central.sonatype.com/artifact/com.landawn.abacus/abacus-event-bus/1.0)
[![Javadocs](https://img.shields.io/badge/javadoc-1.0-brightgreen.svg)](https://www.javadoc.io/doc/com.landawn.abacus/abacus-event-bus/1.0/index.html)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A lightweight, thread-safe Java event bus for loosely coupled components: **typed subscribers**,
**synchronous and executor-based delivery**, **event IDs**, **sticky events**, **throttling**, and
**consecutive-event deduplication**.

The library builds on [abacus-common](https://github.com/landawn/abacus-common) and targets **Java 21+**.

## What's inside

| Type | Purpose |
| --- | --- |
| `EventBus` | Register subscribers, post events, inspect subscriptions, and retain or remove sticky events. Create independent buses or use the process-wide default bus. |
| `Subscribe` | Mark public instance methods as subscribers and configure routing, thread mode, sticky replay, type matching, throttling, and deduplication. |
| `Subscriber<E>` | Functional interface for subscribers implemented with lambdas, method references, or classes. |
| `ThreadMode` | Select delivery on the posting thread or through an executor. Provided by `com.landawn.abacus.util` in abacus-common. |

## Requirements

* Java 21 or above.
* [abacus-common](https://github.com/landawn/abacus-common) on the classpath. It is a `provided`
  dependency, so add it explicitly to your application as shown below.

## Installation

**Maven**

```xml
<dependency>
    <groupId>com.landawn.abacus</groupId>
    <artifactId>abacus-event-bus</artifactId>
    <version>1.0</version>
</dependency>

<!-- Required at runtime; abacus-event-bus does not pull it in transitively -->
<dependency>
    <groupId>com.landawn.abacus</groupId>
    <artifactId>abacus-common</artifactId>
    <version>8.0.1</version>
</dependency>
```

**Gradle**

```gradle
implementation 'com.landawn.abacus:abacus-event-bus:1.0'
implementation 'com.landawn.abacus:abacus-common:8.0.1'
```

## Quick start

**Annotated subscribers** receive matching events on the posting thread by default:

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscribe;

public class Example {
    public static class Listener {
        @Subscribe
        public void onMessage(String message) {
            System.out.println(message);
        }
    }

    public static void main(String[] args) {
        EventBus bus = EventBus.create();
        Listener listener = new Listener();
        bus.register(listener);
        try {
            bus.post("Hello"); // Prints Hello before post() returns.
        } finally {
            bus.unregister(listener);
        }
    }
}
```

Use `EventBus.create("orders")` to name an independent bus, or `EventBus.getDefault()` for the shared
process-wide bus. A bus name identifies the bus; event IDs route messages within it.

**Lambdas and event IDs** make it easy to subscribe to a particular channel. Lambda and method-reference
subscribers require a non-empty event ID:

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscriber;

EventBus bus = EventBus.create();
Subscriber<String> listener = System.out::println;
bus.register(listener, "messages");
bus.post("messages", "Hello channel"); // Delivered.
bus.post("other", "Ignored");         // Different event ID.
bus.post("Ignored");                  // No event ID.
bus.unregister(listener);
```

**Sticky events** are retained for subscribers that opt into replay. This snippet prints `ready`
when the listener registers:

```java
import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscribe;

class StatusListener {
    @Subscribe(eventId = "status", sticky = true)
    public void onStatus(String status) {
        System.out.println(status);
    }
}

EventBus bus = EventBus.create();
bus.postSticky("status", "ready");
StatusListener listener = new StatusListener();
bus.register(listener);
bus.removeStickyEvents("status", String.class);
bus.unregister(listener);
```

Sticky events remain retained until removed. Multiple matching events may be replayed, and their
replay order is unspecified. Use `stickyEvents(...)` to inspect them or `removeAllStickyEvents()` to
clear the bus's retained events.

**Executor-based delivery** uses `ThreadMode.THREAD_POOL_EXECUTOR`. Supply an executor when you need
control over its threading and lifetime:

```java
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.landawn.abacus.eventbus.EventBus;
import com.landawn.abacus.eventbus.Subscriber;
import com.landawn.abacus.util.ThreadMode;

try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
    EventBus bus = EventBus.create("background", executor);
    Subscriber<String> listener = System.out::println;
    bus.register(listener, "jobs", ThreadMode.THREAD_POOL_EXECUTOR);
    bus.post("jobs", "Process this in the executor");
} // Closing the executor waits for submitted tasks to finish.
```

Without a custom executor, executor-based delivery uses the shared pool with daemon workers.
Queued work does not keep the JVM alive. A custom executor determines where callbacks run;
an inline executor such as `Runnable::run` executes them on the posting thread.

## Subscriber options

Annotated methods must be public, non-static, and accept exactly one event parameter.

| `@Subscribe` attribute | Default | Behavior |
| --- | --- | --- |
| `eventId` | `""` | Match events posted without an ID, or select a specific ID. |
| `threadMode` | `ThreadMode.DEFAULT` | Invoke on the posting thread; use `THREAD_POOL_EXECUTOR` to submit to the bus's executor. |
| `strictEventType` | `false` | Accept compatible subtypes; set to `true` to require the exact event class. |
| `sticky` | `false` | Replay matching retained events when the subscriber registers, using its configured thread mode. |
| `intervalMillis` | `0` | Suppress events within a positive minimum interval between accepted delivery attempts. Values at or below zero disable throttling. |
| `deduplicate` | `false` | Suppress consecutive events equal to the last accepted event, using `equals()`. |

Registration overloads can override the event ID and thread mode. Sticky replay, strict type matching,
throttling, and deduplication require `@Subscribe`; they cannot be configured on a plain lambda.

Type matching uses runtime classes, so parameterized types such as `List<String>` and `List<Integer>`
are not separate channels. Use distinct event IDs or event classes when you need that distinction.
Null events are rejected; null and empty event IDs both mean no ID when posting.

Throttling and deduplication are decided at post time, before executor submission. Subscriber
exceptions are caught and logged so other subscribers can still receive the event. Accepted callbacks
may overlap when posts arrive concurrently or an executor runs multiple tasks, so shared subscriber
state must support the concurrency your application uses. Unregister listeners when they are no longer needed.

## Build and test

Build from this directory with Maven and JDK 21 or newer:

```sh
mvn clean package
```

Run unit tests or perform local verification without release signing:

```sh
mvn test
mvn verify -Dgpg.skip=true
```

See the [unit test guide](src/test/README.md) for test organization and focused suite execution.

## Documentation

* [API Javadoc](https://www.javadoc.io/doc/com.landawn.abacus/abacus-event-bus/1.0/index.html)
* [EventBus source and API contracts](src/main/java/com/landawn/abacus/eventbus/EventBus.java)
* [Subscriber annotation options](src/main/java/com/landawn/abacus/eventbus/Subscribe.java)

## Related projects

* [abacus-common](https://github.com/landawn/abacus-common): the core utility library and runtime dependency.
* [abacus-extra](https://github.com/landawn/abacus-extra): additional tuples, point/value types, and array utilities.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
