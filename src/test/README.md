# Unit tests

Run the full suite with `mvn clean test`. Generate coverage with
`mvn clean test jacoco:report`; the report is in `target/site/jacoco/index.html`.
Run the explicit suite with `mvn -Dtest=AbacusEventBusTestSuite test`.
Maven supplies the JVM module options needed by the shutdown-hook tests.
The explicit suite is an alternative entry point; do not select it together with
all individual test classes in the same invocation.

## Ownership

Each behavior has one primary owner. Different overloads, subscriber forms,
thread modes, and failure interleavings remain separate cases where they exercise
different contracts.

| Class | Scope |
| --- | --- |
| `EventBusTest` | Creation, identifiers, singleton, subscriber queries, counts, and snapshots |
| `EventBusRegistrationTest` | Registration overloads, validation, ID overrides, identity, replacement, and removal |
| `EventBusRoutingTest` | Synchronous delivery, event IDs, type hierarchy, strict matching, payload identity, and subscriber failure isolation |
| `EventBusFilteringTest` | Synchronous deduplication and interval behavior |
| `EventBusStickyTest` | Sticky replay, event identity, retained channels, queries, and removal |
| `EventBusAsyncDispatchTest` | Queued dispatch, filtering before execution, queued delivery after unregistration, executor failures, and rollback |
| `EventBusExecutorContractTest` | Executor selection, thread modes, worker properties, shutdown hooks, and executor lifetime |
| `EventBusConcurrencyTest` | Concurrent registration, sticky delivery, reentrancy, deduplication, and deadlock regressions |
| `EventBusLookupCacheTest` | Cache invalidation, event-ID retention, and concurrent delivery |
| `EventBusGenericTest` | Subscriber discovery, inherited annotations, generic resolution, and overloads |
| `EventBusClassLoaderTest` | Subscriber and event class-loader retention |
| `EventBusSubIdentifierTest` | Metadata identity and synthetic interval boundaries |
| `EventBusDiagnosticsTest` | Logging failures, diagnostic formatting, and failure isolation |
| `SubscribeTest` | Annotation defaults, configured values, retention, and target |
| `SubscriberTest` | Generic callback contract and direct invocation through supported implementation forms |

## Organization and assertions

- Name tests `operation_whenCondition_expectedResult`. Keep test and lifecycle
  methods package-private; subscriber fixture methods stay public because discovery
  depends on their visibility. Class-loader fixtures also retain public classes and
  constructors.
- Group source by API operation or behavior, keeping related overloads together.
  Put ordinary cases before boundaries and failures, and helpers/fixtures after tests.
  Source order is for readers: tests must not depend on execution order or `@Order`.
- Keep subscriber fixtures local to their owning class. Use a fresh bus for each
  test; clean up default-singleton registrations in `finally`.
- Use parameterized tests for equivalent cases that differ only in data, such as
  null/empty IDs or lambda/annotated subscriber forms. Preserve distinct regression
  interleavings and distinct overloads instead of merging their assertions away.
- Assert complete delivered values, identity where contractual, and exact counts.
  Sticky replay and subscriber-query ordering are unspecified; compare contents
  without imposing an order. Do not infer cross-subscriber callback ordering from
  the current registry implementation.
- Direct `Subscriber.on(null)` and `EventBus.post(null)` have different contracts:
  the direct callback may accept null, while the bus rejects null events.

## Threading and lifecycle

Use queued executors to inspect accepted work before running callbacks. Reserve
real workers for threading, shutdown, and concurrency contracts. Worker failures
must reach the JUnit thread; waits and joins must be bounded. A subscriber callback
can swallow assertion failures through EventBus's exception handling, so record
observations and assert them on the test thread.

Use `TestBase.runConcurrently` for start-barrier races and
`shutdownAndAwaitTermination` for test-owned executors. Interval expiry checks
wait only for a minimum interval; exact boundaries use synthetic timestamps.
GC regression tests use a bounded collection wait and require a JVM permitting
explicit GC.

Tests that change the EventBus logger share the `eventbus-logger` resource lock
and restore its configuration. Executor-service lifecycle tests live in
`EventBusExecutorContractTest` and share the `eventbus-shutdown-hooks` lock.
Shutdown checks invoke the hook body while executors are strongly reachable and
verify failure isolation and interrupt preservation. A separate, bounded child JVM
initializes EventBus inside a real shutdown hook to check its fallback executor.
The child initializes the common utility dependency before shutdown, inherits the
JaCoCo agent when present, and explicitly dumps coverage after the hook assertions.
This keeps shutdown coverage independent of GC timing and agent-hook ordering.

## Coverage

The 2026-10-15 clean coverage run contains 220 passing test invocations with
abacus-common 8.2.0. Standard discovery passes on Java 25; the explicit suite also
passes on Java 21 with randomized class/method order (seed 15102026), selecting the
same 220 cases without duplicates.

| Measure | Covered / total | Coverage |
| --- | --- | --- |
| Lines | 552 / 559 | 98.7% |
| Branches | 351 / 367 | 95.6% |
| Methods | 71 / 71 | 100% |
| Classes | 4 / 4 | 100% |

These are JaCoCo report totals, including defensive and platform-specific code.
No production code or coverage exclusions were changed to raise the percentage.
The remaining gaps are in `EventBus.java`:

| Area | Remaining limitation |
| --- | --- |
| Android default executor | Requires an Android runtime; the desktop JVM takes the other platform branch. |
| Unsupported thread modes | The dependency currently defines only the two supported enum values. |
| Discovery safeguards | Missing `Subscriber.on(Object)`, cyclic bounds, and unknown reflective types cannot be supplied by ordinary Java subscriber declarations. |
| Private constructor fallback | Public factories reject a null executor before reaching the constructor's null fallback. |
| Registry/metadata safeguards | Normal registration preserves the event-ID index and derives immutable annotation flags from the same method. |
| Diagnostic null fallback | Formatting null does not throw, so the catch block cannot see null. |
| Interval recheck after equality | Reservation changes cause a retry before this check; its suppression path cannot be forced deterministically through the current monotonic-clock API. |

Real shutdown fallback, shutdown failure/interruption, wildcard owners, identical
diamond annotations, mixed sticky handlers, throwing equality, diagnostic failures,
and commit/rejection races are exercised by focused regression tests. Keep future
coverage improvements tied to observable behavior instead of manufacturing invalid
reflection states or removing defensive branches for the metric.

## Consolidation guide

| Former overlap | Retained coverage |
| --- | --- |
| Attribute-by-attribute reflection checks in `SubscribeTest` | One defaults case and one configured-values case, plus retention and target checks |
| Interface-shape and direct-callback checks in `SubscriberTest` | One generic-interface contract and parameterized lambda, method-reference, and anonymous-class invocation |
| Deduplication repeated in `SubscribeTest` and `EventBusTest` | Exact A/A/B/B/A delivery sequence in `EventBusFilteringTest` |
| Basic sticky replay repeated in annotation and sticky tests | Exactly-once replay plus fluent return in `EventBusStickyTest`; registration-first delivery remains separate |
| Lambda event-ID validation in three classes | Missing/null/empty IDs and successful named registration in `EventBusRegistrationTest` |
| Async thread checks across four classes | Annotated/lambda default workers, custom executor selection, inline execution, and mode overrides in `EventBusExecutorContractTest` |
| Null type validation spanning subscriber and sticky APIs | Empty/populated cases in each query owner's class |

When removing another duplicate, identify the retained scenario and preserve every
unique assertion or parameter case. Shared setup alone does not make tests duplicates.
