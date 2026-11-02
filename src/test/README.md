# Unit tests

Run the full suite with `mvn clean test`. Generate coverage with
`mvn clean test jacoco:report`; the report is in `target/site/jacoco/index.html`.
Run the explicit suite with `mvn -Dtest=AbacusEventBusTestSuite test`.
Maven supplies the JVM module options needed by the shutdown-hook tests.
The cleanup plan and scenario consolidation record are in [TEST_CLEANUP_PLAN.md](TEST_CLEANUP_PLAN.md).
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
| `EventBusAsyncDispatchTest` | Public asynchronous posting, filtering before execution, sticky delivery, unregistration, and failure recovery |
| `EventBusReservationTest` | Protected dispatch, reservation commit/release, failure history, throwable identity, and deterministic completion races |
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
  Put ordinary cases before boundaries and failures, fixtures after tests, and helpers/providers last.
  Source order is for readers: tests must not depend on execution order or `@Order`.
- Keep subscriber fixtures local to their owning class. Use a fresh bus for each
  test; clean up default-singleton registrations in `finally`.
- Use parameterized tests for equivalent cases that differ only in data, such as
  null/empty IDs or lambda/annotated subscriber forms. Use named enum/method sources
  for overload and callback matrices so every reported case identifies its variant. Preserve distinct regression
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

Use `TestBase.runConcurrently` for start-barrier races, `awaitBlocked` to coordinate
monitor interleavings, and `shutdownAndAwaitTermination` for test-owned executors. Interval expiry checks
wait only for a minimum interval; exact boundaries use synthetic timestamps.
GC regression tests use a bounded collection wait and require a JVM permitting
explicit GC.

Tests that change the EventBus logger share the `eventbus-logger` resource lock
and restore its configuration. Executor-service lifecycle tests live in
`EventBusExecutorContractTest` and share the `eventbus-shutdown-hooks` lock.
Shutdown checks invoke the hook body while executors are strongly reachable and
verify failure isolation and interrupt preservation. Hook-count assertions explicitly
initialize the shared EventBus runtime before measuring per-factory additions, so
they also work when selected alone in a fresh JVM. A separate, bounded child JVM
initializes EventBus inside a real shutdown hook to check its fallback executor.
The child initializes the common utility dependency before shutdown, inherits the
JaCoCo agent when present, and explicitly dumps coverage after the hook assertions.
This keeps shutdown coverage independent of GC timing and agent-hook ordering.

## Coverage

JaCoCo 0.8.15, from `mvn clean test jacoco:report`. The 2026-11-01 run on Java 25
(255 passing test invocations, abacus-common 8.3.2) is the current total. A JDK 21
child covers the security-manager thread-group branches, because JDK 24 and later
cannot install a security manager. No production code was deleted and no packages
were excluded to raise the percentage.

| Measure | Covered / total | Coverage |
| --- | --- | --- |
| Lines | 609 / 614 | 99.2% |
| Instructions | 2,824 / 2,849 | 99.1% |
| Branches | 373 / 387 | 96.4% |
| Methods | 78 / 78 | 100% |
| Classes | 4 / 4 | 100% |

Line coverage is the figure tracked here. The five missed lines are all in `EventBus.java`
and are not reachable from a legal subscriber or the current `ThreadMode` enum:

| Lines | Reason |
| --- | --- |
| 864, 1769 | `ThreadMode` defines only `DEFAULT` and `THREAD_POOL_EXECUTOR`. Both are supported, so the unsupported-mode throw and the dispatch `default` arm never run. |
| 1053–1054 | `Subscriber.on(Object)` exists. The `NoSuchMethodException` catch is a safeguard for a missing contract method. |
| 1150 | The event-type fallback handles a reflective type that is not a class, parameterized type, generic array, type variable, or wildcard. Cyclic bounds such as `T extends T` are rejected by javac, so a subscriber declaration cannot supply one. |

Partial branch gaps that do not remove a whole line — a null executor passed to the private constructor, an event-id index entry that registration never omits, the null diagnostic fallback, and some `SubIdentifier.equals` field combinations — stay uncovered for the same reason: the public API does not produce those states. Do not manufacture an extra `ThreadMode` constant or a fake reflective type to move the number.

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
| Repeated registration bodies | Named cases for all four annotated overloads and six callback-form/mode combinations |
| Separate successful removal and idempotency tests | Annotated/lambda cases assert delivery stops, both removals return the bus, and both registry views are empty |
| Annotated/lambda broadcast and failure-isolation bodies | One parameterized contract per behavior, preserving both subscriber forms |
| Overlapping sticky removals | Multi-event, wrong-ID, repeated-removal, cross-channel, and cross-type assertions in their retained owner |
| Query snapshot/empty checks | All, unqualified, and named subscriber queries; both sticky-query overloads also verify snapshot independence |
| Reservation cases embedded in asynchronous API tests | Dedicated EventBusReservationTest; all nine internal regression methods preserved |
| Custom cross-post workers and duplicate monitor waits | Shared bounded TestBase helpers with failure propagation |

When removing another duplicate, identify the retained scenario and preserve every
unique assertion or parameter case. Shared setup alone does not make tests duplicates.
