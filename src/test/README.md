# Unit tests

Run all unit tests with `mvn clean test`. Run the explicit JUnit suite with
`mvn -Dtest=AbacusEventBusTestSuite test`. Maven supplies the JVM module options
needed by the shutdown-hook regression tests.

Tests are grouped by behavior:

| Class | Scope |
| --- | --- |
| `EventBusTest` | Creation, registration, queries, routing, and synchronous delivery |
| `EventBusStickyTest` | Sticky replay, queries, and removal |
| `EventBusAsyncDispatchTest` | Queued dispatch, filtering reservations, executor failures, and rollback |
| `EventBusExecutorContractTest` | Executor threading, shutdown hooks, and executor lifetime |
| `EventBusConcurrencyTest` | Concurrent registration, reentrancy, deduplication, and deadlock regressions |
| `EventBusLookupCacheTest` | Cache invalidation, event-ID retention, and concurrent delivery |
| `EventBusGenericTest` | Generic type resolution, inherited annotations, and overloads |
| `EventBusClassLoaderTest` | Subscriber and event class-loader retention |
| `EventBusSubIdentifierTest` | Metadata identity and interval boundaries |
| `EventBusDiagnosticsTest` | Logging failures and subscriber failure isolation |
| `SubscribeTest` | Annotation defaults, attributes, and method requirements |
| `SubscriberTest` | Subscriber interface contracts and integration |

All unit test classes extend `TestBase` and carry the `unit` tag. Subscriber
fixture methods stay public because discovery depends on their visibility.
Keep fixtures local to their test class and use a fresh bus unless testing the
default singleton; always clean up singleton registrations in `finally`.

Use a queued executor for dispatch/filtering assertions and latches for real
threading checks. Worker failures must reach the JUnit thread, and waits must
be bounded. Interval expiry tests wait only for a minimum elapsed interval;
exact boundaries use synthetic timestamps. GC regression tests request
collection with a bounded wait and require a JVM that permits explicit GC.

Tests that change the EventBus logger share the `eventbus-logger` resource lock
and restore its configuration after each check.
