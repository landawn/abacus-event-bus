# Unit-test cleanup plan

Completed after establishing a clean baseline and implementing each step.

## Baseline and result

| Measure | Before | After |
| --- | --- | --- |
| Test methods | 180 | 163 |
| Executed test cases | 220 | 222 |
| Line coverage | 552 / 559 (98.7%) | 552 / 559 (98.7%) |
| Branch coverage | 351 / 367 (95.6%) | 351 / 367 (95.6%) |
| Method coverage | 71 / 71 (100%) | 71 / 71 (100%) |

## Implemented steps

- [x] Capture the original test inventory and coverage baseline. Account for every original test method through retention, movement, or an identified consolidated replacement.
- [x] Consolidate repeated registration overloads, callback forms, visibility/arity validation, ID overrides, and removal/idempotency checks. Keep all four annotated overloads and expand callback registration to six named form/mode combinations.
- [x] Consolidate equivalent routing bodies and overlapping sticky-removal scenarios. Preserve wrong-channel, repeated-removal, multi-event, payload identity, subscriber-form, and unaffected-channel/type assertions.
- [x] Move all nine internal dispatch/reservation tests into `EventBusReservationTest`. Keep public asynchronous posting in `EventBusAsyncDispatchTest`; group tests by operation and put fixtures before helpers/providers.
- [x] Standardize imports, fixture names, and parameter descriptions. Reuse bounded `TestBase` concurrency and monitor-wait helpers; remove duplicate workers and unused fixture state.
- [x] Strengthen complete payload and query-result assertions. Add invalid replacement preservation, named subscriber snapshots, and both sticky snapshot overloads. Query missing IDs against a populated store.
- [x] Fix the shutdown-hook test's initialization-order dependency found by randomized validation. Initialize the shared runtime before measuring hooks added by factory calls.
- [x] Update the ownership and consolidation guide, validate both suite entry points, and compare coverage against the baseline.

## Validation

- `mvn clean test jacoco:report`: 222 passed on Java 25, zero failures/errors/skips.
- Explicit `AbacusEventBusTestSuite` on Java 21 with random class/method ordering and seed `15102026`: the same 222 unique cases passed.
- The shutdown-hook factory test passed when selected alone in a fresh Java 21 JVM.
- All covered instruction and branch counts were preserved on every source line; aggregate coverage is unchanged.
- `git diff --check` passed. Changes are confined to tests and their documentation.

See [README.md](README.md) for test ownership, conventions, retained scenarios, and the existing coverage limitations.
