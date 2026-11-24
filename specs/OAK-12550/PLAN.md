# OAK-12550: Implementation plan

## 1. Regression coverage

Keep the existing `UserMonitorImplTest.testDoneGetMembers` assertions and add
`testDoneGetMembersTimers` with a distinct mock for each named timer.
Verify that declared-only retrieval updates only the declared-members timer and
that declared-and-inherited retrieval updates only the members timer, using the
supplied durations in nanoseconds.

## 2. Implementation

Swap the timer updates in the two branches of `UserMonitorImpl.doneGetMembers`.
No changes to callers, metric registration, or other monitor methods are needed.

## 3. Validation

- Confirmed the new regression test fails against the original implementation.
- After the fix, the following command passed:
  `mvn -q test -pl oak-core -Dtest=UserMonitorImplTest,GroupImplTest`.

## 4. Documentation

Do not add site documentation here. OAK-12495 adds documentation for all user
monitor methods; its metric descriptions should reflect the corrected routing
specified in SPEC.md.
