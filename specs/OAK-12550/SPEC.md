# OAK-12550: Correct group member retrieval timer routing

Tracked in [OAK-12550](https://issues.apache.org/jira/browse/OAK-12550).

## Goals

- Correct `UserMonitorImpl.doneGetMembers` so `declaredOnly=true` updates
  `security.user.get_declared_members.timer`, while `false` updates
  `security.user.get_members.timer` for declared and inherited members.
- Each call updates only the appropriate timer, preserving the supplied duration
  and nanosecond time unit.
- Add regression coverage that distinguishes the two named timers. The existing
  test uses a shared mock timer and cannot detect reversed routing.

## Scope

- Change only the timer selection; preserve metric names, API contracts, callers,
  and other monitoring behavior.
- This is a small correction to existing metric routing; no feature toggle.
- Site documentation for all user monitor methods remains part of
  [OAK-12495](https://issues.apache.org/jira/browse/OAK-12495), whose review
  identified this pre-existing bug.
