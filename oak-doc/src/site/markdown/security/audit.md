<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Audit SPI
--------------------------------------------------------------------------------

### General

The Oak audit SPI records structured events about repository activity and
dispatches them to in-process consumers. Listeners are registered on the OSGi
Whiteboard and invoked synchronously when events are produced. Typical
consumers forward events to a SIEM, write to a compliance archive, or apply
runtime policy.

The SPI is small: an event type, a listener interface, and an emitter service.
It does not prescribe transport, persistence, or out-of-process delivery.
Those are listener concerns.

A *capture site* is a place in Oak's own code that records an audit event.
Today the only ones are in the user-management implementation, on group
membership add and remove.

Two producer paths feed a single listener registry:

- A **commit-attached** path used by Oak-internal capture sites, currently
  group membership changes in the user-management implementation. Events are
  buffered for the duration of a session write, drained on the commit that
  follows, and decorated with commit metadata before dispatch. Events are
  dropped if the commit fails.
- A **fire-and-forget** path exposed to any OSGi bundle through the
  [AuditEventEmitter] service. Events are dispatched immediately on the
  calling thread. They are not tied to a commit and are not buffered.

Both paths converge on the same `AuditEventListener.onEvents(List<AuditEvent>)`
method, so a single listener can consume Oak-internal security events and
bundle-emitted custom events through one entry point.

<a name="modules"></a>
### Module layout

| Module | Role |
|---|---|
| `oak-core-spi`     | Domain-neutral SPI: [AuditEvent], [AuditEventListener], [AuditEventEmitter], the [AuditEvents] static facade, and [AuditConfiguration] (typed handle on the pipeline's runtime state). |
| `oak-security-spi` | Security-domain constants: `SecurityAuditDomain.DOMAIN` (the `"oak.security"` domain) and per-sub-domain vocabulary classes such as `UserAuditTypes` in the `spi.security.user` package. |
| `oak-core`         | Pipeline implementation: listener registry, commit-attached buffer, the observer that drains it on commit success, the emitter, and the configuration component. |

Consumer bundles depend on `oak-core-spi` only. Implementing a listener or
emitting events requires no dependency on `oak-core`, `oak-jcr`, or
`oak-security-spi`.

<a name="event_model"></a>
### Event model

#### AuditEvent

```java
public interface AuditEvent {
    @NotNull AuditDomain getDomain();
    @NotNull AuditType getType();
    long getTimestamp();
    @NotNull Map<String, Object> getPayload();
}
```

- **Domain**: namespace identifying the event source category, as an
  `AuditDomain`. Oak's security stack uses `SecurityAuditDomain.DOMAIN`, which
  wraps `"oak.security"`. Bundles defining new event types build their own with
  `AuditDomain.of("...")`; the SPI imposes no schema.
- **Type**: stable identifier within the domain, as an `AuditType`, e.g.
  `AuditType.of("membership.added")`. Consumers dispatch on it.
- **Timestamp**: milliseconds since epoch at event construction time.
- **Payload**: open map of supplementary data. Consumers MUST tolerate missing
  keys; producers MAY add keys without versioning.

`AuditDomain` and `AuditType` wrap their names rather than passing plain
strings around, and both validate in `of(...)`: a name must be non-blank and
usable as a JCR node name, with no colon and no whitespace. That keeps a
domain safe to use as a path element for listeners that persist events into
the repository, and it means a bad name fails at the producer instead of
reaching a listener. Call `name()` for the underlying string. The
[design document](audit-design.html#SPI_layout) has the full rules.

The public SPI keeps only the `AuditEvent` interface. Concrete events are
built with the static factory `AuditEvent.of(domain, type, payload)`, and
consumers discriminate events by `getDomain()` plus `getType()` rather than by
`instanceof` checks.

The `oak.security` domain pins its type strings and payload keys in
per-sub-domain classes next to the security area they describe. User-membership
constants live in `UserAuditTypes` in the `spi.security.user` package
(`MEMBER_ADDED`, `PAYLOAD_GROUP_PATH`, and so on). Bundles emitting custom
events implement `AuditEvent` directly or call `AuditEvent.of(...)` with their
own domain string.

<a name="commit_metadata_keys"></a>
#### Commit metadata payload keys

Events produced by the commit-attached pipeline are decorated at drain time
with three additional payload entries:

| Key | Value | Source |
|---|---|---|
| `oak.commit.sessionId` | session identifier of the writing session | `CommitInfo.getSessionId()` |
| `oak.commit.userId`    | acting user id (`CommitInfo.OAK_UNKNOWN`, i.e. `"oak:unknown"`, for system commits) | `CommitInfo.getUserId()` |
| `oak.commit.timestamp` | commit timestamp in milliseconds since epoch | `CommitInfo.getDate()` |

The three key names are published as `AuditEvent.COMMIT_SESSION_ID`,
`AuditEvent.COMMIT_USER_ID`, and `AuditEvent.COMMIT_TIMESTAMP`; use those
rather than string literals.

Events arriving through the fire-and-forget pipeline cannot carry these keys:
Oak strips caller-supplied values for exactly these three at dispatch. For
events delivered through Oak dispatch, their presence is therefore a reliable
commit-attached signal. The Javadoc on `AuditEvent#getPayload()` is the
normative statement of this contract. Consumers that need to tell the two
sources apart call `AuditEvent.isCommitAttested(event)`, which returns
`true` when all three keys are present and non-null. The `oak.commit.userId`
value `"oak:unknown"` is a deliberate anonymity marker for system commits;
listeners MUST NOT attempt to resolve it to a real user.

<a name="pipelines"></a>
### Pipelines

#### Commit-attached pipeline

Used by Oak-internal capture sites in the user-management implementation. Events
are buffered against the writing session and only dispatched when
`Root.commit()` succeeds, strictly **after** durable persistence rather than
inside the commit hook chain. If validators reject the commit or the merge
fails, the buffered events are discarded.

The dispatch sequence:

1. A capture site appends an event to the per-session buffer.
2. The session reaches `Root.commit()`; commit hooks and validators run; the
   merge persists durably.
3. An `Observer` registered by the audit configuration fires on the commit
   thread, drains the buffer for the originating session, and decorates each
   event with `oak.commit.sessionId`, `oak.commit.userId`, and
   `oak.commit.timestamp`.
4. The registry sorts listeners by rank, filters by domain, and invokes each
   matching listener's `onEvents(List<AuditEvent>)`.

Step 2 does not require an explicit `Session.save()`. Operations that commit
on their own, such as `Workspace.move` or `VersionManager.checkin`, reach
`Root.commit()` too and drain the buffer the same way.

Because dispatch happens after durable persistence, a delivered event implies
the corresponding write actually landed. A failed commit never produces an
audit event.

The converse does not hold, and consumers building a compliance trail need to
know it. The per-session buffer is capped, so a session that records more
than 10,000 events before committing has its later events dropped, with a
single WARN logged for that session rather than one per dropped event. A
persisted write can therefore leave no audit event behind. The cap exists to
bound the memory one runaway session can pin; it resets on the next commit,
refresh, or commit failure. Treat that WARN as a gap in the trail.

This path is internal to Oak; bundles that want to record their own events use
the fire-and-forget pipeline below.

#### Fire-and-forget pipeline

Available to any OSGi bundle that wants to record an event for its own domain.
Events fire immediately on the calling thread; there is no buffering and no
rollback:

1. The caller resolves `AuditEventEmitter` via `@Reference`.
2. The caller gates allocation with `isEnabledFor(domain)`.
3. The caller invokes `emit(event)`.
4. The registry sorts listeners by rank, filters by domain, and invokes each
   matching listener's `onEvents(List<AuditEvent>)`.

Properties:

- **No commit boundary.** The event is dispatched as soon as `emit` is called;
  subsequent JCR operations do not affect it.
- **Synchronous on the calling thread.** Listeners performing I/O are
  responsible for wrapping themselves in an async dispatcher.
- **Per-listener isolation.** Exceptions thrown by one listener, whether from
  `onEvents` or from the `getDomain()` / `getRank()` accessors consulted
  during routing, are logged and swallowed; remaining listeners still run.
  `emit` never propagates a listener exception back to the caller.
- **No payload decoration, but reserved keys are stripped.** No `oak.commit.*`
  keys are added; caller-supplied values for the three reserved attestation
  keys (`oak.commit.sessionId`, `oak.commit.userId`,
  `oak.commit.timestamp`) are removed before delivery. Every other entry
  reaches listeners exactly as the caller provided it.

<a name="configuration"></a>
### Configuration

The pipeline is gated by a feature toggle and is **off by default**. Nothing
is captured or dispatched until the toggle is enabled, which keeps the cost
of a deployed-but-unused pipeline at zero.

Registering a listener is not enough on its own. Both conditions have to
hold: the toggle is enabled, and at least one listener is registered for the
event's domain. Enable the toggle the same way as any other Oak feature
toggle, through the `FeatureToggle` service published on the Whiteboard; the
[OSGi configuration](../osgi_config.html) page describes the mechanism under
Feature Toggles. For a worked example of locating this toggle and flipping
it, see the embedded wiring snippet in
[Audit Pipeline Design](audit-design.html).

`AuditPipeline` in `oak-core` owns the pipeline and is published as
an OSGi service of type `AuditConfiguration`. It carries an OSGi
object-class definition, so it appears in the Felix console alongside Oak's
other components.

<a name="pipeline_state_probe"></a>
### Probing pipeline state

Components can ask whether the audit pipeline is currently active via
[AuditConfiguration]`.isActive()`, without depending on the implementation
class. `AuditConfiguration` is an OSGi service; resolve it via a DS
`@Reference`:

```java
@Component(service = MyComponent.class)
public class MyComponent {

    @Reference
    private AuditConfiguration audit;

    public void doWork() {
        if (audit.isActive()) {
            // Feature toggle is ON and at least one listener is registered.
            // Safe to do work that only matters when audit will actually
            // dispatch (e.g. allocate richer payload context).
        }
    }
}
```

`AuditConfiguration` is published as an OSGi service only. Embedded callers
(tests, `oak-run` tools) use `AuditEvents.isEnabled()` on the static facade
instead, which evaluates the same two conditions.

`isActive()` returns `true` when the audit feature toggle is enabled AND at
least one `AuditEventListener` is registered on the Whiteboard. A
deployed-but-unused pipeline (toggle ON, no listener registered) reports
`false`, matching the no-allocation semantics of `AuditEvents.isEnabled()`.
The NOOP `AuditConfiguration`, returned when no implementation is bound at
all, reports `false`.

Note that audit is a top-level Oak concern, not a `SecurityConfiguration`:
`AuditConfiguration` is not reachable via
`SecurityProvider.getConfiguration(...)`. Use a `@Reference` to
`AuditConfiguration`.

<a name="monitoring"></a>
### Monitoring

The pipeline registers metrics through the `StatisticsProvider` it resolves at
activation, so they surface via JMX or Sling Metrics like Oak's other metrics.
Nothing is registered when no `StatisticsProvider` is bound.

| Name | Type | Description |
|---|---|---|
| `security.audit.events;domain=<domain>` | Meter | Events dispatched, per domain. Counts events that reached at least one listener, so it excludes anything dropped at the toggle or the listener gate. |
| `security.audit.listener.duration;listener=<class>` | Timer | Wall-clock duration of one `onEvents` call, per listener class. |
| `security.audit.listener.failures;listener=<class>` | Meter | Dispatches that ended in a `Throwable` from the listener. |
| `security.audit.events.dropped;domain=<domain>` | Meter | Events discarded because the originating session hit the per-session buffer cap. |

The `domain=` and `listener=` suffixes follow Oak's `StatsProviderUtil`
label convention, which Prometheus and similar systems split back into a
metric name plus labels.

The listener timer is worth an alert. Listeners run synchronously on the
commit thread, so time spent in `onEvents` is added directly to commit
latency for the writing session. A listener that starts doing I/O inline
shows up here before it shows up as a user complaint.

The dropped-events meter is the one that matters for a compliance trail: a
non-zero value means a persisted write left no audit event behind. The same
condition logs a WARN, but the meter is what you can alert on.

<a name="user_api_semantics"></a>
### User-API-level audit, not a transaction log

Oak's audit SPI captures activity at the level of user API calls, not at the
level of the transaction log. The distinction matters when choosing whether
the audit SPI fits a given use case.

- **What fires audit events:** capture sites in Oak's user-management
  implementation. Group membership changes record member add/remove events.
  The usual route is a `Group.addMember(...)` / `.removeMember(...)` call from
  the Jackrabbit user-management API, but the same capture site also covers
  membership applied by the protected-item importer during XML import, which
  reaches it without any user-facing API call. Equivalent capture sites can
  cover other security-relevant areas.
- **What does NOT fire audit events:** changes made by commit hooks, editors,
  or validators during commit processing. If a hook transforms the tree in
  flight (autocreated properties, denormalised indexes, side-effect writes
  from a `Validator` or `Editor`), those tree changes are not recorded even
  though they end up in the merged `NodeState`.

This is intentional. The audit SPI answers "who called the API", which is the
right level for security audit, compliance trails, and "who removed user X
from group Y" investigations. It does not enumerate every node mutation that
landed in the merged commit.

Consumers needing every node mutation (event sourcing, change-data capture,
derived index rebuilding) should use Oak's `NodeStore.addObserver(...)` /
`BackgroundObserver` mechanism instead. Those observers see the post-merge
`NodeState` diff and capture mutations regardless of which API surface or
commit hook produced them. The audit SPI and a `NodeStore` observer answer
different questions; deploy the one that matches your use case.

<a name="clustering"></a>
### Clustering

Audit events are node-local. A write on one cluster node produces events on
that node only, dispatched to the listeners registered there. The drain
observer ignores commits for which `CommitInfo.isExternal()` is `true`, and
cluster sync from a peer node is exactly that, so the same write does not
produce a second event when it reaches the other nodes.

For a listener deployed on every node this gives the property you want: each
audited write is delivered once, on the node that performed it. Aggregating
into a single SIEM or compliance archive therefore needs the listener to tag
events with the node they came from, since the SPI adds no node-identity
payload key. `DocumentNodeStore.getClusterId()` is the per-node identifier;
note that `ClusterRepositoryInfo.getId(...)` is not, since it returns one id
shared by the whole cluster.

Two consequences to plan for. A listener deployed on only some nodes sees
only the writes performed on those nodes, which for a compliance trail is a
silent gap rather than an error. And a node going down loses whatever its
listeners had buffered in their own async queues, if they use one; the audit
SPI dispatches synchronously and holds no cross-node state, so durability
past the dispatch call belongs to the listener.

Everything above applies to the document store, where clustering is
supported. A segment-store cold-standby instance produces no commit-attached
events at all, for the reason given under
[Commit flow](audit-design.html#Commit_flow).

<a name="emitting_events"></a>
### Emitting events from a bundle

Bundles emit events through the [AuditEventEmitter] OSGi service. A single
implementation is registered by `oak-core`.

```java
@Component
public class ContentPublishAuditor {

    private static final AuditDomain DOMAIN = AuditDomain.of("example.content");

    @Reference
    private AuditEventEmitter audit;

    public void onPublished(String path, String variant) {
        if (audit.isEnabledFor(DOMAIN)) {
            audit.emit(new ContentPublishedEvent(path, variant));
        }
    }
}
```

The `isEnabledFor` gate short-circuits when no listener is registered for the
domain, so callers can skip event construction on hot paths. The check is
cheap; producers SHOULD use it.

A minimal event implementation:

```java
class ContentPublishedEvent implements AuditEvent {

    private static final AuditDomain DOMAIN = AuditDomain.of("example.content");
    private static final AuditType TYPE = AuditType.of("content.published");

    private final String path;
    private final String variant;
    private final long timestamp = System.currentTimeMillis();

    ContentPublishedEvent(String path, String variant) {
        this.path = path;
        this.variant = variant;
    }

    @Override public AuditDomain getDomain()          { return DOMAIN; }
    @Override public AuditType getType()              { return TYPE; }
    @Override public long getTimestamp()             { return timestamp; }
    @Override public Map<String, Object> getPayload() {
        return Map.of("path", path, "variant", variant);
    }
}
```

Events emitted this way are not tied to a JCR session or commit. The caller
need not hold a `Session` or `Root`, so lifecycle events such as workflow
transitions, replication outcomes, or background-job completion are valid
producers.

<a name="implementing_a_listener"></a>
### Implementing a listener

A listener is an OSGi component registered as a service of type
[AuditEventListener]. The Whiteboard registry discovers it automatically.

```java
@Component(service = AuditEventListener.class)
public class SiemForwarder implements AuditEventListener {

    @Override
    public AuditDomain getDomain() {
        return SecurityAuditDomain.DOMAIN;
    }

    @Override
    public int getRank() {
        return 0;
    }

    @Override
    public void onEvents(List<AuditEvent> events) {
        for (AuditEvent e : events) {
            if (!AuditEvent.isCommitAttested(e)) {
                continue;   // caller-asserted, not an Oak-attested write
            }
            Map<String, Object> p = e.getPayload();
            String sessionId = (String) p.get(AuditEvent.COMMIT_SESSION_ID);
            String userId    = (String) p.get(AuditEvent.COMMIT_USER_ID);
            siem.forward(e, sessionId, userId);
        }
    }
}
```

Contract notes:

- **`getDomain()`** is queried on every dispatch and MUST return a stable,
  non-null value across the listener's lifetime. A listener subscribes to
  exactly one domain. To consume multiple domains, register multiple listener
  components.
- **`getRank()`** orders listeners within a domain, higher rank first, default
  0. Useful when one listener must observe state set by another (for example,
  a redaction listener running before a SIEM forwarder).
- Both accessors are treated as listener code. A listener whose `getDomain()`
  or `getRank()` throws is skipped for that dispatch and picked up again once
  the accessor stops throwing; the failure is logged at WARN the first time
  for that listener instance and at DEBUG afterwards, so a broken listener
  cannot flood the log. Other listeners are unaffected.
- **`onEvents(List<AuditEvent>)`** is invoked with a non-empty, non-null list
  of events in capture order. The same method serves both pipelines:
  commit-attached events arrive in a batch sized by the originating session's
  buffer; fire-and-forget events arrive in singleton lists.
- Implementations MUST be non-blocking. Expensive I/O belongs in an async
  wrapper owned by the listener.
- Implementations MUST tolerate unknown payload keys and missing optional
  keys. The payload schema is open.

<a name="trust_model"></a>
### Trust model

The fire-and-forget producer surface is open by design.

- Any bundle that resolves `AuditEventEmitter` can emit any event for any
  domain, including `"oak.security"`. There is no compile-time check, no
  reserved domain registry, and no runtime gate on the emitting bundle.
- Listeners therefore receive caller-asserted data. An event arriving through
  `onEvents` reflects the emitting bundle's claim, not Oak-verified truth.
- Oak does not verify, sign, or annotate events with their originating
  bundle. Consumers that require Oak attestation MUST distinguish events at
  the consumer side.

The distinguishing signal is payload-based and enforced at dispatch: events
produced by the commit-attached pipeline carry the `oak.commit.sessionId`,
`oak.commit.userId`, and `oak.commit.timestamp` keys, unconditionally
overwritten from the commit's `CommitInfo`. Fire-and-forget events cannot
carry them, because Oak strips caller-supplied values for exactly these
three keys before delivery. `AuditEvent.isCommitAttested(event)` does the
check, so listeners need neither the key names nor the rule. A SIEM
forwarder that treats only attested events as Oak-verified mutations is
operating within the contract. The Javadoc on `AuditEvent#getPayload()` is
the normative statement, including the boundaries of the attestation: it
applies to Oak dispatch only and does not survive re-emission.

The open surface is a deliberate trade-off. A reserved-domain registry or
typed event subclasses would put Oak in the middle of every producer bundle's
policy decision; the open surface lets any higher-stack bundle emit on its own
schedule and shifts allowlisting to the consumer side, where the deployment
owner already controls listener registration.

Recommended consumer-side discipline:

| Need | Approach |
|---|---|
| Distinguish Oak-attested mutations from caller-asserted events. | Call `AuditEvent.isCommitAttested(event)`. It anchors on the three reserved keys, not on the `oak.commit.` prefix in general. |
| Restrict trusted producers. | Maintain a consumer-side allowlist of trusted domain prefixes and reject unknown domains. |
| Compliance audit (Oak-verified writes only). | Subscribe to `"oak.security"` and keep only events for which `AuditEvent.isCommitAttested(event)` is `true`. |

<a name="further_reading"></a>
### Further Reading

- [Audit Pipeline Design](audit-design.html): the design document covering
  the SPI shape, pipeline internals, OSGi and embedded wiring, threading
  invariants, and performance characteristics.
- [OAK-12331](https://issues.apache.org/jira/browse/OAK-12331): the issue
  that introduced the audit SPI.

<!-- references -->
[AuditEvent]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/audit/AuditEvent.html
[AuditEventListener]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/audit/AuditEventListener.html
[AuditEventEmitter]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/audit/AuditEventEmitter.html
[AuditEvents]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/audit/AuditEvents.html
[AuditConfiguration]: /oak/docs/apidocs/org/apache/jackrabbit/oak/spi/audit/AuditConfiguration.html
