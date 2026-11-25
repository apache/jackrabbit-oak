/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.security.audit;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.event.Level;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CommitMetadataDecoratorTest {

    @Before
    public void resetStripWarnLatch() {
        // The strip WARN is once-per-JVM; reset so each test observes a
        // deterministic latch state regardless of suite ordering.
        CommitMetadataDecorator.STRIP_WARNED.set(false);
    }

    private static AuditEvent original(@NotNull AuditDomain domain, @NotNull AuditType type, @NotNull Map<String, Object> payload) {
        return new AuditEvent() {
            @Override public @NotNull AuditDomain getDomain() { return domain; }
            @Override public @NotNull AuditType getType() { return type; }
            @Override public long getTimestamp() { return 12345L; }
            @Override public @NotNull Map<String, Object> getPayload() { return payload; }
        };
    }

    @Test
    public void decoratesPayloadWithCommitMetadata() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("member.added"), Map.of("group", "/g", "member", "/m"));
        CommitInfo info = new CommitInfo("session-1", "alice", Map.of(), false);
        List<AuditEvent> out = CommitMetadataDecorator.decorate(List.of(in), info);
        assertEquals(1, out.size());
        AuditEvent decorated = out.get(0);
        Map<String, Object> p = decorated.getPayload();
        assertEquals("session-1", p.get("oak.commit.sessionId"));
        assertEquals("alice", p.get("oak.commit.userId"));
        assertTrue(p.containsKey("oak.commit.timestamp"));
        // original payload entries preserved
        assertEquals("/g", p.get("group"));
        assertEquals("/m", p.get("member"));
        // original event passed in not mutated
        assertEquals(Map.of("group", "/g", "member", "/m"), in.getPayload());
        assertNotSame(in, decorated);
    }

    @Test
    public void emptyEventsReturnsEmpty() {
        CommitInfo info = new CommitInfo("session-1", "alice", Map.of(), false);
        // Semantic check, not identity check — couples to behavior, not impl.
        assertTrue(CommitMetadataDecorator.decorate(Collections.emptyList(), info).isEmpty());
    }

    @Test
    public void preservesDomainTypeTimestamp() {
        AuditEvent in = original(AuditDomain.of("example.content"), AuditType.of("fragment.published"), Map.of("path", "/p"));
        CommitInfo info = new CommitInfo("s", "u", Map.of(), false);
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        assertEquals("example.content", decorated.getDomain().name());
        assertEquals("fragment.published", decorated.getType().name());
        assertEquals(12345L, decorated.getTimestamp());
    }

    @Test
    public void systemCommitUserIdIsOakUnknown() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("system.event"), Map.of());
        // CommitInfo with null userId resolves to OAK_UNKNOWN
        CommitInfo info = CommitInfo.EMPTY;
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        // CommitInfo.OAK_UNKNOWN exposed via getUserId() for empty/system commits
        assertEquals(CommitInfo.OAK_UNKNOWN, decorated.getPayload().get("oak.commit.userId"));
    }

    @Test
    public void preservesOrderOfEvents() {
        AuditEvent a = original(AuditDomain.of("oak.security"), AuditType.of("a"), Map.of());
        AuditEvent b = original(AuditDomain.of("oak.security"), AuditType.of("b"), Map.of());
        AuditEvent c = original(AuditDomain.of("oak.security"), AuditType.of("c"), Map.of());
        CommitInfo info = new CommitInfo("s", "u", Map.of(), false);
        List<AuditEvent> out = CommitMetadataDecorator.decorate(asList(a, b, c), info);
        assertEquals("a", out.get(0).getType().name());
        assertEquals("b", out.get(1).getType().name());
        assertEquals("c", out.get(2).getType().name());
    }

    //--------------------------------------------< overwrite invariant tests >---
    // Security-critical regression guards. The "the three reserved commit.*
    // keys are present iff the event came from Oak's commit-attached
    // pipeline" trust-model property (normative statement on
    // AuditEvent#getPayload(); see also audit-design.md) rests on
    // TWO halves: the decorator UNCONDITIONALLY overwriting caller-supplied
    // values on the commit path (pinned here) and the dispatch-time strip on
    // the fire-and-forget path (pinned in the strip section below and
    // end-to-end in AuditPipelineTest). A future refactor that swaps .put()
    // for .putIfAbsent() / contains-check would silently let bundles spoof
    // commit identity in audit logs. Each test pins one independent property
    // a single-line regression could break.

    @Test
    public void decoratorOverwritesCallerProvidedSessionId() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"),
                Map.of(CommitMetadataDecorator.KEY_SESSION_ID, "spoofed-session"));
        CommitInfo info = new CommitInfo("real-session", "alice", Map.of(), false);
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        assertEquals("real-session", decorated.getPayload().get(CommitMetadataDecorator.KEY_SESSION_ID));
        assertNotEquals("spoofed value must not survive decoration",
                "spoofed-session", decorated.getPayload().get(CommitMetadataDecorator.KEY_SESSION_ID));
    }

    @Test
    public void decoratorOverwritesCallerProvidedUserId() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"),
                Map.of(CommitMetadataDecorator.KEY_USER_ID, "admin"));
        CommitInfo info = new CommitInfo("s", "alice", Map.of(), false);
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        assertEquals("alice", decorated.getPayload().get(CommitMetadataDecorator.KEY_USER_ID));
        assertNotEquals("spoofed userId must not survive decoration",
                "admin", decorated.getPayload().get(CommitMetadataDecorator.KEY_USER_ID));
    }

    @Test
    public void decoratorOverwritesCallerProvidedTimestamp() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"),
                Map.of(CommitMetadataDecorator.KEY_TIMESTAMP, 99999999L));
        // CommitInfo's date is set internally to System.currentTimeMillis()
        // at construction; we read the actual value via getDate() to
        // compare against the decorated payload.
        CommitInfo info = new CommitInfo("s", "alice", Map.of(), false);
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        assertEquals(info.getDate(), decorated.getPayload().get(CommitMetadataDecorator.KEY_TIMESTAMP));
        assertNotEquals("spoofed timestamp must not survive decoration",
                99999999L, decorated.getPayload().get(CommitMetadataDecorator.KEY_TIMESTAMP));
    }

    /**
     * Type-variance check: caller submits {@code commit.timestamp} as a
     * {@code String}, decorator overwrites with the real {@code long}.
     * Guards against future "type-aware merge" pseudo-smartening that
     * would skip the overwrite when types differ.
     */
    @Test
    public void decoratorOverwritesAcrossValueTypes() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"),
                Map.of(CommitMetadataDecorator.KEY_TIMESTAMP, "definitely-a-string-not-a-long"));
        CommitInfo info = new CommitInfo("s", "u", Map.of(), false);
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        Object timestamp = decorated.getPayload().get(CommitMetadataDecorator.KEY_TIMESTAMP);
        // CommitInfo.getDate() returns long; merged.put(..., commitTimestamp)
        // auto-boxes to Long. The original String value is gone.
        assertTrue("timestamp must be Long, not the caller-provided String",
                timestamp instanceof Long);
    }

    /**
     * Trust-contract regression (inverse form): when a single caller-supplied
     * payload spoofs ALL THREE Oak-attested keys at once
     * ({@code commit.sessionId}, {@code commit.userId}, {@code commit.timestamp}),
     * the decorator overwrites every one of them with the {@code CommitInfo}
     * values. Pins the security property documented on
     * {@link org.apache.jackrabbit.oak.spi.audit.AuditEvent#getPayload()}:
     * exactly these three keys are Oak-attested and cannot be forged by the
     * caller. Complements the per-key overwrite tests above by proving all
     * three are protected within one event — a partial-overwrite regression
     * that fixed only some keys would slip past single-key tests.
     */
    @Test
    public void decoratorOverwritesAllThreeOakAttestedKeysSimultaneously() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"), Map.of(
                CommitMetadataDecorator.KEY_SESSION_ID, "spoofed-session",
                CommitMetadataDecorator.KEY_USER_ID, "spoofed-user",
                CommitMetadataDecorator.KEY_TIMESTAMP, 1L));
        CommitInfo info = new CommitInfo("real-session", "real-user", Map.of(), false);
        Map<String, Object> p = CommitMetadataDecorator.decorate(List.of(in), info).get(0).getPayload();

        assertEquals("real-session", p.get(CommitMetadataDecorator.KEY_SESSION_ID));
        assertEquals("real-user", p.get(CommitMetadataDecorator.KEY_USER_ID));
        assertEquals(info.getDate(), p.get(CommitMetadataDecorator.KEY_TIMESTAMP));
        assertNotEquals("spoofed-session", p.get(CommitMetadataDecorator.KEY_SESSION_ID));
        assertNotEquals("spoofed-user", p.get(CommitMetadataDecorator.KEY_USER_ID));
        assertNotEquals(1L, p.get(CommitMetadataDecorator.KEY_TIMESTAMP));
    }

    /**
     * Pins the negative half of the trust contract: a {@code commit.*} key
     * that is NOT one of the three Oak-attested keys is forwarded verbatim
     * from the caller (it is untrusted). A listener must not treat the
     * {@code commit.} prefix as a blanket attestation — only the three named
     * keys are protected. See
     * {@link org.apache.jackrabbit.oak.spi.audit.AuditEvent#getPayload()}.
     */
    @Test
    public void decoratorDoesNotProtectOtherCommitPrefixedKeys() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"), Map.of("commit.custom", "caller-supplied"));
        CommitInfo info = new CommitInfo("s", "u", Map.of(), false);
        Map<String, Object> p = CommitMetadataDecorator.decorate(List.of(in), info).get(0).getPayload();
        // The three Oak-attested keys are added/overwritten...
        assertEquals("s", p.get(CommitMetadataDecorator.KEY_SESSION_ID));
        // ...but an arbitrary commit.* key is left exactly as the caller set it.
        assertEquals("caller-supplied", p.get("commit.custom"));
    }

    /**
     * Symmetric to the overwrite tests: when the input payload omits the
     * commit.* keys entirely, the decorator ADDS them. A regression that
     * turned {@code .put()} into {@code if (containsKey) .put()} would
     * pass the overwrite-when-present tests but break add-when-absent.
     * Mixed coverage with {@link #decoratesPayloadWithCommitMetadata()}
     * is insufficient — that test conflates the add-when-absent half
     * with non-commit-key preservation.
     */
    @Test
    public void decoratorAddsCommitKeysWhenAbsent() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"), Map.of());
        CommitInfo info = new CommitInfo("s", "u", Map.of(), false);
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        assertTrue(decorated.getPayload().containsKey(CommitMetadataDecorator.KEY_SESSION_ID));
        assertTrue(decorated.getPayload().containsKey(CommitMetadataDecorator.KEY_USER_ID));
        assertTrue(decorated.getPayload().containsKey(CommitMetadataDecorator.KEY_TIMESTAMP));
        assertEquals("s", decorated.getPayload().get(CommitMetadataDecorator.KEY_SESSION_ID));
        assertEquals("u", decorated.getPayload().get(CommitMetadataDecorator.KEY_USER_ID));
    }

    @Test
    public void decoratedPayloadIsUnmodifiable() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"), Map.of("k", "v"));
        CommitInfo info = new CommitInfo("s", "u", Map.of(), false);
        AuditEvent decorated = CommitMetadataDecorator.decorate(List.of(in), info).get(0);
        try {
            decorated.getPayload().put("newkey", "newvalue");
            fail("decorated payload must be unmodifiable — caller mutation must throw");
        } catch (UnsupportedOperationException expected) {
            // expected — Collections.unmodifiableMap wrapping
        }
    }

    //-----------------------------------< reserved-key strip (f-a-f path) >---
    // stripReservedCommitKeys is the fire-and-forget counterpart of the
    // overwrite invariant above: the commit-attached path OVERWRITES the
    // three Oak-attested keys, the fire-and-forget path STRIPS them. Both
    // enforce the same trust contract on AuditEvent#getPayload(): a
    // dispatched payload carries those keys iff Oak put them there.

    @Test
    public void stripRemovesAllThreeReservedKeysAndKeepsTheRest() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"), Map.of(
                CommitMetadataDecorator.KEY_SESSION_ID, "forged-session",
                CommitMetadataDecorator.KEY_USER_ID, "forged-user",
                CommitMetadataDecorator.KEY_TIMESTAMP, 1L,
                "commit.custom", "passenger",
                "k", "v"));
        AuditEvent stripped = CommitMetadataDecorator.stripReservedCommitKeys(in);

        Map<String, Object> p = stripped.getPayload();
        assertFalse(p.containsKey(CommitMetadataDecorator.KEY_SESSION_ID));
        assertFalse(p.containsKey(CommitMetadataDecorator.KEY_USER_ID));
        assertFalse(p.containsKey(CommitMetadataDecorator.KEY_TIMESTAMP));
        // Only the three reserved keys are stripped — commit.-prefixed
        // passengers and ordinary entries are forwarded verbatim.
        assertEquals("passenger", p.get("commit.custom"));
        assertEquals("v", p.get("k"));
        // Domain/type/capture-timestamp survive: the wrapper delegates, it
        // does NOT rebuild via AuditEvent.of() (which would reset the
        // timestamp to wall-clock now).
        assertEquals("oak.security", stripped.getDomain().name());
        assertEquals("x", stripped.getType().name());
        assertEquals(12345L, stripped.getTimestamp());
        // Input event untouched.
        assertEquals("forged-session", in.getPayload().get(CommitMetadataDecorator.KEY_SESSION_ID));
    }

    @Test
    public void stripWithSingleReservedKeyStripsIt() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"), Map.of(
                CommitMetadataDecorator.KEY_USER_ID, "forged-user",
                "k", "v"));
        Map<String, Object> p = CommitMetadataDecorator.stripReservedCommitKeys(in).getPayload();
        assertFalse("any one reserved key must trigger the strip",
                p.containsKey(CommitMetadataDecorator.KEY_USER_ID));
        assertEquals("v", p.get("k"));
    }

    @Test
    public void stripReturnsSameInstanceWhenNoReservedKeyPresent() {
        AuditEvent in = original(AuditDomain.of("oak.security"), AuditType.of("x"), Map.of("commit.custom", "passenger", "k", "v"));
        assertSame("clean payloads must not be wrapped — concrete event type preserved",
                in, CommitMetadataDecorator.stripReservedCommitKeys(in));
    }

    /**
     * No silent data deletion: the first strip in the JVM logs a WARN
     * naming the stripped keys plus the event's domain and type — and
     * NEVER the forged values (they are attacker-controlled and would
     * poison the log). Subsequent strips stay at DEBUG so a persistent
     * emitter cannot use the pipeline as a WARN-flood vector.
     */
    @Test
    public void stripLogsWarnOnceWithKeyNamesButNeverValues() {
        LogCustomizer log = LogCustomizer.forLogger(CommitMetadataDecorator.class)
                .enable(Level.WARN).create();
        log.starting();
        try {
            CommitMetadataDecorator.stripReservedCommitKeys(original(AuditDomain.of("oak.security"), AuditType.of("strip.warn.type"),
                    Map.of(CommitMetadataDecorator.KEY_SESSION_ID, "forged-session-value", "k", "v")));
            CommitMetadataDecorator.stripReservedCommitKeys(original(AuditDomain.of("oak.security"), AuditType.of("strip.other.type"),
                    Map.of(CommitMetadataDecorator.KEY_USER_ID, "forged-user-value")));

            List<String> logs = log.getLogs();
            assertEquals("strip must WARN exactly once, then drop to DEBUG", 1, logs.size());
            String warn = logs.get(0);
            assertTrue("WARN must name the stripped key; was: " + warn,
                    warn.contains(CommitMetadataDecorator.KEY_SESSION_ID));
            assertTrue("WARN must name the event domain; was: " + warn,
                    warn.contains("oak.security"));
            assertTrue("WARN must name the event type; was: " + warn,
                    warn.contains("strip.warn.type"));
            assertFalse("WARN must never echo the forged value; was: " + warn,
                    warn.contains("forged-session-value"));
        } finally {
            log.finished();
        }
    }

    @Test
    public void strippedPayloadIsUnmodifiable() {
        // Mutable input payload on purpose: a stub that returned the input
        // event (or its map) unchanged would let the put() succeed.
        Map<String, Object> mutable = new HashMap<>();
        mutable.put(CommitMetadataDecorator.KEY_SESSION_ID, "forged");
        mutable.put("k", "v");
        AuditEvent stripped = CommitMetadataDecorator.stripReservedCommitKeys(
                original(AuditDomain.of("oak.security"), AuditType.of("x"), mutable));
        try {
            stripped.getPayload().put("newkey", "newvalue");
            fail("stripped payload must be unmodifiable — caller mutation must throw");
        } catch (UnsupportedOperationException expected) {
            // expected
        }
    }

    /**
     * TOCTOU regression: the strip consults {@code getPayload()} EXACTLY
     * once and snapshots the filtered result eagerly in the wrapper
     * constructor (mirrors {@code DecoratedAuditEvent}). {@link AuditEvent}
     * is directly implementable, so a hostile implementation could return a
     * clean map when checked and a forged one when the listener later reads
     * it — lazy filtering or a second consult would re-open the hole this
     * strip closes.
     */
    @Test
    public void stripSnapshotsPayloadEagerlyFromSingleConsult() {
        AtomicInteger consults = new AtomicInteger();
        AuditEvent hostile = new AuditEvent() {
            @Override public @NotNull AuditDomain getDomain() { return AuditDomain.of("oak.security"); }
            @Override public @NotNull AuditType getType() { return AuditType.of("x"); }
            @Override public long getTimestamp() { return 12345L; }
            @Override public @NotNull Map<String, Object> getPayload() {
                if (consults.incrementAndGet() == 1) {
                    return Map.of(CommitMetadataDecorator.KEY_SESSION_ID, "forged", "k", "v");
                }
                return Map.of("toctou.marker", "swapped-after-check");
            }
        };
        AuditEvent stripped = CommitMetadataDecorator.stripReservedCommitKeys(hostile);
        assertEquals("strip must consult the delegate payload exactly once",
                1, consults.get());

        Map<String, Object> p = stripped.getPayload();
        assertEquals("reading the stripped payload must not re-consult the delegate",
                1, consults.get());
        assertFalse(p.containsKey(CommitMetadataDecorator.KEY_SESSION_ID));
        assertFalse("payload swapped in after the check must be invisible",
                p.containsKey("toctou.marker"));
        assertEquals("v", p.get("k"));
    }
}
