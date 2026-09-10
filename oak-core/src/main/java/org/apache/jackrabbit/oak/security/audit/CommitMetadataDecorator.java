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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces the {@code commit.*} trust contract at both dispatch
 * boundaries: {@link #decorate} stamps commit-attached payloads with
 * commit metadata (sessionId, userId, timestamp) at drain time, and
 * {@link #stripReservedCommitKeys} removes caller-supplied values for the
 * same three keys from fire-and-forget payloads before delivery.
 * <p>
 * Both operations return NEW {@link AuditEvent} instances that wrap the
 * originals; the input events are not mutated. The wrapper's payload map
 * is unmodifiable.
 *
 * <h3>Security invariant</h3>
 * Both halves enforce the same property: listeners can treat the presence
 * of {@link #KEY_SESSION_ID}, {@link #KEY_USER_ID}, or {@link #KEY_TIMESTAMP}
 * in a dispatched payload as Oak-attested — see the normative trust contract
 * on {@link org.apache.jackrabbit.oak.spi.audit.AuditEvent#getPayload()}.
 * {@link #decorate} <strong>unconditionally overwrites</strong> the three
 * keys with the values from the {@link CommitInfo} captured for the
 * surrounding commit; {@link #stripReservedCommitKeys} removes
 * caller-supplied values for the same keys on the fire-and-forget path.
 * Weakening either half — {@code putIfAbsent} / {@code computeIfAbsent} /
 * conditional {@code put} in the decorator, or skipping the strip at
 * dispatch — is a regression in the trust model.
 *
 * <h3>Payload null-value contract</h3>
 * The decorator trusts the no-null-keys/no-null-values contract documented
 * on {@link org.apache.jackrabbit.oak.spi.audit.AuditEventListener#onEvents}.
 * Buggy event implementations that violate it may leak null values to
 * listeners — runtime validation is the event author's responsibility,
 * not the decorator's. Adding per-entry null checks here would impose
 * hot-path cost for what is an SPI-contract violation.
 */
final class CommitMetadataDecorator {

    private static final Logger log = LoggerFactory.getLogger(CommitMetadataDecorator.class);

    // Aliases for the reserved keys declared on the SPI. Single source of
    // truth: the strip path here and AuditEvent.isCommitAttested must agree
    // on the names, or a listener's attestation check silently diverges from
    // what Oak actually stamps.
    static final String KEY_SESSION_ID = AuditEvent.COMMIT_SESSION_ID;
    static final String KEY_USER_ID = AuditEvent.COMMIT_USER_ID;
    static final String KEY_TIMESTAMP = AuditEvent.COMMIT_TIMESTAMP;

    /**
     * One-shot latch for the strip WARN (package-private so tests can
     * reset it). First strip in the JVM logs WARN; subsequent strips log
     * DEBUG — an emitter that persistently sends reserved keys would
     * otherwise hand any bundle a WARN-flood vector.
     */
    static final AtomicBoolean STRIP_WARNED = new AtomicBoolean();

    private CommitMetadataDecorator() {
        // utility class
    }

    /**
     * Strips caller-supplied values for the three Oak-attested keys
     * ({@link #KEY_SESSION_ID}, {@link #KEY_USER_ID}, {@link #KEY_TIMESTAMP})
     * from a fire-and-forget payload. Applied by {@code BufferSink.dispatch}
     * before listener delivery so the presence of those keys in a dispatched
     * payload is a reliable Oak-attestation signal — the fire-and-forget
     * counterpart of the unconditional overwrite in {@link #decorate}.
     * Non-reserved {@code commit.*} keys and all other entries are forwarded
     * verbatim; events without any reserved key are returned unchanged (no
     * wrapping, no copy), preserving concrete event types for well-behaved
     * emitters.
     * <p>
     * <strong>TOCTOU.</strong> The payload is consulted exactly once and the
     * filtered snapshot is taken eagerly in the wrapper constructor —
     * {@link AuditEvent} is directly implementable, so deciding on one
     * {@code getPayload()} result and delivering another (lazy filtering, a
     * second consult) would let a hostile implementation pass the check
     * clean and hand listeners a forged map. A hostile implementation that
     * instead returns a clean map to THIS consult escapes wrapping, but the
     * snapshot listeners read is that same clean map; presenting forged keys
     * later is only possible where Oak dispatch is not mediating (direct
     * {@code listener.onEvents()} invocation) — the already-accepted
     * deployment-boundary bypass.
     * <p>
     * The wrapper delegates domain/type/timestamp rather than rebuilding via
     * {@link AuditEvent#of} — rebuilding would reset the capture timestamp
     * to wall-clock now.
     */
    static @NotNull AuditEvent stripReservedCommitKeys(@NotNull AuditEvent event) {
        Map<String, Object> payload = event.getPayload();
        if (!payload.containsKey(KEY_SESSION_ID)
                && !payload.containsKey(KEY_USER_ID)
                && !payload.containsKey(KEY_TIMESTAMP)) {
            return event;
        }
        logStrip(event, payload);
        return new StrippedAuditEvent(event, payload);
    }

    private static void logStrip(@NotNull AuditEvent event, @NotNull Map<String, Object> payload) {
        boolean firstTime = STRIP_WARNED.compareAndSet(false, true);
        if (!firstTime && !log.isDebugEnabled()) {
            return;
        }
        // Key NAMES only — the forged values are attacker-controlled and
        // must never reach the log.
        List<String> stripped = new ArrayList<>(3);
        if (payload.containsKey(KEY_SESSION_ID)) {
            stripped.add(KEY_SESSION_ID);
        }
        if (payload.containsKey(KEY_USER_ID)) {
            stripped.add(KEY_USER_ID);
        }
        if (payload.containsKey(KEY_TIMESTAMP)) {
            stripped.add(KEY_TIMESTAMP);
        }
        if (firstTime) {
            log.warn("Stripped reserved commit attestation key(s) {} from fire-and-forget audit event"
                    + " (domain '{}', type '{}'); these keys are Oak-attested and cannot be supplied"
                    + " by emitters. Further occurrences are logged at DEBUG.",
                    stripped, event.getDomain(), event.getType());
        } else {
            log.debug("Stripped reserved commit attestation key(s) {} from fire-and-forget audit event"
                    + " (domain '{}', type '{}').",
                    stripped, event.getDomain(), event.getType());
        }
    }

    static @NotNull List<AuditEvent> decorate(@NotNull List<AuditEvent> events,
                                              @NotNull CommitInfo info) {
        if (events.isEmpty()) {
            return Collections.emptyList();
        }
        String sessionId = info.getSessionId();
        String userId = info.getUserId();
        long timestamp = info.getDate();
        List<AuditEvent> out = new ArrayList<>(events.size());
        for (AuditEvent e : events) {
            out.add(new DecoratedAuditEvent(e, sessionId, userId, timestamp));
        }
        return out;
    }

    private static final class DecoratedAuditEvent implements AuditEvent {

        private final AuditEvent delegate;
        private final Map<String, Object> payload;

        DecoratedAuditEvent(@NotNull AuditEvent delegate,
                            @NotNull String sessionId,
                            @NotNull String userId,
                            long commitTimestamp) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            Map<String, Object> merged = new HashMap<>(delegate.getPayload());
            merged.put(KEY_SESSION_ID, sessionId);
            merged.put(KEY_USER_ID, userId);
            merged.put(KEY_TIMESTAMP, commitTimestamp);
            this.payload = Collections.unmodifiableMap(merged);
        }

        @Override public @NotNull AuditDomain getDomain() { return delegate.getDomain(); }
        @Override public @NotNull AuditType getType() { return delegate.getType(); }
        @Override public long getTimestamp() { return delegate.getTimestamp(); }
        @Override public @NotNull Map<String, Object> getPayload() { return payload; }
    }

    /**
     * Delegating wrapper whose payload is the eagerly-filtered snapshot of
     * the single {@code getPayload()} consult taken in
     * {@link #stripReservedCommitKeys} — see the TOCTOU note there. The
     * delegate's payload accessor is never consulted again.
     */
    private static final class StrippedAuditEvent implements AuditEvent {

        private final AuditEvent delegate;
        private final Map<String, Object> payload;

        StrippedAuditEvent(@NotNull AuditEvent delegate,
                           @NotNull Map<String, Object> consultedPayload) {
            this.delegate = Objects.requireNonNull(delegate, "delegate");
            Map<String, Object> filtered = new HashMap<>(consultedPayload);
            filtered.remove(KEY_SESSION_ID);
            filtered.remove(KEY_USER_ID);
            filtered.remove(KEY_TIMESTAMP);
            this.payload = Collections.unmodifiableMap(filtered);
        }

        @Override public @NotNull AuditDomain getDomain() { return delegate.getDomain(); }
        @Override public @NotNull AuditType getType() { return delegate.getType(); }
        @Override public long getTimestamp() { return delegate.getTimestamp(); }
        @Override public @NotNull Map<String, Object> getPayload() { return payload; }
    }
}
