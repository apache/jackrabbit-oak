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
package org.apache.jackrabbit.oak.spi.audit;

import java.util.Collections;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.audit.impl.AuditEventImpl;
import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ProviderType;

/**
 * Structured audit event. Implementations are expected to be immutable
 * value types.
 * <p>
 * Events may originate from two pipelines:
 * <ul>
 *   <li>Oak-internal capture sites tied to a successful
 *       {@code Root.commit()}. The commit-attached drain step decorates
 *       payload with the three reserved attestation entries before
 *       dispatch; use {@link #isCommitAttested(AuditEvent)} to test for
 *       them rather than checking the keys by hand.</li>
 *   <li>Any bundle calling {@link AuditEventEmitter#emit(AuditEvent)}.
 *       Such events carry the payload provided by the caller, except that
 *       Oak strips caller-supplied values for the three reserved
 *       attestation keys before dispatch — see the trust contract on
 *       {@link #getPayload()}.</li>
 * </ul>
 * The {@link #getDomain()} value selects the listeners that receive this
 * event.
 * <p>
 * Most callers do not implement this interface directly: use the static
 * factory {@link #of(AuditDomain, AuditType, Map)} (or the no-payload
 * overload {@link #of(AuditDomain, AuditType)}) to construct an immutable
 * event with the current wall-clock timestamp. The unexported
 * {@code spi.audit.impl.AuditEventImpl} backs these factories.
 */
@ProviderType
public interface AuditEvent {

    /**
     * Payload key carrying the id of the session whose commit produced this
     * event. One of the three reserved attestation keys; see the trust
     * contract on {@link #getPayload()} and {@link #isCommitAttested}.
     */
    String COMMIT_SESSION_ID = "oak.commit.sessionId";

    /**
     * Payload key carrying the user id of the commit that produced this
     * event. {@code "oak:unknown"} for system commits, which listeners
     * <strong>MUST NOT</strong> try to resolve to a real identity.
     */
    String COMMIT_USER_ID = "oak.commit.userId";

    /**
     * Payload key carrying the commit timestamp (millis since epoch), i.e.
     * when the change became durable — as opposed to
     * {@link #getTimestamp()}, which is when the event was captured.
     */
    String COMMIT_TIMESTAMP = "oak.commit.timestamp";

    /**
     * Returns the domain that owns this event. Listeners are domain-scoped:
     * an {@link AuditEventListener} only receives events whose
     * {@code getDomain()} matches its own {@link AuditEventListener#getDomain()}.
     *
     * @return non-null domain (e.g. {@code "oak.security"}).
     */
    @NotNull
    AuditDomain getDomain();

    /**
     * Returns the event type within the domain. Types are stable across
     * releases for any given domain.
     *
     * @return non-null type.
     */
    @NotNull
    AuditType getType();

    /**
     * Returns the wall-clock timestamp (millis since epoch) captured at
     * the API call site when the event was constructed — i.e. the
     * <em>capture</em> timestamp.
     * <p>
     * For commit-attached events this can differ from the
     * <em>commit</em> timestamp ({@code commit.timestamp} in
     * {@link #getPayload()}): the capture timestamp is taken when the
     * Oak API call ran; the commit timestamp is taken when the surrounding
     * {@code Root.commit()} actually merged. The two can diverge when the
     * surrounding operation takes a long time between capture and commit.
     * Listeners that want "when did the change become visible?" should
     * read {@code commit.timestamp}; listeners that want "when did the
     * API call ran?" should read this value.
     *
     * @return event capture timestamp in milliseconds since epoch.
     */
    long getTimestamp();

    /**
     * Returns the structured payload for this event. The default
     * implementation returns an empty map; concrete event types override
     * this to expose typed accessors and include their fields here.
     * <p>
     * For commit-attached events, Oak's drain path adds entries with the
     * keys {@code oak.commit.sessionId}, {@code oak.commit.userId}, and
     * {@code oak.commit.timestamp} when the buffer is drained on commit
     * success. Oak does not <em>add</em> these entries on the fire-and-forget
     * path (see the trust contract below).
     * <p>
     * <strong>Trust contract</strong> (normative — other audit SPI and
     * implementation docs defer to this paragraph). For events delivered
     * through Oak dispatch, the three reserved keys
     * {@code oak.commit.sessionId}, {@code oak.commit.userId} and
     * {@code oak.commit.timestamp} are Oak-attested:
     * <ul>
     *   <li>On the <em>commit-attached</em> path Oak <em>unconditionally
     *       overwrites</em> the three keys with the values from
     *       {@code CommitInfo} at drain time (via
     *       {@code CommitMetadataDecorator}).</li>
     *   <li>On the <em>fire-and-forget</em> path
     *       ({@link AuditEventEmitter#emit(AuditEvent)} /
     *       {@code AuditDispatch.dispatch}) Oak <em>strips</em> caller-supplied
     *       values for the same three keys before delivery.</li>
     * </ul>
     * A listener may therefore treat the presence of the three keys in a
     * dispatched payload as "commit-attached event, values supplied by
     * Oak". Use {@link #isCommitAttested(AuditEvent)} for that test rather
     * than reading the keys directly. Every other entry — including other
     * {@code oak.commit.*}-prefixed keys — is forwarded verbatim from the
     * caller-supplied payload on both paths and is untrusted: anchor trust
     * on the three reserved keys, never on the prefix in general.
     * <p>
     * Boundaries of the attestation:
     * <ul>
     *   <li><em>Oak dispatch only.</em> Code that invokes
     *       {@code AuditEventListener.onEvents(...)} directly bypasses both
     *       the overwrite and the strip; constraining which bundles can do
     *       that is a deployment-level control.</li>
     *   <li><em>Attestation does not survive re-emission.</em> A forwarder
     *       that copies a commit-attached payload into a new event and
     *       re-emits it via {@code emit(...)} gets the three keys stripped
     *       again — the re-emitted event is no longer the Oak-attested
     *       original.</li>
     *   <li><em>No redaction.</em> Apart from the three reserved keys on the
     *       fire-and-forget path, the payload is never filtered at dispatch;
     *       the producer-side hygiene rules on {@link #of} still apply.</li>
     * </ul>
     *
     * @return non-null, immutable payload map.
     */
    @NotNull
    default Map<String, Object> getPayload() {
        return Collections.emptyMap();
    }

    /**
     * Creates an immutable audit event with the supplied payload and the
     * current wall-clock timestamp. The payload Map is defensively copied
     * via {@link Map#copyOf}; the caller's Map reference is decoupled
     * from the event.
     *
     * @param domain  non-blank domain identifier.
     * @param type    non-blank event type identifier within {@code domain}.
     * @param payload immutable, non-null payload Map. Values are stored
     *                by reference — see the shallow-copy note below.
     * @return non-null event instance.
     * @throws IllegalArgumentException if {@code domain} or {@code type} is blank.
     *
     * @apiNote
     * <p><strong>Shallow-copy semantics.</strong> {@link Map#copyOf} decouples
     * the caller's Map reference but does NOT clone payload <em>values</em>.
     * Callers MUST pass immutable values (Strings, boxed primitives,
     * {@link java.util.List#copyOf(java.util.Collection) List.copyOf} /
     * {@link java.util.Set#copyOf(java.util.Collection) Set.copyOf} results).
     * Mutating a payload value after passing it to {@code of(...)} produces
     * undefined dispatch behavior on the commit-attached path, where capture
     * and dispatch are separated by the surrounding commit.
     *
     * <p><strong>Security warning.</strong> The {@code payload} map values
     * are forwarded verbatim to listeners. Callers MUST NOT pass:
     * <ul>
     *   <li>Any {@link javax.jcr.Credentials} subtype.</li>
     *   <li>The value of a {@code rep:password} or {@code rep:credentials}
     *       property.</li>
     *   <li>Any token-bearing object (e.g. {@code TokenInfo},
     *       {@code TokenCredentials}, raw token strings).</li>
     *   <li>Any node, property, or value that could transitively expose such
     *       data (e.g. a {@code Node} pointing at a {@code rep:User} subtree).</li>
     * </ul>
     * Pass user identifiers, paths, timestamps, and other non-sensitive
     * scalars only. <strong>Oak does not redact or filter the payload at
     * dispatch.</strong> See {@code oak-doc/src/site/markdown/security/audit-design.md}
     * for the producer-side responsibility under the open trust model.
     */
    @NotNull
    static AuditEvent of(@NotNull AuditDomain domain,
                         @NotNull AuditType type,
                         @NotNull Map<String, Object> payload) {
        return new AuditEventImpl(domain, type, System.currentTimeMillis(), Map.copyOf(payload));
    }

    /**
     * Convenience overload for events with no payload.
     *
     * @param domain the event domain.
     * @param type   the event type within {@code domain}.
     * @return non-null event instance with an empty payload.
     */
    @NotNull
    static AuditEvent of(@NotNull AuditDomain domain, @NotNull AuditType type) {
        return of(domain, type, Map.of());
    }

    /**
     * Returns {@code true} when the event carries Oak's commit attestation,
     * i.e. when it reached the listener through the commit-attached path
     * after a successful {@code Root.commit()}.
     * <p>
     * This is the supported way to make that distinction. The attestation
     * is carried by three reserved payload keys, but their names are an
     * implementation detail: read them directly and the check silently
     * breaks if they are ever renamed. The semantics — what the attestation
     * does and does not guarantee — are documented on {@link #getPayload()}.
     *
     * @param event the event to test, non-null.
     * @return {@code true} when all three reserved attestation entries are
     *         present.
     */
    static boolean isCommitAttested(@NotNull AuditEvent event) {
        Map<String, Object> payload = event.getPayload();
        return payload.containsKey(COMMIT_SESSION_ID)
                && payload.containsKey(COMMIT_USER_ID)
                && payload.containsKey(COMMIT_TIMESTAMP);
    }
}
