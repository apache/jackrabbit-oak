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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.audit.AuditBufferLifecycle;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-thread, per-session staging area for audit events. Events captured
 * via {@link org.apache.jackrabbit.oak.spi.audit.AuditEvents#record}
 * are appended to a session-scoped buffer held in a {@link ThreadLocal};
 * the buffer is allocated lazily on first {@code record} and is removed
 * when {@link #drain(String)} is called (i.e. on commit snapshot) or when
 * a lifecycle event clears it.
 * <p>
 * The buffer also implements {@link AuditBufferLifecycle.Listener}; it is
 * installed via {@code AuditBufferLifecycle.install(this)} by
 * {@link AuditPipeline} on activation.
 * <p>
 * <strong>Soft per-session cap.</strong> A single session must not be able
 * to accumulate an unbounded number of staged events (e.g. a very large
 * transaction, or a session that records without ever committing/refreshing).
 * Once {@value #MAX_EVENTS_PER_SESSION} events are staged for a session,
 * further events are dropped and a single WARN is logged for that session
 * (not one per dropped event). The drop is bounded and self-healing: the
 * next {@link #drain(String)} / {@link #onRefresh(String)} /
 * {@link #onCommitFailed(String)} clears the session slot and re-arms the
 * warning.
 * <p>
 * Threading contract: capture, drain and lifecycle calls all happen on
 * the session's caller thread. Cross-thread invocation is not supported
 * — sessions are not thread-safe in Oak. Because the staging area is a
 * {@link ThreadLocal}, a drain issued from a thread other than the one
 * that captured simply sees an empty buffer (returns {@code null}); it
 * never observes or removes another thread's events.
 */
final class AuditBuffer implements AuditBufferLifecycle.Listener {

    private static final Logger log = LoggerFactory.getLogger(AuditBuffer.class);

    /**
     * Soft upper bound on the number of events staged for a single session
     * on a single thread. Events beyond this are dropped (with a single
     * WARN per session) to bound the per-thread memory a runaway session
     * can pin.
     */
    static final int MAX_EVENTS_PER_SESSION = 10_000;

    /**
     * Thread-local map keyed by {@code sessionId}
     * ({@code ContentSession.toString()}). The inner {@link SessionBuffer}
     * is created lazily on the first {@link #record(String, AuditEvent)}
     * for the given session, kept alive across multiple captures, and
     * removed by {@link #drain(String)} / {@link #onCommitFailed(String)} /
     * {@link #onRefresh(String)}.
     * <p>
     * The outer map starts {@code null} (a single {@link ThreadLocal}
     * lookup yielding {@code null}) and is allocated on first capture
     * for the thread.
     */
    private final ThreadLocal<Map<String, SessionBuffer>> tl = new ThreadLocal<>();

    /**
     * Appends {@code event} to the session's per-thread buffer,
     * allocating the inner buffer lazily. Drops the event (logging a
     * single WARN per session) once the session has reached
     * {@link #MAX_EVENTS_PER_SESSION} staged events.
     *
     * @param sessionId session id, non-null.
     * @param event     event to record, non-null.
     */
    void record(@NotNull String sessionId, @NotNull AuditEvent event) {
        Map<String, SessionBuffer> bySession = tl.get();
        if (bySession == null) {
            bySession = new HashMap<>(4);
            tl.set(bySession);
        }
        SessionBuffer sb = bySession.computeIfAbsent(sessionId, k -> new SessionBuffer());
        // Soft per-session cap. Overflow drops the LATEST events with a
        // WARN-once (no per-event log spam). Deferred follow-up: surface the
        // truncation IN-BAND (e.g. an audit.system meta-domain overflow event
        // carrying a dropped count) so a consumer sees the gap, not just a log
        // line. Threat is narrow — an attacker would need write access AND a
        // single transaction emitting >MAX_EVENTS_PER_SESSION audit events to
        // push a later (sensitive) event past the cap; bounded and self-healing
        // (the slot re-arms on the next drain/refresh).
        if (sb.events.size() >= MAX_EVENTS_PER_SESSION) {
            if (!sb.overflowWarned) {
                sb.overflowWarned = true;
                log.warn("Audit buffer for session {} reached the cap of {} staged events; " +
                        "dropping further events for this session until the next commit/refresh. " +
                        "This usually indicates a very large transaction or a session that records " +
                        "audit events without committing.", sessionId, MAX_EVENTS_PER_SESSION);
            }
            return;
        }
        sb.events.add(event);
    }

    /**
     * Test-only inspector. Returns a <strong>defensive copy</strong> of the
     * events staged for {@code sessionId} <strong>without</strong> removing
     * them. Mutating the returned list does not affect the buffer. Production
     * drain goes through {@link #drain(String)}.
     *
     * @param sessionId session id, non-null.
     * @return an immutable copy of the staged events, or {@code null} when
     *         nothing was staged for the session on the current thread.
     */
    @Nullable
    List<AuditEvent> peek(@NotNull String sessionId) {
        Map<String, SessionBuffer> bySession = tl.get();
        if (bySession == null) {
            return null;
        }
        SessionBuffer sb = bySession.get(sessionId);
        return (sb == null) ? null : List.copyOf(sb.events);
    }

    /**
     * Detaches and returns the staged events for {@code sessionId},
     * leaving the buffer empty for that session.
     *
     * @param sessionId session id, non-null.
     * @return the staged events, or {@code null} when nothing was
     *         staged for the session on the current thread.
     */
    @Nullable
    List<AuditEvent> drain(@NotNull String sessionId) {
        Map<String, SessionBuffer> bySession = tl.get();
        if (bySession == null) {
            return null;
        }
        SessionBuffer drained = bySession.remove(sessionId);
        if (bySession.isEmpty()) {
            tl.remove();
        }
        return (drained == null) ? null : drained.events;
    }

    /**
     * Drops all staged events for the <strong>current thread</strong>.
     * Called by {@link AuditPipeline#deactivate} so the
     * deactivator thread leaves no residue.
     * <p>
     * Note: this cannot reach across thread boundaries. ThreadLocal
     * entries on other threads remain until their owning thread next
     * calls {@link #record(String, AuditEvent)}, {@link #drain(String)},
     * or the {@code AuditBufferLifecycle} listener is invoked. The
     * resulting residual leak is bounded by
     * {@code worker-pool × in-flight sessions}.
     */
    void clearAll() {
        tl.remove();
    }

    //----------------------------------------< AuditBufferLifecycle.Listener >---
    @Override
    public void onCommitFailed(@NotNull String sessionId) {
        drain(sessionId);
    }

    @Override
    public void onRefresh(@NotNull String sessionId) {
        drain(sessionId);
    }

    /**
     * Per-session staging holder: the captured events plus a one-shot
     * flag that ensures the overflow WARN is logged at most once per
     * session slot (re-armed when the slot is recreated after a drain).
     */
    private static final class SessionBuffer {
        final List<AuditEvent> events = new ArrayList<>(4);
        boolean overflowWarned;
    }
}
