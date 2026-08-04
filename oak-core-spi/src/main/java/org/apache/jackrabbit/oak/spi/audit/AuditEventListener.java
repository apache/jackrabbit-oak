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

import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.osgi.annotation.versioning.ConsumerType;

/**
 * Receives a burst of audit events for the listener's domain. One method
 * for both commit-attached events (drained after a successful
 * {@code Root.commit()}) and fire-and-forget events (dispatched
 * immediately via {@link AuditEventEmitter#emit(AuditEvent)}).
 * <p>
 * Implementations must be non-blocking. Invocation is synchronous on the
 * dispatching thread; expensive work (I/O, fan-out, persistence) belongs
 * in an async wrapper provided by the consumer.
 * <p>
 * Exceptions and Errors thrown from {@link #onEvents} — or from the
 * {@link #getDomain()} / {@link #getRank()} accessors consulted during
 * routing — are caught, logged at {@code WARN}, and swallowed by the
 * dispatcher; they never propagate back to the dispatching thread. The
 * dispatcher swallows {@link Throwable} broadly to ensure that one
 * misconfigured listener (e.g., a {@link LinkageError} from a missing
 * transitive dependency) cannot prevent other listeners from receiving
 * events or abort the surrounding commit. A listener whose accessor
 * throws is skipped for that dispatch (it receives nothing) and is
 * picked up again once the accessor stops throwing.
 * <p>
 * Listener invocation order is determined by {@link #getRank()} (higher
 * value first). The dispatcher applies a stable sort, so listeners with
 * equal rank are invoked in {@code Whiteboard} order.
 *
 * <h3>Trust model</h3>
 * Events delivered through this method may originate from either:
 * <ul>
 *   <li>Oak-internal capture sites tied to a successful
 *       {@code Root.commit()}. Such events carry Oak's commit attestation:
 *       {@link AuditEvent#COMMIT_SESSION_ID},
 *       {@link AuditEvent#COMMIT_USER_ID} and
 *       {@link AuditEvent#COMMIT_TIMESTAMP}. The user id is
 *       {@code "oak:unknown"} for system commits and listeners
 *       <strong>MUST NOT</strong> attempt to resolve it to a real user
 *       identity.</li>
 *   <li>Any bundle calling {@link AuditEventEmitter#emit(AuditEvent)}.
 *       The accuracy of such events is the emitting bundle's responsibility;
 *       Oak does not verify them. They cannot carry the three reserved
 *       attestation keys — Oak strips caller-supplied values for them
 *       before delivery. Other {@code oak.commit.*}-prefixed keys are
 *       forwarded verbatim and are untrusted.</li>
 * </ul>
 * Consumers that need to distinguish between the two sources should call
 * {@link AuditEvent#isCommitAttested(AuditEvent)} — the normative statement
 * and the boundaries of this attestation are documented on
 * {@link AuditEvent#getPayload()}.
 */
@ConsumerType
public interface AuditEventListener {

    /**
     * Returns the domain this listener is interested in. The registry
     * queries {@code getDomain()} on every dispatch (no cache), so
     * implementations must return a stable value across the listener's
     * lifetime — if the value changes between dispatches the listener
     * may silently start or stop receiving events. If it throws, the
     * listener is skipped for that dispatch — see the Throwable-isolation
     * note in the class Javadoc.
     *
     * @return non-null domain.
     */
    @NotNull
    AuditDomain getDomain();

    /**
     * Returns the dispatch rank for this listener — higher value is
     * invoked first. The default implementation returns {@code 0}.
     *
     * @return rank value.
     */
    default int getRank() {
        return 0;
    }

    /**
     * Invoked when one or more events for this listener's domain are
     * dispatched. Events arrive in capture order (earliest first).
     *
     * @param events the non-empty list of events for this listener's
     *               domain. Each event's payload map values are never
     *               null; optional fields are absent from the map.
     */
    void onEvents(@NotNull List<AuditEvent> events);
}
