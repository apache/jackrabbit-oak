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

import org.osgi.annotation.versioning.ProviderType;

/**
 * Audit pipeline configuration handle. Exposes pipeline-level state
 * ({@link #isActive()}) so admin tooling, monitoring agents, and other
 * Oak components can probe the pipeline without depending on its
 * implementation class.
 *
 * <p><strong>Wiring.</strong> Audit is a top-level Oak concern, not a
 * {@code SecurityConfiguration}. Implementations are registered on the
 * {@link org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard} (and, in
 * OSGi deployments, as an OSGi service of this type). The pipeline
 * subscribes to the root NodeStore's
 * {@link org.apache.jackrabbit.oak.spi.commit.Observable} for commit
 * notifications; it contributes no commit hooks.
 *
 * <p><strong>Cardinality:</strong> unary optional. Multiple implementations
 * are not supported — the {@link AuditBufferLifecycle} is a singleton install
 * and multiple observers on the same root NodeStore would each produce a
 * duplicate dispatch. Multiplexing belongs at the listener layer
 * ({@link AuditEventListener}), not at the configuration layer.
 *
 * <p>When no implementation is bound, callers either see no service
 * (Whiteboard / OSGi lookups return empty) or the {@link #NOOP} constant
 * if they want a guaranteed-non-null handle. {@code NOOP.isActive()}
 * returns {@code false}.
 */
@ProviderType
public interface AuditConfiguration {

    /**
     * Returns {@code true} when the audit pipeline is currently active —
     * i.e., the audit feature toggle is enabled AND at least one
     * {@link AuditEventListener} is registered on the Whiteboard. The
     * two predicates AND together so a deployed-but-unused pipeline still
     * reports {@code false}, matching the no-allocation semantics
     * documented at {@link AuditEvents#isEnabled()}.
     *
     * <p>Equivalent in semantics to {@code AuditEvents.isEnabled()}, but
     * reachable via the typed handle. Drift-prevention: both paths read
     * through the volatile {@code AuditEvents.sink} (single source of
     * truth). Any future divergence MUST be documented explicitly in
     * both Javadocs.
     *
     * @return {@code true} when the toggle is enabled and at least one
     *         listener is registered; {@code false} otherwise.
     */
    boolean isActive();

    /**
     * NOOP default. Reports {@link #isActive()} as {@code false}.
     */
    AuditConfiguration NOOP = new Noop();

    /**
     * NOOP implementation. Package-private by design — consumers refer
     * to the {@link #NOOP} constant.
     */
    final class Noop implements AuditConfiguration {

        @Override
        public boolean isActive() {
            return false;
        }
    }
}
