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

import org.apache.jackrabbit.oak.api.Root;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Static façade used by Oak-internal code (commit-attached capture sites,
 * the OSGi {@link AuditEventEmitter} impl) to talk to the audit pipeline.
 * The façade is wired to a {@link Sink} on activation of the audit module;
 * when no module is deployed (or the feature toggle is off) it short-circuits
 * with zero allocation.
 */
public final class AuditDispatch {

    /**
     * Sink contract implemented by the audit module. Installed via
     * {@link #install(Sink)} on activation of the audit module; on
     * deactivation the sink is reset to a NOOP.
     */
    public interface Sink {

        /**
         * Returns {@code true} when the feature toggle is enabled and at
         * least one listener is registered (for any domain). Cheapest gate.
         */
        boolean isEnabled();

        /**
         * Returns {@code true} when the feature toggle is enabled AND at
         * least one listener is registered for the given domain. Used to
         * avoid event allocation when no consumer cares about the domain.
         */
        boolean isEnabledFor(@NotNull AuditDomain domain);

        /**
         * Commit-attached path. Buffers the event against the session
         * backing the supplied {@link Root}. Dispatched on commit success;
         * discarded on commit failure.
         */
        void record(@NotNull Root root, @NotNull AuditEvent event);

        /**
         * Fire-and-forget path. Dispatches the event synchronously on the
         * calling thread to all listeners registered for its domain. Not
         * buffered; not tied to any commit. Caller-supplied values for the
         * three reserved {@code commit.*} attestation keys are stripped
         * before delivery — see the trust contract on
         * {@link AuditEvent#getPayload()}.
         */
        void dispatch(@NotNull AuditEvent event);
    }

    private static final Sink NOOP = new Sink() {
        @Override public boolean isEnabled() { return false; }
        @Override public boolean isEnabledFor(@NotNull AuditDomain domain) { return false; }
        @Override public void record(@NotNull Root root, @NotNull AuditEvent event) { }
        @Override public void dispatch(@NotNull AuditEvent event) { }
    };

    private static volatile Sink sink = NOOP;

    private AuditDispatch() {
        // utility class
    }

    /**
     * Installs the active sink. Called by the audit module on activation.
     * Passing {@code null} resets the façade to the NOOP sink.
     * <p>
     * <strong>Bundle deployment is the security boundary.</strong> An attacker
     * with bundle-deploy capability can intercept (by installing a custom
     * Sink that exfiltrates events) or silently disable (by installing the
     * NOOP via {@code install(null)}) the audit pipeline. Protecting against
     * this requires OSGi-level controls (bundle signing, deployment policy);
     * SPI-level access controls cannot help once a hostile bundle is already
     * deployed. Embedded (non-OSGi) deployments inherit the JVM classpath as
     * the boundary instead.
     */
    public static void install(@Nullable Sink newSink) {
        sink = (newSink != null) ? newSink : NOOP;
    }

    public static boolean isEnabled() {
        return sink.isEnabled();
    }

    public static boolean isEnabledFor(@NotNull AuditDomain domain) {
        return sink.isEnabledFor(domain);
    }

    public static void record(@NotNull Root root, @NotNull AuditEvent event) {
        sink.record(root, event);
    }

    public static void dispatch(@NotNull AuditEvent event) {
        sink.dispatch(event);
    }
}
