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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Static façade used by {@code MutableRoot} (in {@code oak-core}) to notify
 * the audit module of session-scoped lifecycle events that must purge any
 * staged audit events:
 * <ul>
 *     <li>{@link #onCommitFailed(String)} — the surrounding
 *     {@code Root.commit()} threw before audit could dispatch.</li>
 *     <li>{@link #onRefresh(String)} — the session called
 *     {@code Root.refresh()}, discarding pending transient changes. Note:
 *     {@code Root.rebase()} does NOT trigger this callback — rebase
 *     preserves transient changes (they are replayed on the new base), so
 *     the audit events staged alongside them must survive too.</li>
 * </ul>
 * When no audit module is deployed, the installed listener is a NOOP and
 * each call costs a single volatile read plus a virtual method dispatch.
 * <p>
 * The audit module installs its buffer on activation
 * ({@code install(buffer)}) and clears it on deactivation
 * ({@code install(null)}).
 */
public final class AuditBufferLifecycle {

    /**
     * Lifecycle listener contract implemented by the audit module's
     * per-session buffer.
     */
    public interface Listener {

        /**
         * Invoked when {@code Root.commit()} threw before audit dispatch
         * was reached. The implementation must drop any events staged
         * for the given session.
         *
         * @param sessionId the session id (as returned by
         *                  {@code ContentSession.toString()}), non-null.
         */
        void onCommitFailed(@NotNull String sessionId);

        /**
         * Invoked when {@code Root.refresh()} is called, discarding the
         * session's pending transient changes. The implementation must drop
         * any events staged for the given session.
         * <p>
         * <strong>Not</strong> invoked by {@code Root.rebase()}: rebase
         * preserves transient changes, so the audit events staged alongside
         * them must survive the rebase and be dispatched on the eventual
         * commit.
         *
         * @param sessionId the session id, non-null.
         */
        void onRefresh(@NotNull String sessionId);
    }

    private static final Listener NOOP = new Listener() {
        @Override
        public void onCommitFailed(@NotNull String sessionId) {
            // no audit module deployed.
        }

        @Override
        public void onRefresh(@NotNull String sessionId) {
            // no audit module deployed.
        }
    };

    private static volatile Listener listener = NOOP;

    private AuditBufferLifecycle() {
        // utility class
    }

    /**
     * Installs the active listener. Called by the audit module on
     * activation. Passing {@code null} resets to the NOOP listener.
     * <p>
     * <strong>Bundle deployment is the security boundary.</strong> An attacker
     * with bundle-deploy capability can intercept (by installing a custom
     * Listener) or silently disable (by installing the NOOP via
     * {@code install(null)}) the buffer-lifecycle wiring. Protecting against
     * this requires OSGi-level controls (bundle signing, deployment policy);
     * SPI-level access controls cannot help once a hostile bundle is already
     * deployed. Embedded (non-OSGi) deployments inherit the JVM classpath as
     * the boundary instead.
     *
     * @param newListener the listener to install, or {@code null}.
     */
    public static void install(@Nullable Listener newListener) {
        listener = (newListener != null) ? newListener : NOOP;
    }

    /**
     * Notifies the installed listener that the commit failed. Safe to
     * call when no module is deployed (NOOP).
     *
     * @param sessionId the session id, non-null.
     */
    public static void onCommitFailed(@NotNull String sessionId) {
        listener.onCommitFailed(sessionId);
    }

    /**
     * Notifies the installed listener that the session refreshed or
     * rebased. Safe to call when no module is deployed (NOOP).
     *
     * @param sessionId the session id, non-null.
     */
    public static void onRefresh(@NotNull String sessionId) {
        listener.onRefresh(sessionId);
    }
}
