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
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whiteboard-backed registry of {@link AuditEventListener} services.
 * <p>
 * Every predicate ({@link #hasAnyListener()}, {@link #hasListenerFor(String)})
 * and every retrieval ({@link #getListeners()}) goes through a live
 * {@code getServices()} call to the underlying {@link
 * org.apache.jackrabbit.oak.spi.whiteboard.Tracker Tracker} — no cached
 * domain set is held inside this registry. The {@code Tracker} SPI exposes
 * no listener add/remove notification, so any cache here would either be
 * stale on listener arrival/departure or require polling. The capture-site
 * fast path is kept cheap by relying on the underlying Whiteboard's
 * own dispatch: {@code DefaultWhiteboard.lookup(...)} returns the singleton
 * {@link java.util.Collections#emptyList()} when no services of the type
 * are registered, so {@link #hasAnyListener()} is constant-time and
 * allocation-free in the no-listener regime.
 * <p>
 * Listener invocation order is determined by
 * {@link AuditEventListener#getRank()} (higher first). The registry
 * applies a stable sort on every call to {@link #getListeners()} —
 * relying on the underlying {@code Whiteboard} is not portable:
 * {@code DefaultWhiteboard} does not honor OSGi {@code service.ranking},
 * only {@code OsgiWhiteboard} does.
 * <p>
 * <strong>Accessor isolation.</strong> {@code getDomain()} and
 * {@code getRank()} are listener code just like {@code onEvents()} — a
 * consumer bundle with a broken classpath throws {@link LinkageError} from
 * whichever method is called first. The per-listener isolation barrier
 * documented on {@link AuditEventListener} therefore covers the accessors
 * too: a listener whose accessor throws is skipped (logged at WARN once
 * per listener identity, then DEBUG) instead of propagating into capture
 * gates and dispatch loops, where the throw would either fail the
 * user-facing write operation or starve healthy peer listeners.
 */
final class WhiteboardAuditEventListenerRegistry
        extends AbstractServiceTracker<AuditEventListener> {

    private static final Logger log =
            LoggerFactory.getLogger(WhiteboardAuditEventListenerRegistry.class);

    /**
     * Stable comparator descending by the rank snapshotted into
     * {@link Ranked}; ties preserve {@code Whiteboard} insertion order
     * because {@link List#sort(Comparator)} is stable. Sorting operates on
     * the snapshot so a throwing {@code getRank()} can never surface from
     * inside the comparator.
     */
    private static final Comparator<Ranked> BY_RANK_DESC =
            Comparator.comparingInt((Ranked r) -> r.rank).reversed();

    /**
     * Identity keys of listeners already WARN-logged as broken — the skip
     * itself is per-call (a listener that stops throwing is picked up
     * again), only the WARN is latched. Bounded by the number of distinct
     * broken listener instances seen over the registry's lifetime.
     */
    private final Set<String> warnedBroken = ConcurrentHashMap.newKeySet();

    WhiteboardAuditEventListenerRegistry() {
        super(AuditEventListener.class);
    }

    /**
     * Returns the currently registered listeners, sorted by
     * {@link AuditEventListener#getRank()} descending (stable). Listeners
     * whose {@code getRank()} throws are skipped — see the accessor
     * isolation note in the class Javadoc.
     *
     * @return non-null immutable list of registered listeners (possibly
     *         empty).
     */
    @NotNull
    List<AuditEventListener> getListeners() {
        List<AuditEventListener> services = getServices();
        if (services.isEmpty()) {
            return List.of();
        }
        // Snapshot each rank under the per-listener guard BEFORE sorting.
        // Every listener is vetted regardless of count — a lone broken
        // listener must be skipped too, not returned through a fast path.
        List<Ranked> ranked = new ArrayList<>(services.size());
        for (AuditEventListener listener : services) {
            try {
                ranked.add(new Ranked(listener, listener.getRank()));
            } catch (Throwable t) {
                logBrokenListener(listener, "getRank()", t);
            }
        }
        ranked.sort(BY_RANK_DESC);
        List<AuditEventListener> out = new ArrayList<>(ranked.size());
        for (Ranked r : ranked) {
            out.add(r.listener);
        }
        return List.copyOf(out);
    }

    /**
     * Cheap predicate: is at least one listener registered (any domain)?
     * Read on the capture hot path.
     *
     * @return {@code true} when at least one listener is currently
     *         registered.
     */
    boolean hasAnyListener() {
        return !getServices().isEmpty();
    }

    /**
     * Cheap predicate: is at least one listener registered for the
     * supplied {@code domain}? Read on the capture hot path.
     * <p>
     * Linear scan of the live listener list. Capture sites typically face
     * a listener count in single digits, so iterating is competitive with
     * (and simpler than) a maintained domain set.
     *
     * @param domain the domain to check, non-null.
     * @return {@code true} when at least one listener is registered for
     *         the domain.
     */
    boolean hasListenerFor(@NotNull String domain) {
        for (AuditEventListener listener : getServices()) {
            try {
                if (domain.equals(listener.getDomain())) {
                    return true;
                }
            } catch (Throwable t) {
                logBrokenListener(listener, "getDomain()", t);
            }
        }
        return false;
    }

    /**
     * Logs a broken-accessor skip at WARN once per listener identity, then
     * DEBUG — capture gates poll {@link #hasListenerFor} on every audited
     * write, so an unconditional WARN would let one broken bundle flood the
     * log. Keyed by instance identity, not class: a re-registered
     * replacement instance warns again.
     */
    private void logBrokenListener(@NotNull AuditEventListener listener,
                                   @NotNull String accessor,
                                   @NotNull Throwable t) {
        String key = listener.getClass().getName() + "@"
                + Integer.toHexString(System.identityHashCode(listener));
        if (warnedBroken.add(key)) {
            log.warn("Skipping broken AuditEventListener {}: {} threw {}. The listener is"
                    + " skipped per call until it stops throwing; further occurrences are"
                    + " logged at DEBUG.",
                    key, accessor, t.getClass().getSimpleName(), t);
        } else {
            log.debug("Skipping broken AuditEventListener {}: {} threw {}.",
                    key, accessor, t.getClass().getSimpleName(), t);
        }
    }

    /**
     * Listener with its rank snapshotted under the accessor guard in
     * {@link #getListeners()}.
     */
    private static final class Ranked {
        final AuditEventListener listener;
        final int rank;

        Ranked(@NotNull AuditEventListener listener, int rank) {
            this.listener = listener;
            this.rank = rank;
        }
    }
}
