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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.metric.util.StatsProviderUtil;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Metrics for the audit pipeline. Records how many events are dispatched per
 * domain, how long each listener spends in
 * {@link org.apache.jackrabbit.oak.spi.audit.AuditEventListener#onEvents},
 * how often a listener fails, and how many events are dropped at the
 * per-session buffer cap.
 * <p>
 * The listener timer is the operationally interesting one: listeners run
 * synchronously on the commit thread, so time spent in {@code onEvents} is
 * added to commit latency for the writing session. The dropped-event meter is
 * the one that matters for a compliance trail — a non-zero value means a
 * persisted write left no audit event behind.
 * <p>
 * Metric names carry {@code ;domain=<name>} / {@code ;listener=<class>}
 * label suffixes via {@link StatsProviderUtil}, the convention Prometheus and
 * similar systems split back into a name plus labels. Domains and listener
 * classes are both unbounded in principle, so the per-label {@code Meter} and
 * {@code Timer} handles are cached in a {@link ConcurrentHashMap} rather than
 * resolved per event: {@code getMeter(...)} is a lookup on the provider's own
 * registry and not free.
 * <p>
 * {@link #NOOP} is used when no {@link StatisticsProvider} is bound, which is
 * the common case for embedded callers and tests. Every record method on it is
 * an empty call, so callers never null-check.
 */
class AuditMonitor {

    /**
     * Monitor that records nothing. Installed when no
     * {@link StatisticsProvider} is available.
     */
    static final AuditMonitor NOOP = new AuditMonitor(null);

    private static final String EVENTS = "security.audit.events";
    private static final String EVENTS_DROPPED = "security.audit.events.dropped";
    private static final String LISTENER_DURATION = "security.audit.listener.duration";
    private static final String LISTENER_FAILURES = "security.audit.listener.failures";

    private static final String LABEL_DOMAIN = "domain";
    private static final String LABEL_LISTENER = "listener";

    /**
     * {@code null} for {@link #NOOP}; every record method short-circuits on it.
     */
    private final StatsProviderUtil stats;

    private final Map<String, MeterStats> eventMeters = new ConcurrentHashMap<>();
    private final Map<String, MeterStats> droppedMeters = new ConcurrentHashMap<>();
    private final Map<String, TimerStats> listenerTimers = new ConcurrentHashMap<>();
    private final Map<String, MeterStats> listenerFailureMeters = new ConcurrentHashMap<>();

    /**
     * @param statisticsProvider provider to register metrics on, or
     *                           {@code null} to record nothing.
     */
    AuditMonitor(@Nullable StatisticsProvider statisticsProvider) {
        this.stats = (statisticsProvider == null)
                ? null
                : new StatsProviderUtil(statisticsProvider);
    }

    /**
     * Records that {@code count} events were dispatched to at least one
     * listener in {@code domain}. Not called for events discarded at the
     * toggle or the listener gate: those never reach a consumer, so counting
     * them would misreport the dispatch rate.
     *
     * @param domain the events' domain, non-null.
     * @param count  number of events dispatched, positive.
     */
    void eventsDispatched(@NotNull AuditDomain domain, int count) {
        if (stats == null) {
            return;
        }
        eventMeters.computeIfAbsent(domain.name(),
                        name -> stats.getMeterStats().apply(EVENTS, Map.of(LABEL_DOMAIN, name)))
                .mark(count);
    }

    /**
     * Records that one event was dropped because the capturing session had
     * reached the per-session buffer cap. A non-zero rate here is a gap in
     * the audit trail, not just a capacity signal.
     *
     * @param domain the dropped event's domain, non-null.
     */
    void eventDropped(@NotNull AuditDomain domain) {
        if (stats == null) {
            return;
        }
        droppedMeters.computeIfAbsent(domain.name(),
                        name -> stats.getMeterStats().apply(EVENTS_DROPPED, Map.of(LABEL_DOMAIN, name)))
                .mark();
    }

    /**
     * Records how long a listener spent handling a batch.
     *
     * @param listener      the listener class, non-null.
     * @param durationNanos elapsed wall-clock time in nanoseconds.
     */
    void listenerDuration(@NotNull Class<?> listener, long durationNanos) {
        if (stats == null) {
            return;
        }
        listenerTimers.computeIfAbsent(listener.getName(),
                        name -> stats.getTimerStats().apply(LISTENER_DURATION, Map.of(LABEL_LISTENER, name)))
                .update(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Records that a listener threw. Counted separately from the duration
     * timer so a consistently failing listener is visible even when it fails
     * fast.
     *
     * @param listener the listener class, non-null.
     */
    void listenerFailed(@NotNull Class<?> listener) {
        if (stats == null) {
            return;
        }
        listenerFailureMeters.computeIfAbsent(listener.getName(),
                        name -> stats.getMeterStats().apply(LISTENER_FAILURES, Map.of(LABEL_LISTENER, name)))
                .mark();
    }
}
