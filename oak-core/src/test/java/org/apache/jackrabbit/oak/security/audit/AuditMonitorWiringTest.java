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
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventListener;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.toggle.FeatureToggle;
import org.apache.jackrabbit.oak.spi.whiteboard.DefaultWhiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Verifies the {@link AuditMonitor} is actually invoked from the commit-attached
 * dispatch path and from the buffer's overflow branch, rather than merely being
 * callable ({@link AuditMonitorTest} covers the recording itself).
 * <p>
 * Uses a counting {@code AuditMonitor} subclass instead of a
 * {@code StatisticsProvider}: the assertions are about which pipeline branch
 * calls which method, so counting the calls is the direct observation.
 */
public class AuditMonitorWiringTest {

    private static final String SESSION_ID = "wiring-session";
    private static final String USER_ID = "alice";
    private static final NodeState ROOT = EmptyNodeState.EMPTY_NODE;
    private static final AuditDomain DOMAIN = AuditDomain.of("test.domain");
    private static final AuditType TYPE = AuditType.of("test.type");

    private DefaultWhiteboard whiteboard;
    private Feature featureToggle;
    private CountingMonitor monitor;
    private AuditBuffer buffer;
    private WhiteboardAuditEventListenerRegistry registry;
    private AuditDrainObserver observer;

    @Before
    public void setUp() {
        whiteboard = new DefaultWhiteboard();
        featureToggle = Feature.newFeature(AuditPipeline.FEATURE_TOGGLE_NAME, whiteboard);
        monitor = new CountingMonitor();
        buffer = new AuditBuffer(monitor);
        registry = new WhiteboardAuditEventListenerRegistry();
        registry.start(whiteboard);
        observer = new AuditDrainObserver(featureToggle, buffer, registry, monitor);
        setToggle(true);
    }

    @After
    public void tearDown() {
        if (featureToggle != null) {
            featureToggle.close();
        }
        if (registry != null) {
            registry.stop();
        }
        if (buffer != null) {
            buffer.clearAll();
        }
    }

    @Test
    public void dispatchedEventsAreCounted() {
        registerListener(DOMAIN);
        buffer.record(SESSION_ID, AuditEvent.of(DOMAIN, TYPE));
        buffer.record(SESSION_ID, AuditEvent.of(DOMAIN, TYPE));

        observer.contentChanged(ROOT, localCommit());

        assertEquals("both events counted once", 2, monitor.dispatched);
        assertEquals(1, monitor.durations);
        assertEquals(0, monitor.failures);
    }

    /**
     * Two listeners on one domain must not double the event count: the meter
     * answers "how many events flowed through", not "how many deliveries
     * happened".
     */
    @Test
    public void twoListenersOnOneDomainCountEventsOnce() {
        registerListener(DOMAIN);
        registerListener(DOMAIN);
        buffer.record(SESSION_ID, AuditEvent.of(DOMAIN, TYPE));

        observer.contentChanged(ROOT, localCommit());

        assertEquals("one event, counted once", 1, monitor.dispatched);
        assertEquals("but timed per listener", 2, monitor.durations);
    }

    /**
     * An event whose domain has no listener reached no consumer, so it must
     * not appear in the dispatch meter.
     */
    @Test
    public void eventWithNoListenerIsNotCounted() {
        registerListener(AuditDomain.of("other.domain"));
        buffer.record(SESSION_ID, AuditEvent.of(DOMAIN, TYPE));

        observer.contentChanged(ROOT, localCommit());

        assertEquals(0, monitor.dispatched);
        assertEquals(0, monitor.durations);
    }

    @Test
    public void toggleOffDispatchesAndCountsNothing() {
        setToggle(false);
        registerListener(DOMAIN);
        buffer.record(SESSION_ID, AuditEvent.of(DOMAIN, TYPE));

        observer.contentChanged(ROOT, localCommit());

        assertEquals(0, monitor.dispatched);
        assertEquals(0, monitor.durations);
    }

    /**
     * A throwing listener is counted as a failure, and still contributes a
     * duration: it burned commit-thread time before it threw.
     */
    @Test
    public void throwingListenerIsCountedAsFailure() {
        whiteboard.register(AuditEventListener.class, new AuditEventListener() {
            @Override
            public @NotNull AuditDomain getDomain() {
                return DOMAIN;
            }

            @Override
            public void onEvents(@NotNull List<AuditEvent> events) {
                throw new IllegalStateException("boom");
            }
        }, Map.of());
        buffer.record(SESSION_ID, AuditEvent.of(DOMAIN, TYPE));

        observer.contentChanged(ROOT, localCommit());

        assertEquals(1, monitor.failures);
        assertEquals("timed even though it threw", 1, monitor.durations);
        assertEquals("nothing was successfully consumed", 0, monitor.dispatched);
    }

    @Test
    public void eventsDroppedAtTheCapAreCounted() {
        for (int i = 0; i < AuditBuffer.MAX_EVENTS_PER_SESSION + 3; i++) {
            buffer.record(SESSION_ID, AuditEvent.of(DOMAIN, TYPE));
        }

        assertEquals("three events past the cap", 3, monitor.dropped);
    }

    //--------------------------------------------------------------< utils >---

    private static CommitInfo localCommit() {
        return new CommitInfo(SESSION_ID, USER_ID);
    }

    private void setToggle(boolean enabled) {
        Tracker<FeatureToggle> tracker = whiteboard.track(FeatureToggle.class);
        try {
            for (FeatureToggle ft : tracker.getServices()) {
                if (AuditPipeline.FEATURE_TOGGLE_NAME.equals(ft.getName())) {
                    ft.setEnabled(enabled);
                }
            }
        } finally {
            tracker.stop();
        }
    }

    private void registerListener(@NotNull AuditDomain domain) {
        whiteboard.register(AuditEventListener.class, new AuditEventListener() {
            private final List<AuditEvent> received = new ArrayList<>();

            @Override
            public @NotNull AuditDomain getDomain() {
                return domain;
            }

            @Override
            public void onEvents(@NotNull List<AuditEvent> events) {
                received.addAll(events);
            }
        }, Map.of());
    }

    /**
     * Counts calls per metric. Not a Mockito mock so the assertions read as
     * plain numbers, and so the class is safe to call from the commit thread
     * without stubbing.
     */
    private static final class CountingMonitor extends AuditMonitor {

        int dispatched;
        int dropped;
        int durations;
        int failures;

        CountingMonitor() {
            super(null);
        }

        @Override
        void eventsDispatched(@NotNull AuditDomain domain, int count) {
            dispatched += count;
        }

        @Override
        void eventDropped(@NotNull AuditDomain domain) {
            dropped++;
        }

        @Override
        void listenerDuration(@NotNull Class<?> listener, long durationNanos) {
            durations++;
        }

        @Override
        void listenerFailed(@NotNull Class<?> listener) {
            failures++;
        }
    }
}
