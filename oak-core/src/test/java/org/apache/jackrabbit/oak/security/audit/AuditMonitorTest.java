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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link AuditMonitor}. Backed by a real
 * {@link MetricStatisticsProvider} rather than a mock, so the assertions run
 * against the metric names and label suffixes an operator actually sees in
 * JMX.
 */
public class AuditMonitorTest {

    private static final AuditDomain DOMAIN = AuditDomain.of("test.domain");
    private static final AuditDomain OTHER_DOMAIN = AuditDomain.of("other.domain");

    private ScheduledExecutorService executor;
    private MetricStatisticsProvider statisticsProvider;
    private MetricRegistry registry;
    private AuditMonitor monitor;

    @Before
    public void setUp() {
        executor = newSingleThreadScheduledExecutor();
        statisticsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executor);
        registry = statisticsProvider.getRegistry();
        monitor = new AuditMonitor(statisticsProvider);
    }

    @After
    public void tearDown() {
        if (statisticsProvider != null) {
            statisticsProvider.close();
        }
        if (executor != null) {
            new ExecutorCloser(executor).close();
        }
    }

    //-------------------------------------------------------< event meter >---

    @Test
    public void eventsDispatchedMarksPerDomainMeter() {
        monitor.eventsDispatched(DOMAIN, 3);

        assertEquals(3, meterCount("security.audit.events;domain=test.domain"));
    }

    @Test
    public void eventsDispatchedAccumulatesAcrossCalls() {
        monitor.eventsDispatched(DOMAIN, 2);
        monitor.eventsDispatched(DOMAIN, 5);

        assertEquals(7, meterCount("security.audit.events;domain=test.domain"));
    }

    @Test
    public void eventsDispatchedKeepsDomainsSeparate() {
        monitor.eventsDispatched(DOMAIN, 1);
        monitor.eventsDispatched(OTHER_DOMAIN, 4);

        assertEquals(1, meterCount("security.audit.events;domain=test.domain"));
        assertEquals(4, meterCount("security.audit.events;domain=other.domain"));
    }

    //-----------------------------------------------------< dropped meter >---

    @Test
    public void eventDroppedMarksPerDomainMeter() {
        monitor.eventDropped(DOMAIN);
        monitor.eventDropped(DOMAIN);

        assertEquals(2, meterCount("security.audit.events.dropped;domain=test.domain"));
    }

    @Test
    public void droppedMeterIsSeparateFromDispatchedMeter() {
        monitor.eventsDispatched(DOMAIN, 5);
        monitor.eventDropped(DOMAIN);

        assertEquals(5, meterCount("security.audit.events;domain=test.domain"));
        assertEquals(1, meterCount("security.audit.events.dropped;domain=test.domain"));
    }

    //--------------------------------------------------< listener metrics >---

    @Test
    public void listenerDurationRecordsPerListenerTimer() {
        monitor.listenerDuration(TestListener.class, TimeUnit.MILLISECONDS.toNanos(7));

        String name = "security.audit.listener.duration;listener="
                + TestListener.class.getName();
        assertNotNull(registry.getTimers().get(name));
        assertEquals(1, registry.getTimers().get(name).getCount());
    }

    @Test
    public void listenerDurationKeepsListenersSeparate() {
        monitor.listenerDuration(TestListener.class, 100L);
        monitor.listenerDuration(OtherTestListener.class, 200L);
        monitor.listenerDuration(OtherTestListener.class, 300L);

        assertEquals(1, timerCount("security.audit.listener.duration;listener="
                + TestListener.class.getName()));
        assertEquals(2, timerCount("security.audit.listener.duration;listener="
                + OtherTestListener.class.getName()));
    }

    @Test
    public void listenerFailedMarksPerListenerMeter() {
        monitor.listenerFailed(TestListener.class);

        assertEquals(1, meterCount("security.audit.listener.failures;listener="
                + TestListener.class.getName()));
    }

    @Test
    public void listenerFailureIsSeparateFromDuration() {
        monitor.listenerDuration(TestListener.class, 50L);
        monitor.listenerFailed(TestListener.class);

        assertEquals(1, timerCount("security.audit.listener.duration;listener="
                + TestListener.class.getName()));
        assertEquals(1, meterCount("security.audit.listener.failures;listener="
                + TestListener.class.getName()));
    }

    //----------------------------------------------------------------< NOOP >---

    @Test
    public void noopMonitorRecordsNothing() {
        // Every method must be callable and must not touch a provider.
        AuditMonitor.NOOP.eventsDispatched(DOMAIN, 5);
        AuditMonitor.NOOP.eventDropped(DOMAIN);
        AuditMonitor.NOOP.listenerDuration(TestListener.class, 100L);
        AuditMonitor.NOOP.listenerFailed(TestListener.class);

        assertNoAuditMetricsRegistered();
    }

    @Test
    public void nullProviderBehavesLikeNoop() {
        AuditMonitor nullBacked = new AuditMonitor(null);

        nullBacked.eventsDispatched(DOMAIN, 5);
        nullBacked.eventDropped(DOMAIN);
        nullBacked.listenerDuration(TestListener.class, 100L);
        nullBacked.listenerFailed(TestListener.class);

        assertNoAuditMetricsRegistered();
    }

    @Test
    public void noopProviderIsAccepted() {
        // StatisticsProvider.NOOP hands back NoopStats for every handle;
        // the monitor must not treat that differently from a real provider.
        AuditMonitor noopBacked = new AuditMonitor(StatisticsProvider.NOOP);

        noopBacked.eventsDispatched(DOMAIN, 5);
        noopBacked.listenerFailed(TestListener.class);

        assertNull(registry.getMeters().get("security.audit.events;domain=test.domain"));
    }

    //--------------------------------------------------------------< utils >---

    /**
     * The registry is not empty to begin with — {@code MetricStatisticsProvider}
     * registers its own baseline metrics — so "recorded nothing" is asserted
     * against the audit metric names rather than against registry size.
     */
    private void assertNoAuditMetricsRegistered() {
        assertTrue("no audit meter should be registered",
                registry.getMeters().keySet().stream().noneMatch(n -> n.startsWith("security.audit.")));
        assertTrue("no audit timer should be registered",
                registry.getTimers().keySet().stream().noneMatch(n -> n.startsWith("security.audit.")));
    }

    private long meterCount(String name) {
        assertNotNull("no meter registered under " + name, registry.getMeters().get(name));
        return registry.getMeters().get(name).getCount();
    }

    private long timerCount(String name) {
        assertNotNull("no timer registered under " + name, registry.getTimers().get(name));
        return registry.getTimers().get(name).getCount();
    }

    private static final class TestListener {
        // Name-only stand-in: the monitor labels by Class#getName().
    }

    private static final class OtherTestListener {
        // Second label so the per-listener separation is observable.
    }
}
