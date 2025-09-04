/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.stats.GaugeStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.BATCH_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.COLLECT_DELETED_OLD_REVS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.COLLECT_DELETED_PROPS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.COLLECT_FULL_GC_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.COLLECT_ORPHAN_NODES_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.COLLECT_UNMERGED_BC_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.COUNTER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.DELAY_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.DELETED_ORPHAN_NODE;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.DELETED_UNMERGED_BC;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.DELETE_FULL_GC_DOCS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.EMBEDDED_VERIFICATION_ENABLED;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.ENABLED;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.FULL_GC;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.FULL_GC_ACTIVE_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.FULL_GC_GENERATION;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.FULL_GC_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.FAILURE_COUNTER;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.MAX_AGE;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.MODE;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.PROGRESS_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.READ_DOC;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.SKIPPED_DOC;
import static org.apache.jackrabbit.oak.plugins.document.FullGCStatsCollectorImpl.UPDATED_DOC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit Cases for {@link FullGCStatsCollectorImpl}
 */
public class FullGCStatsCollectorImplTest {

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final MetricStatisticsProvider statsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executor);
    private final FullGCStatsCollectorImpl stats = new FullGCStatsCollectorImpl(statsProvider);

    @After
    public void shutDown(){
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void getReadDocCount() throws IllegalAccessException {
        final Meter m = getMeter(READ_DOC);
        long count = m.getCount();
        stats.documentRead();
        assertEquals(count + 1, m.getCount());
        assertEquals(count + 1, ((MeterStats) readField(stats, "readDoc", true)).getCount());
    }

    @Test
    public void getDocumentsSkippedUpdationCount() throws IllegalAccessException {
        Meter m = getMeter(SKIPPED_DOC);
        long count = m.getCount();
        stats.documentsUpdateSkipped(17);
        assertEquals(count + 17, m.getCount());
        assertEquals(count + 17, ((MeterStats) readField(stats, "skippedDoc", true)).getCount());
    }

    @Test
    public void getOrphanNodesDeletedCount() throws IllegalAccessException {
        Meter m = getMeter(DELETED_ORPHAN_NODE);
        long count = m.getCount();
        stats.orphanNodesDeleted(10);
        assertEquals(count + 10, m.getCount());
        assertEquals(count + 10, ((MeterStats) readField(stats, "deletedOrphanNode", true)).getCount());
    }

    @Test
    public void getPropertiesDeletedCount() throws IllegalAccessException {
        Meter m = getMeter(DELETED_PROPERTY);
        long count = m.getCount();
        stats.propertiesDeleted(10);
        assertEquals(count + 10, m.getCount());
        assertEquals(count + 10, ((MeterStats) readField(stats, "deletedProperty", true)).getCount());
    }

    @Test
    public void getUnmergedBCDeletedCount() throws IllegalAccessException {
        Meter m = getMeter(DELETED_UNMERGED_BC);
        long count = m.getCount();
        stats.unmergedBranchCommitsDeleted(10);
        assertEquals(count + 10, m.getCount());
        assertEquals(count + 10, ((MeterStats) readField(stats, "deletedUnmergedBC", true)).getCount());
    }

    @Test
    public void getDocumentsUpdatedCount() throws IllegalAccessException {
        Meter m = getMeter(UPDATED_DOC);
        long count = m.getCount();
        stats.documentsUpdated(10);
        assertEquals(count + 10, m.getCount());
        assertEquals(count + 10, ((MeterStats) readField(stats, "updatedDoc", true)).getCount());
    }

    @Test
    public void timers() {
        final VersionGarbageCollector.VersionGCStats vgcs = new VersionGarbageCollector.VersionGCStats();
        vgcs.fullGCActiveElapsed = MILLISECONDS.toMicros(2);
        vgcs.fullGCDocsElapsed = MILLISECONDS.toMicros(3);
        vgcs.collectFullGCElapsed = MILLISECONDS.toMicros(5);
        vgcs.collectOrphanNodesElapsed = MILLISECONDS.toMicros(6);
        vgcs.collectDeletedPropsElapsed = MILLISECONDS.toMicros(7);
        vgcs.collectDeletedOldRevsElapsed = MILLISECONDS.toMicros(11);
        vgcs.collectUnmergedBCElapsed = MILLISECONDS.toMicros(13);
        vgcs.deleteFullGCDocsElapsed = MILLISECONDS.toMicros(15);
        vgcs.fullGCActive.start();

        stats.finished(vgcs);
        assertTimer(2, FULL_GC_ACTIVE_TIMER);
        assertTimer(3, FULL_GC_TIMER);
        assertTimer(5, COLLECT_FULL_GC_TIMER);
        assertTimer(6, COLLECT_ORPHAN_NODES_TIMER);
        assertTimer(7, COLLECT_DELETED_PROPS_TIMER);
        assertTimer(11, COLLECT_DELETED_OLD_REVS_TIMER);
        assertTimer(13, COLLECT_UNMERGED_BC_TIMER);
        assertTimer(15, DELETE_FULL_GC_DOCS_TIMER);
    }

    @Test
    public void counters() {
        Counter counter = getCounter(COUNTER);
        Counter failureCounter = getCounter(FAILURE_COUNTER);

        VersionGarbageCollector.VersionGCStats vgcs = new VersionGarbageCollector.VersionGCStats();
        stats.started();
        stats.finished(vgcs);
        assertEquals(1, counter.getCount());
        assertEquals(0, failureCounter.getCount());

        vgcs.success = false;
        stats.started();
        stats.finished(vgcs);
        assertEquals(2, counter.getCount());
        assertEquals(1, failureCounter.getCount());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getEnabled() throws IllegalAccessException {
        stats.enabled(true);
        final Gauge<Boolean> gauge = getGauge(ENABLED);
        assertTrue(gauge.getValue());
        assertTrue(((GaugeStats<Boolean>) readField(stats, "enabled", true)).getValue());

        // since it is gauge, updating won't change the value.
        stats.enabled(false);
        final Gauge<Boolean> updated = getGauge(ENABLED);
        assertTrue(updated.getValue());

    }

    @Test
    @SuppressWarnings("unchecked")
    public void getMode() throws IllegalAccessException {
        stats.mode(4);
        final Gauge<Integer> gauge = getGauge(MODE);
        assertEquals(4, (int)gauge.getValue());
        assertEquals(4, (int)((GaugeStats<Integer>) readField(stats, "mode", true)).getValue());

        // update the value
        stats.mode(5);
        final Gauge<Integer> updated = getGauge(MODE);
        assertEquals(4, (int)updated.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getDelayFactor() throws IllegalAccessException {
        stats.delayFactor(4.0);
        final Gauge<Double> gauge = getGauge(DELAY_FACTOR);
        assertEquals(4.0, gauge.getValue(), 0.01);
        assertEquals(4.0, ((GaugeStats<Double>) readField(stats, "delayFactor", true)).getValue(), 0.01);

        // update the value
        stats.delayFactor(5.0);
        final Gauge<Double> updated = getGauge(DELAY_FACTOR);
        assertEquals(4.0, updated.getValue(), 0.01);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getBatchSize() throws IllegalAccessException {
        stats.batchSize(400);
        final Gauge<Integer> gauge = getGauge(BATCH_SIZE);
        assertEquals(400, (int)gauge.getValue());
        assertEquals(400, (int)((GaugeStats<Integer>) readField(stats, "batchSize", true)).getValue());

        // update the value
        stats.batchSize(500);
        final Gauge<Integer> updated = getGauge(BATCH_SIZE);
        assertEquals(400, (int)updated.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getProgressSize() throws IllegalAccessException {
        stats.progressSize(4000);
        final Gauge<Integer> gauge = getGauge(PROGRESS_SIZE);
        assertEquals(4000, (int)gauge.getValue());
        assertEquals(4000, (int)((GaugeStats<Integer>) readField(stats, "progressSize", true)).getValue());

        // update the value
        stats.progressSize(5000);
        final Gauge<Integer> updated = getGauge(PROGRESS_SIZE);
        assertEquals(4000, (int)updated.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getEmbeddedVerificationEnabled() throws IllegalAccessException {
        stats.verificationEnabled(true);
        final Gauge<Boolean> gauge = getGauge(EMBEDDED_VERIFICATION_ENABLED);
        assertTrue(gauge.getValue());
        assertTrue(((GaugeStats<Boolean>) readField(stats, "embeddedVerificationEnabled", true)).getValue());

        // update the value
        stats.verificationEnabled(false);
        final Gauge<Boolean> updated = getGauge(EMBEDDED_VERIFICATION_ENABLED);
        assertTrue(updated.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getMaxAge() throws IllegalAccessException {
        stats.maxAge(86400);
        final Gauge<Long> gauge = getGauge(MAX_AGE);
        assertEquals(86400L, (long)gauge.getValue());
        assertEquals(86400L, (long)((GaugeStats<Long>) readField(stats, "maxAge", true)).getValue());

        // update the value
        stats.maxAge(980000);
        final Gauge<Long> updated = getGauge(MAX_AGE);
        assertEquals(86400L, (long)updated.getValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getFullGCGeneration() throws IllegalAccessException {
        stats.fullGCGeneration(3);
        final Gauge<Long> gauge = getGauge(FULL_GC_GENERATION);
        assertEquals(3L, (long)gauge.getValue());
        assertEquals(3L, (long)((GaugeStats<Long>) readField(stats, "fullGCGeneration", true)).getValue());

        // update the value
        stats.fullGCGeneration(5);
        final Gauge<Long> updated = getGauge(FULL_GC_GENERATION);
        assertEquals(3L, (long)updated.getValue());
    }

    @Test
    public void getFullGcOsgiConfigs() {
        stats.maxAge(86400);
        stats.enabled(true);
        stats.verificationEnabled(false);
        stats.progressSize(4000);
        stats.batchSize(500);
        stats.delayFactor(5.0);
        stats.mode(4);
        stats.fullGCGeneration(2);

        // update the value
        assertTrue(stats.toString().contains("maxAge=86400"));
        assertTrue(stats.toString().contains("enabled=true"));
        assertTrue(stats.toString().contains("embeddedVerificationEnabled=false"));
        assertTrue(stats.toString().contains("progressSize=4000"));
        assertTrue(stats.toString().contains("batchSize=500"));
        assertTrue(stats.toString().contains("delayFactor=5.0"));
        assertTrue(stats.toString().contains("mode=4"));
        assertTrue(stats.toString().contains("fullGCGeneration=2"));
    }

    @Test
    public void getFullGcOsgiConfigsDefaults() {

        // update the value
        assertTrue(stats.toString().contains("maxAge=0"));
        assertTrue(stats.toString().contains("enabled=false"));
        assertTrue(stats.toString().contains("embeddedVerificationEnabled=false"));
        assertTrue(stats.toString().contains("progressSize=0"));
        assertTrue(stats.toString().contains("batchSize=0"));
        assertTrue(stats.toString().contains("delayFactor=0.0"));
        assertTrue(stats.toString().contains("mode=0"));
    }

    // helper methods

    private void assertTimer(long expected, String name) {
        assertEquals(expected, NANOSECONDS.toMillis(getTimer(name).getSnapshot().getMax()));
    }

    private Timer getTimer(String name) {
        return statsProvider.getRegistry().getTimers().get(FULL_GC + "." + name);
    }

    private Meter getMeter(String name) {
        return statsProvider.getRegistry().getMeters().get(FULL_GC + "." + name);
    }

    private Counter getCounter(String name) {
        return statsProvider.getRegistry().getCounters().get(FULL_GC + "." + name);
    }

    @SuppressWarnings("unchecked")
    private <T> Gauge<T> getGauge(String name) {
        return (Gauge<T>) statsProvider.getRegistry().getGauges().get(FULL_GC + "." + name);
    }

}
