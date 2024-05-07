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
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.COLLECT_DELETED_OLD_REVS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.COLLECT_DELETED_PROPS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.COLLECT_DETAILED_GARBAGE_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.COLLECT_ORPHAN_NODES_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.COLLECT_UNMERGED_BC_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.COUNTER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.DELETED_ORPHAN_NODE;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.DELETED_UNMERGED_BC;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.DELETE_FULL_GC_DOCS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.FULL_GC;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.FULL_GC_ACTIVE_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.FULL_GC_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.FAILURE_COUNTER;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.READ_DOC;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.SKIPPED_DOC;
import static org.apache.jackrabbit.oak.plugins.document.FullRevisionGCStatsCollectorImpl.UPDATED_DOC;
import static org.junit.Assert.assertEquals;

/**
 * Unit Cases for {@link FullRevisionGCStatsCollectorImpl}
 */
public class FullRevisionGCStatsCollectorImplTest {

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final MetricStatisticsProvider statsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executor);
    private final FullRevisionGCStatsCollectorImpl stats = new FullRevisionGCStatsCollectorImpl(statsProvider);

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
        vgcs.collectDetailedGarbageElapsed = MILLISECONDS.toMicros(5);
        vgcs.collectOrphanNodesElapsed = MILLISECONDS.toMicros(6);
        vgcs.collectDeletedPropsElapsed = MILLISECONDS.toMicros(7);
        vgcs.collectDeletedOldRevsElapsed = MILLISECONDS.toMicros(11);
        vgcs.collectUnmergedBCElapsed = MILLISECONDS.toMicros(13);
        vgcs.deleteFullGCDocsElapsed = MILLISECONDS.toMicros(15);
        vgcs.fullGCActive.start();

        stats.finished(vgcs);
        assertTimer(2, FULL_GC_ACTIVE_TIMER);
        assertTimer(3, FULL_GC_TIMER);
        assertTimer(5, COLLECT_DETAILED_GARBAGE_TIMER);
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

}
