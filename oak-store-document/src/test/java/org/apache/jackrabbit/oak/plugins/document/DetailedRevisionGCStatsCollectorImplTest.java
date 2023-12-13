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
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.COLLECT_DELETED_OLD_REVS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.COLLECT_DELETED_PROPS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.COLLECT_DETAILED_GARBAGE_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.COLLECT_UNMERGED_BC_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.COUNTER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.DELETED_PROPERTY;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.DELETED_UNMERGED_BC;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.DELETE_DETAILED_GC_DOCS_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.DETAILED_GC;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.DETAILED_GC_ACTIVE_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.DETAILED_GC_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.FAILURE_COUNTER;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.READ_DOC;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.SKIPPED_DOC;
import static org.apache.jackrabbit.oak.plugins.document.DetailedRevisionGCStatsCollectorImpl.UPDATED_DOC;
import static org.junit.Assert.assertEquals;

/**
 * Unit Cases for {@link DetailedRevisionGCStatsCollectorImpl}
 */
public class DetailedRevisionGCStatsCollectorImplTest {

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final MetricStatisticsProvider statsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executor);
    private final DetailedRevisionGCStatsCollectorImpl stats = new DetailedRevisionGCStatsCollectorImpl(statsProvider);

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
        vgcs.detailedGCActiveElapsed = MILLISECONDS.toMicros(2);
        vgcs.detailedGCDocsElapsed = MILLISECONDS.toMicros(3);
        vgcs.collectDetailedGarbageElapsed = MILLISECONDS.toMicros(5);
        vgcs.collectDeletedPropsElapsed = MILLISECONDS.toMicros(7);
        vgcs.collectDeletedOldRevsElapsed = MILLISECONDS.toMicros(11);
        vgcs.collectUnmergedBCElapsed = MILLISECONDS.toMicros(13);
        vgcs.deleteDetailedGCDocsElapsed = MILLISECONDS.toMicros(15);
        vgcs.detailedGCActive.start();

        stats.finished(vgcs);
        assertTimer(2, DETAILED_GC_ACTIVE_TIMER);
        assertTimer(3, DETAILED_GC_TIMER);
        assertTimer(5, COLLECT_DETAILED_GARBAGE_TIMER);
        assertTimer(7, COLLECT_DELETED_PROPS_TIMER);
        assertTimer(11, COLLECT_DELETED_OLD_REVS_TIMER);
        assertTimer(13, COLLECT_UNMERGED_BC_TIMER);
        assertTimer(15, DELETE_DETAILED_GC_DOCS_TIMER);
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
        return statsProvider.getRegistry().getTimers().get(DETAILED_GC + "." + name);
    }

    private Meter getMeter(String name) {
        return statsProvider.getRegistry().getMeters().get(DETAILED_GC + "." + name);
    }

    private Counter getCounter(String name) {
        return statsProvider.getRegistry().getCounters().get(DETAILED_GC + "." + name);
    }

}
