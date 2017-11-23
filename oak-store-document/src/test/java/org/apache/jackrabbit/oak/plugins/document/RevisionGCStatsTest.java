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

import java.util.concurrent.ScheduledExecutorService;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.junit.After;
import org.junit.Test;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.jackrabbit.oak.plugins.document.RevisionGCStats.RGC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RevisionGCStatsTest {

    private ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private MetricStatisticsProvider statsProvider =
            new MetricStatisticsProvider(getPlatformMBeanServer(), executor);
    private RevisionGCStats stats = new RevisionGCStats(statsProvider);

    @After
    public void shutDown(){
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void getReadDocCount() {
        Meter m = getMeter(RevisionGCStats.READ_DOC);
        long count = m.getCount();
        stats.documentRead();
        assertEquals(count + 1, m.getCount());
        assertEquals(count + 1, stats.getReadDocCount());
    }

    @Test
    public void getDeletedDocCount() {
        Meter m = getMeter(RevisionGCStats.DELETE_DOC);
        long count = m.getCount();
        stats.documentsDeleted(17);
        assertEquals(count + 17, m.getCount());
        assertEquals(count + 17, stats.getDeletedDocCount());
    }

    @Test
    public void getDeletedLeafDocCount() {
        Meter m = getMeter(RevisionGCStats.DELETE_LEAF_DOC);
        long count = m.getCount();
        stats.leafDocumentsDeleted(17);
        assertEquals(count + 17, m.getCount());
        assertEquals(count + 17, stats.getDeletedLeafDocCount());
    }

    @Test
    public void getDeletedSplitDocCount() {
        Meter m = getMeter(RevisionGCStats.DELETE_SPLIT_DOC);
        long count = m.getCount();
        stats.splitDocumentsDeleted(17);
        assertEquals(count + 17, m.getCount());
        assertEquals(count + 17, stats.getDeletedSplitDocCount());
    }

    @Test
    public void getDeletedIntSplitDocCount() {
        Meter m = getMeter(RevisionGCStats.DELETE_INT_SPLIT_DOC);
        long count = m.getCount();
        stats.intermediateSplitDocumentsDeleted(17);
        assertEquals(count + 17, m.getCount());
        assertEquals(count + 17, stats.getDeletedIntSplitDocCount());
    }

    @Test
    public void getResetDeletedFlagCount() {
        Meter m = getMeter(RevisionGCStats.RESET_DELETED_FLAG);
        long count = m.getCount();
        stats.deletedOnceFlagReset();
        assertEquals(count + 1, m.getCount());
        assertEquals(count + 1, stats.getResetDeletedFlagCount());
    }

    @Test
    public void timers() {
        VersionGCStats vgcs = new VersionGCStats();
        vgcs.collectDeletedDocsElapsed = MILLISECONDS.toMicros(2);
        vgcs.checkDeletedDocsElapsed = MILLISECONDS.toMicros(3);
        vgcs.deleteDeletedDocsElapsed = MILLISECONDS.toMicros(5);
        vgcs.collectAndDeleteSplitDocsElapsed = MILLISECONDS.toMicros(7);
        vgcs.sortDocIdsElapsed = MILLISECONDS.toMicros(11);
        vgcs.updateResurrectedDocumentsElapsed = MILLISECONDS.toMicros(13);
        vgcs.active.start();
        while (vgcs.active.elapsed(MILLISECONDS) < 5) {
            // busy wait
            assertTrue(vgcs.active.isRunning());
        }
        vgcs.active.stop();

        stats.finished(vgcs);
        assertTimer(vgcs.active.elapsed(MILLISECONDS), RevisionGCStats.ACTIVE_TIMER);
        assertTimer(2, RevisionGCStats.READ_DOC_TIMER);
        assertTimer(3, RevisionGCStats.CHECK_DELETED_TIMER);
        assertTimer(5, RevisionGCStats.DELETE_DOC_TIMER);
        assertTimer(7, RevisionGCStats.DELETE_SPLIT_DOC_TIMER);
        assertTimer(11, RevisionGCStats.SORT_IDS_TIMER);
        assertTimer(13, RevisionGCStats.RESET_DELETED_FLAG_TIMER);
    }

    private void assertTimer(long expected, String name) {
        assertEquals(expected, NANOSECONDS.toMillis(getTimer(name).getSnapshot().getMax()));
    }

    private Timer getTimer(String name) {
        return statsProvider.getRegistry().getTimers().get(RGC + "." + name);
    }

    private Meter getMeter(String name) {
        return statsProvider.getRegistry().getMeters().get(RGC + "." + name);
    }
}
