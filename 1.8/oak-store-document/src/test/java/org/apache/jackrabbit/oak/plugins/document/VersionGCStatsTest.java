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

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.junit.Before;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.junit.Assert.assertEquals;

public class VersionGCStatsTest {

    private static final Callable START = new Callable() {
        @Override
        public void call(Stopwatch watch) {
            watch.start();
        }
    };

    private static final Callable STOP = new Callable() {
        @Override
        public void call(Stopwatch watch) {
            watch.stop();
        }
    };

    private VersionGCStats stats = new VersionGCStats();
    
    @Before
    public void before() throws Exception {
        forEachStopwatch(stats, START);
        while (stats.updateResurrectedDocuments.elapsed(TimeUnit.MILLISECONDS) < 10) {
            Thread.sleep(1);
        }
        forEachStopwatch(stats, STOP);
    }
    
    @Test
    public void addRun() {
        VersionGCStats cumulative = new VersionGCStats();
        cumulative.addRun(stats);
        assertEquals(stats.active.elapsed(MICROSECONDS), cumulative.activeElapsed);
        assertEquals(stats.collectDeletedDocs.elapsed(MICROSECONDS), cumulative.collectDeletedDocsElapsed);
        assertEquals(stats.checkDeletedDocs.elapsed(MICROSECONDS), cumulative.checkDeletedDocsElapsed);
        assertEquals(stats.deleteDeletedDocs.elapsed(MICROSECONDS), cumulative.deleteDeletedDocsElapsed);
        assertEquals(stats.collectAndDeleteSplitDocs.elapsed(MICROSECONDS), cumulative.collectAndDeleteSplitDocsElapsed);
        assertEquals(stats.sortDocIds.elapsed(MICROSECONDS), cumulative.sortDocIdsElapsed);
        assertEquals(stats.updateResurrectedDocuments.elapsed(MICROSECONDS), cumulative.updateResurrectedDocumentsElapsed);
    }

    @Test
    public void addRunCumulative() {
        VersionGCStats cumulative = new VersionGCStats();
        cumulative.addRun(stats);
        // double stats by adding to itself
        cumulative.addRun(cumulative);
        // now the stats must have doubled
        assertEquals(stats.active.elapsed(MICROSECONDS) * 2, cumulative.activeElapsed);
        assertEquals(stats.collectDeletedDocs.elapsed(MICROSECONDS) * 2, cumulative.collectDeletedDocsElapsed);
        assertEquals(stats.checkDeletedDocs.elapsed(MICROSECONDS) * 2, cumulative.checkDeletedDocsElapsed);
        assertEquals(stats.deleteDeletedDocs.elapsed(MICROSECONDS) * 2, cumulative.deleteDeletedDocsElapsed);
        assertEquals(stats.collectAndDeleteSplitDocs.elapsed(MICROSECONDS) * 2, cumulative.collectAndDeleteSplitDocsElapsed);
        assertEquals(stats.sortDocIds.elapsed(MICROSECONDS) * 2, cumulative.sortDocIdsElapsed);
        assertEquals(stats.updateResurrectedDocuments.elapsed(MICROSECONDS) * 2, cumulative.updateResurrectedDocumentsElapsed);
    }

    private void forEachStopwatch(VersionGCStats stats, Callable c) {
        c.call(stats.active);
        c.call(stats.collectDeletedDocs);
        c.call(stats.checkDeletedDocs);
        c.call(stats.deleteDeletedDocs);
        c.call(stats.collectAndDeleteSplitDocs);
        c.call(stats.sortDocIds);
        c.call(stats.updateResurrectedDocuments);
    }
    
    private interface Callable {

        void call(Stopwatch watch);
    }
}
