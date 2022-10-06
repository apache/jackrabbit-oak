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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.plugins.metric.MetricStatisticsProvider;
import org.junit.After;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.ImmutableList.of;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.JOURNAL_CREATE_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.JOURNAL_CREATE_THROTTLING_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_CREATE_SPLIT_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_CREATE_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_CREATE_THROTTLING_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_CREATE_UPSERT_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_CREATE_UPSERT_THROTTLING_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_REMOVE_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_REMOVE_THROTTLING_TIMER;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_UPDATE_FAILURE_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_UPDATE_RETRY_COUNT_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_UPDATE_THROTTLING;
import static org.apache.jackrabbit.oak.plugins.document.ThrottlingStatsCollectorImpl.NODES_UPDATE_THROTTLING_TIMER;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getILoggerFactory;

/**
 * Junit for {@link ThrottlingStatsCollectorImpl}
 */
public class ThrottlingStatsCollectorImplTest {

    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor();
    private final MetricStatisticsProvider statsProvider = new MetricStatisticsProvider(getPlatformMBeanServer(), executor);
    private final ThrottlingStatsCollector stats = new ThrottlingStatsCollectorImpl(statsProvider);

    @After
    public void shutDown(){
        statsProvider.close();
        new ExecutorCloser(executor).close();
    }

    @Test
    public void doneCreateJournal() {

        stats.doneCreate(100, JOURNAL, of("a", "b"), true);
        assertEquals(2, getMeter(JOURNAL_CREATE_THROTTLING).getCount());
        assertEquals(100, getTimer(JOURNAL_CREATE_THROTTLING_TIMER).getSnapshot().getMax());

        stats.doneCreate(200, JOURNAL, of("c", "d"), true);
        assertEquals(4, getMeter(JOURNAL_CREATE_THROTTLING).getCount());
        assertEquals(200, getTimer(JOURNAL_CREATE_THROTTLING_TIMER).getSnapshot().getMax());

        // journal metrics updated even if insert is not successful
        stats.doneCreate(200, JOURNAL, of("e", "f"), false);
        assertEquals(6, getMeter(JOURNAL_CREATE_THROTTLING).getCount());
        assertEquals(200, getTimer(JOURNAL_CREATE_THROTTLING_TIMER).getSnapshot().getMax());

        // Nodes metrics are not updated
        assertEquals(0, getMeter(NODES_CREATE_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(0, getTimer(NODES_CREATE_THROTTLING_TIMER).getSnapshot().getMax());
    }

    @Test
    public void doneCreateNodes() {

        // empty list of ids
        stats.doneCreate(100, NODES, of(), true);
        assertEquals(0, getMeter(NODES_CREATE_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(0, getTimer(NODES_CREATE_THROTTLING_TIMER).getSnapshot().getMax());

        stats.doneCreate(100, NODES, of("a", "b"), true);
        assertEquals(2, getMeter(NODES_CREATE_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(50, getTimer(NODES_CREATE_THROTTLING_TIMER).getSnapshot().getMax());

        // adding an Id with previous Doc
        stats.doneCreate(200, NODES, of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"), true);
        assertEquals(3, getMeter(NODES_CREATE_THROTTLING).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(200, getTimer(NODES_CREATE_THROTTLING_TIMER).getSnapshot().getMax());

        // insert is not successful
        stats.doneCreate(200, NODES, of("c"), false);
        assertEquals(3, getMeter(NODES_CREATE_THROTTLING).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(200, getTimer(NODES_CREATE_THROTTLING_TIMER).getSnapshot().getMax());

        // journal metrics are not updated
        assertEquals(0, getMeter(JOURNAL_CREATE_THROTTLING).getCount());
        assertEquals(0, getTimer(JOURNAL_CREATE_THROTTLING_TIMER).getSnapshot().getMax());
    }

    @Test
    public void doneCreateOrUpdate() {

        // empty list of ids
        stats.doneCreateOrUpdate(100, NODES, of());
        assertEquals(0, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(0, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());

        stats.doneCreateOrUpdate(100, NODES, of("a", "b"));
        assertEquals(2, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(50, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());

        // adding an Id with previous Doc
        stats.doneCreateOrUpdate(200, NODES, of("15:p/a/b/c/d/e/f/g/h/i/j/k/l/m/r182f83543dd-0-0/3"));
        assertEquals(3, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(200, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());

        // insert is done for journal collection
        stats.doneCreateOrUpdate(200, JOURNAL, of("c"));
        assertEquals(3, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(1, getMeter(NODES_CREATE_SPLIT_THROTTLING).getCount());
        assertEquals(200, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());

    }

    @Test
    public void doneFindAndModify() {

        // adding wrong collection type
        stats.doneFindAndModify(100, JOURNAL, "foo", false, true, 3);
        assertEquals(0, getMeter(NODES_UPDATE_THROTTLING).getCount());
        assertEquals(0, getTimer(NODES_UPDATE_THROTTLING_TIMER).getSnapshot().getMax());
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());
        assertEquals(0, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(0, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());

        // new entry is added successfully
        stats.doneFindAndModify(100, NODES, "foo", true, true, 0);
        assertEquals(1, getMeter(NODES_CREATE_UPSERT_THROTTLING).getCount());
        assertEquals(100, getTimer(NODES_CREATE_UPSERT_THROTTLING_TIMER).getSnapshot().getMax());
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());

        // old entry is updated successfully
        stats.doneFindAndModify(100, NODES, "foo", false, true, 0);
        assertEquals(1, getMeter(NODES_UPDATE_THROTTLING).getCount());
        assertEquals(100, getTimer(NODES_UPDATE_THROTTLING_TIMER).getSnapshot().getMax());
        assertEquals(0, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());

        // entry updated after 3 retries
        stats.doneFindAndModify(100, NODES, "foo", false, true, 3);
        assertEquals(2, getMeter(NODES_UPDATE_THROTTLING).getCount());
        assertEquals(100, getTimer(NODES_UPDATE_THROTTLING_TIMER).getSnapshot().getMax());
        assertEquals(3, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());

    }

    @Test
    public void doneFindAndModifyRetryAndFailure() {

        // operation has failed to update document
        stats.doneFindAndModify(100, NODES, "foo", true, false, 3);
        assertEquals(1, getMeter(NODES_UPDATE_FAILURE_THROTTLING).getCount());
        assertEquals(3, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());

        // operation succeeds after 2 retries
        stats.doneFindAndModify(100, NODES, "foo", true, true, 2);
        assertEquals(5, getMeter(NODES_UPDATE_RETRY_COUNT_THROTTLING).getCount());

    }

    @Test
    public void doneRemove() {

        stats.doneRemove(100, NODES);
        assertEquals(1, getMeter(NODES_REMOVE_THROTTLING).getCount());
        assertEquals(100, getTimer(NODES_REMOVE_THROTTLING_TIMER).getSnapshot().getMax());

        stats.doneRemove(100, NODES);
        assertEquals(2, getMeter(NODES_REMOVE_THROTTLING).getCount());
        assertEquals(100, getTimer(NODES_REMOVE_THROTTLING_TIMER).getSnapshot().getMax());

    }

    private Meter getMeter(String name) {
        return statsProvider.getRegistry().getMeters().get(name);
    }

    private Timer getTimer(String name) {
        return statsProvider.getRegistry().getTimers().get(name);
    }
}
