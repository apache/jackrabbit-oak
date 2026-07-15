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
 *
 */

package org.apache.jackrabbit.oak.segment;

import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.apache.jackrabbit.oak.segment.RecordCache.newRecordCache;
import static org.junit.Assert.assertEquals;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheAccessTrackerTest {
    private CacheAccessTracker<String, RecordId> cache;
    private Closeable closer;

    private MeterStats accessStats;
    private MeterStats missStats;

    @Before
    public void setup() {
        ScheduledExecutorService scheduler = newScheduledThreadPool(1);
        StatisticsProvider statistics = new DefaultStatisticsProvider(scheduler);
        cache = new CacheAccessTracker<String, RecordId>("foo", statistics, newRecordCache(100));
        closer = new ExecutorCloser(scheduler);

        accessStats = statistics.getMeter("foo.access-count", StatsOptions.DEFAULT);
        missStats = statistics.getMeter("foo.miss-count", StatsOptions.DEFAULT);
    }

    @After
    public void tearDown() throws IOException {
        closer.close();
    }

    @Test
    public void testNoAccess() {
        assertEquals(0, accessStats.getCount());
        assertEquals(0, missStats.getCount());
    }

    @Test
    public void testMissOnEmpty() {
        cache.get("non existing");

        assertEquals(1, accessStats.getCount());
        assertEquals(1, missStats.getCount());
    }

    @Test
    public void testHit() {
        cache.put("one", RecordId.NULL);
        cache.get("one");

        assertEquals(1, accessStats.getCount());
        assertEquals(0, missStats.getCount());
    }

    @Test
    public void testMissOnNonEmpty() {
        cache.put("one", RecordId.NULL);
        cache.get("non existing");

        assertEquals(1, accessStats.getCount());
        assertEquals(1, missStats.getCount());
    }

    @Test
    public void testHitMiss() {
        cache.put("one", RecordId.NULL);
        cache.get("one");
        cache.get("non existing");

        assertEquals(2, accessStats.getCount());
        assertEquals(1, missStats.getCount());
    }
}
