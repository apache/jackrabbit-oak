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

package org.apache.jackrabbit.oak.plugins.index;

import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public class TrackingCorruptIndexHandlerTest {

    private TrackingCorruptIndexHandler handler = new TrackingCorruptIndexHandler();
    private Clock clock = new Clock.Virtual();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @After
    public void cleanup() {
        scheduledExecutorService.shutdown();
    }

    @Test
    public void basics() throws Exception{
        handler.setClock(clock);
        handler.indexUpdateFailed("async", "/oak:index/foo", new Exception());

        //Index would not be considered corrupt until timeout
        assertFalse(handler.getCorruptIndexData("async").containsKey("/oak:index/foo"));

        clock.waitUntil(clock.getTime() + handler.getCorruptIntervalMillis() + 1);
        assertTrue(handler.getCorruptIndexData("async").containsKey("/oak:index/foo"));

        //Should only be visible for "async"
        assertFalse(handler.getCorruptIndexData("async-fulltext").containsKey("/oak:index/foo"));

        handler.markWorkingIndexes(Collections.singleton("/oak:index/foo"));
        assertFalse(handler.getCorruptIndexData("async").containsKey("/oak:index/foo"));
    }

    @Test
    public void testCorruptCounter() {
        MeterStats meter = new DefaultStatisticsProvider(scheduledExecutorService).
                getMeter(TrackingCorruptIndexHandler.CORRUPT_INDEX_METER_NAME, StatsOptions.METRICS_ONLY);

        handler.setMeterStats(meter);
        handler.setClock(clock);
        handler.indexUpdateFailed("async", "/oak:index/foo", new Exception());
        assertEquals(1, meter.getCount());
        handler.indexUpdateFailed("async", "/oak:index/bar", new Exception());
        assertEquals(2, meter.getCount());

        HashSet<String> set = new HashSet<>();
        set.add("/oak:index/foo");
        handler.markWorkingIndexes(set);

        assertEquals(1, meter.getCount());
    }

    @Test
    public void disbaled() throws Exception{
        handler.setClock(clock);
        handler.indexUpdateFailed("async", "/oak:index/foo", new Exception());

        clock.waitUntil(clock.getTime() + handler.getCorruptIntervalMillis() + 1);
        assertTrue(handler.getCorruptIndexData("async").containsKey("/oak:index/foo"));

        handler.setCorruptInterval(0, TimeUnit.SECONDS);

        //With timeout set to zero no corrupt index should be reported
        assertFalse(handler.getCorruptIndexData("async").containsKey("/oak:index/foo"));
    }

    @Test
    public void warningLoggedAfterSomeTime() throws Exception{
        handler.setClock(clock);
        handler.indexUpdateFailed("async", "/oak:index/foo", new Exception());

        assertFalse(handler.skippingCorruptIndex("async", "/oak:index/foo", Calendar.getInstance()));

        clock.waitUntil(clock.getTime() + handler.getErrorWarnIntervalMillis() + 1);

        assertTrue(handler.skippingCorruptIndex("async", "/oak:index/foo", Calendar.getInstance()));
        assertFalse(handler.skippingCorruptIndex("async", "/oak:index/foo", Calendar.getInstance()));
    }

}