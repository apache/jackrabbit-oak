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
package org.apache.jackrabbit.oak.cache.impl;

import java.util.Map;
import java.util.Random;

import javax.annotation.Nonnull;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.junit.Test;

import static org.apache.jackrabbit.oak.cache.impl.CacheStatsMetrics.ELEMENT;
import static org.apache.jackrabbit.oak.cache.impl.CacheStatsMetrics.EVICTION;
import static org.apache.jackrabbit.oak.cache.impl.CacheStatsMetrics.HIT;
import static org.apache.jackrabbit.oak.cache.impl.CacheStatsMetrics.MISS;
import static org.apache.jackrabbit.oak.cache.impl.CacheStatsMetrics.REQUEST;
import static org.apache.jackrabbit.oak.cache.impl.CacheStatsMetrics.metricName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CacheStatsMetricsTest {

    private static final Random RANDOM = new Random();
    private static final long HIT_COUNT = RANDOM.nextInt(Integer.MAX_VALUE);
    private static final long MISS_COUNT = RANDOM.nextInt(Integer.MAX_VALUE);
    private static final long REQUEST_COUNT = HIT_COUNT + MISS_COUNT;
    private static final long EVICTION_COUNT = RANDOM.nextInt(Integer.MAX_VALUE);
    private static final long ELEMENT_COUNT = RANDOM.nextInt(Integer.MAX_VALUE);
    private static final long LOAD_TIME = RANDOM.nextInt(Integer.MAX_VALUE);

    @Test
    public void metrics() {
        MetricRegistry registry = new MetricRegistry();
        CacheStatsMetrics metrics = new CacheStatsMetrics();
        metrics.setMetricRegistry(registry);

        CacheStatsMBean bean = new TestStats("stats");
        metrics.addCacheStatsMBean(bean);

        Map<String, Counter> counters = registry.getCounters();

        Counter counter = counters.get(metricName(bean.getName(), REQUEST));
        assertNotNull(counter);
        assertEquals(REQUEST_COUNT, counter.getCount());

        counter = counters.get(metricName(bean.getName(), HIT));
        assertNotNull(counter);
        assertEquals(HIT_COUNT, counter.getCount());

        counter = counters.get(metricName(bean.getName(), MISS));
        assertNotNull(counter);
        assertEquals(MISS_COUNT, counter.getCount());

        counter = counters.get(metricName(bean.getName(), EVICTION));
        assertNotNull(counter);
        assertEquals(EVICTION_COUNT, counter.getCount());

        counter = counters.get(metricName(bean.getName(), ELEMENT));
        assertNotNull(counter);
        assertEquals(ELEMENT_COUNT, counter.getCount());

        counter = counters.get(metricName(bean.getName(), CacheStatsMetrics.LOAD_TIME));
        assertNotNull(counter);
        assertEquals(LOAD_TIME, counter.getCount());

        metrics.removeCacheStatsMBean(bean);
        assertEquals(0, registry.getCounters().size());
    }

    private static class TestStats extends AbstractCacheStats {

        TestStats(@Nonnull String name) {
            super(name);
        }

        @Override
        protected com.google.common.cache.CacheStats getCurrentStats() {
            return new com.google.common.cache.CacheStats(
                    HIT_COUNT, MISS_COUNT, MISS_COUNT, 0, LOAD_TIME, EVICTION_COUNT);
        }

        @Override
        public long getElementCount() {
            return ELEMENT_COUNT;
        }

        @Override
        public long getMaxTotalWeight() {
            return 1000;
        }

        @Override
        public long estimateCurrentWeight() {
            return 1000;
        }
    }
}
