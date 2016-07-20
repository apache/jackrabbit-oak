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
package org.apache.jackrabbit.oak.cache;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;

/**
 * Cache statistics.
 */
public class CacheStats extends AnnotatedStandardMBean implements CacheStatsMBean {
    private final Cache<Object, Object> cache;
    private final Weigher<Object, Object> weigher;
    private final long maxWeight;
    private final String name;
    private com.google.common.cache.CacheStats lastSnapshot = 
            new com.google.common.cache.CacheStats(
            0, 0, 0, 0, 0, 0);

    /**
     * Construct the cache stats object.
     * 
     * @param cache the cache
     * @param name the name of the cache
     * @param weigher the weigher used to estimate the current weight
     * @param maxWeight the maximum weight
     */
    @SuppressWarnings("unchecked")
    public CacheStats(Cache<?, ?> cache, String name, 
            Weigher<?, ?> weigher, long maxWeight) {
        super(CacheStatsMBean.class);
        this.cache = (Cache<Object, Object>) cache;
        this.name = name;
        this.weigher = (Weigher<Object, Object>) weigher;
        this.maxWeight = maxWeight;
    }

    @Override
    public long getRequestCount() {
        return stats().requestCount();
    }

    @Override
    public long getHitCount() {
        return stats().hitCount();
    }

    @Override
    public double getHitRate() {
        return stats().hitRate();
    }

    @Override
    public long getMissCount() {
        return stats().missCount();
    }

    @Override
    public double getMissRate() {
        return stats().missRate();
    }

    @Override
    public long getLoadCount() {
        return stats().loadCount();
    }

    @Override
    public long getLoadSuccessCount() {
        return stats().loadSuccessCount();
    }

    @Override
    public long getLoadExceptionCount() {
        return stats().loadExceptionCount();
    }

    @Override
    public double getLoadExceptionRate() {
        return stats().loadExceptionRate();
    }

    @Override
    public long getTotalLoadTime() {
        return stats().totalLoadTime();
    }

    @Override
    public double getAverageLoadPenalty() {
        return stats().averageLoadPenalty();
    }

    @Override
    public long getEvictionCount() {
        return stats().evictionCount();
    }

    @Override
    public long getElementCount() {
        return cache.size();
    }

    @Override
    public long estimateCurrentWeight() {
        if (weigher == null) {
            return -1;
        }
        long size = 0;
        for (Map.Entry<?, ?> e : cache.asMap().entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();
            size += weigher.weigh(k, v);
        }
        return size;
    }

    @Override
    public long getMaxTotalWeight() {
        return maxWeight;
    }

    @Override
    public synchronized void resetStats() {
        //Cache stats cannot be rest at Guava level. Instead we
        //take a snapshot and then subtract it from future stats calls
        lastSnapshot = cache.stats();
    }

    @Override
    public String cacheInfoAsString() {
        return Objects.toStringHelper("CacheStats")
                .add("hitCount", getHitCount())
                .add("hitRate", String.format("%1.2f", getHitRate()))
                .add("missCount", getMissCount())
                .add("missRate", String.format("%1.2f", getMissRate()))
                .add("requestCount", getRequestCount())
                .add("loadCount", getLoadCount())
                .add("loadSuccessCount", getLoadSuccessCount())
                .add("loadExceptionCount", getLoadExceptionCount())
                .add("totalLoadTime", timeInWords(getTotalLoadTime()))
                .add("averageLoadPenalty (nanos)", String.format("%1.2f", getAverageLoadPenalty()))
                .add("evictionCount", getEvictionCount())
                .add("elementCount", getElementCount())
                .add("totalWeight", humanReadableByteCount(estimateCurrentWeight()))
                .add("maxWeight", humanReadableByteCount(getMaxTotalWeight()))
                .toString();
    }

    @Override
    public String getName() {
        return name;
    }

    private com.google.common.cache.CacheStats stats() {
        return cache.stats().minus(lastSnapshot);
    }

    static String timeInWords(long nanos) {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        return String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
    }
}
