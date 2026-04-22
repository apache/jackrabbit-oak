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

package org.apache.jackrabbit.oak.cache;

import static java.lang.String.format;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.cache.CacheStats;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract base class for providing cache statistic via the {@link CacheStatsMBean}.
 */
public abstract class AbstractCacheStats extends AnnotatedStandardMBean implements CacheStatsMBean {

    @NotNull
    private final String name;

    private CacheStats lastSnapshot =
            new CacheStats(0, 0, 0, 0, 0, 0);

    /**
     * Create a new {@code CacheStatsMBean} for a cache with the given {@code name}.
     * @param name
     */
    protected AbstractCacheStats(@NotNull String name) {
        super(CacheStatsMBean.class);
        this.name = Objects.requireNonNull(name);
    }

    /**
     * Call back invoked to retrieve the most recent {@code CacheStats} instance of the
     * underlying cache.
     */
    protected abstract CacheStats getCurrentStats();

    private CacheStats stats() {
        return getCurrentStats().minus(lastSnapshot);
    }

    @Override
    public synchronized void resetStats() {
        // Cache stats cannot be rest at Guava level. Instead we
        // take a snapshot and then subtract it from future stats calls
        lastSnapshot = getCurrentStats();
    }

    @NotNull
    @Override
    public String getName() {
        return name;
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
    public String cacheInfoAsString() {
        return new StringJoiner(", ", "CacheStats [", "]")
                .add("hitCount=" + getHitCount())
                .add("hitRate=" + format("%1.2f", getHitRate()))
                .add("missCount=" + getMissCount())
                .add("missRate=" + format("%1.2f", getMissRate()))
                .add("requestCount=" + getRequestCount())
                .add("loadCount=" + getLoadCount())
                .add("loadSuccessCount=" + getLoadSuccessCount())
                .add("loadExceptionCount=" + getLoadExceptionCount())
                .add("totalLoadTime=" + timeInWords(getTotalLoadTime()))
                .add("averageLoadPenalty=" + format("%1.2f ns", getAverageLoadPenalty()))
                .add("evictionCount=" + getEvictionCount())
                .add("elementCount=" + getElementCount())
                .add("totalWeight=" + humanReadableByteCount(estimateCurrentWeight()))
                .add("maxWeight=" + humanReadableByteCount(getMaxTotalWeight()))
                .toString();
    }

    public static String timeInWords(long nanos) {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        return String.format("%d min, %d sec",
                TimeUnit.MILLISECONDS.toMinutes(millis),
                TimeUnit.MILLISECONDS.toSeconds(millis) -
                        TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
        );
    }

}
