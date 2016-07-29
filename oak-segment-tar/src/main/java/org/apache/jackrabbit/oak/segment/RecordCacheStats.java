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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheStats;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;

// FIXME OAK-4619: Unify RecordCacheStats and CacheStats
/**
 * Statistics for {@link RecordCache}.
 */
public class RecordCacheStats extends AnnotatedStandardMBean implements CacheStatsMBean {

    @Nonnull
    private final String name;

    @Nonnull
    private final Supplier<CacheStats> stats;

    @Nonnull
    private final Supplier<Long> elementCount;

    private CacheStats lastSnapshot;

    public RecordCacheStats(
            @Nonnull String name, @Nonnull Supplier<CacheStats> stats, @Nonnull Supplier<Long> elementCount) {
        super(CacheStatsMBean.class);
        this.name = checkNotNull(name);
        this.stats = checkNotNull(stats);
        this.elementCount = checkNotNull(elementCount);
        this.lastSnapshot = stats.get();
    }

    private CacheStats stats() {
        return stats.get().minus(lastSnapshot);
    }

    @Override
    public synchronized void resetStats() {
        lastSnapshot = stats.get();
    }

    @Nonnull
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
    public long getElementCount() {
        return elementCount.get();
    }

    @Override
    public long getMaxTotalWeight() {
        return -1;
    }

    @Override
    public long estimateCurrentWeight() {
        return -1;
    }

    @Override
    public String cacheInfoAsString() {
        return Objects.toStringHelper("CacheStats(" + name + ")")
            .add("hitCount", getHitCount())
            .add("hitRate", format("%1.2f", getHitRate()))
            .add("missCount", getMissCount())
            .add("missRate", format("%1.2f", getMissRate()))
            .add("requestCount", getRequestCount())
            .add("loadCount", getLoadCount())
            .add("loadSuccessCount", getLoadSuccessCount())
            .add("loadExceptionCount", getLoadExceptionCount())
            .add("totalLoadTime", format("%d s", NANOSECONDS.toSeconds(getTotalLoadTime())))
            .add("averageLoadPenalty ", format("%1.2f ns", getAverageLoadPenalty()))
            .add("evictionCount", getEvictionCount())
            .add("elementCount", getElementCount())
            .add("totalWeight", humanReadableByteCount(estimateCurrentWeight()))
            .add("maxWeight", humanReadableByteCount(getMaxTotalWeight()))
            .toString();
    }

}
