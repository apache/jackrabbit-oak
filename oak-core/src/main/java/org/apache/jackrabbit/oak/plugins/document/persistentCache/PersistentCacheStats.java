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
 */
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import com.google.common.base.Objects;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.api.jmx.PersistentCacheStatsMBean;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.Counting;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;

import javax.management.openmbean.CompositeData;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Persistence Cache Statistics.
 */
public class PersistentCacheStats extends AnnotatedStandardMBean implements PersistentCacheStatsMBean {

    private static final Boolean ENABLE_READ_TIMER;
    private static final Boolean ENABLE_LOAD_TIMER;
    private static final Boolean ENABLE_REJECTED_PUT;
    static {
        String enableReadTimer = System.getProperty("PersistentCacheStats.readTimer", "false");
        String enableLoadTimer = System.getProperty("PersistentCacheStats.loadTimer", "false");
        String enableRejectedPut = System.getProperty("PersistentCacheStats.rejectedPut", "false");
        ENABLE_READ_TIMER = Boolean.parseBoolean(enableReadTimer);
        ENABLE_LOAD_TIMER = Boolean.parseBoolean(enableLoadTimer);
        ENABLE_REJECTED_PUT = Boolean.parseBoolean(enableRejectedPut);
    }

    private static final String HITS = "HITS";
    private static final String REQUESTS = "REQUESTS";
    private static final String LOAD_TIMER = "LOAD_TIMER";
    private static final String LOAD_EXCEPTIONS = "LOAD_EXCEPTIONS";
    private static final String PUT_ONE = "CACHE_PUT";
    private static final String BROADCAST_RECV = "BROADCAST_RECV";
    private static final String INVALIDATE_ONE = "INVALIDATE_ONE";
    private static final String INVALIDATE_ALL = "INVALIDATE_ALL";
    private static final String READ_TIMER = "READ_TIMER";
    private static final String USED_DISK_SPACE = "USED_SPACE_BYTES";
    private static final String PUT_REJECTED_ALREADY_PERSISTED = "PUT_REJECTED_ALREADY_PERSISTED";
    private static final String PUT_REJECTED_ENTRY_NOT_USED = "PUT_REJECTED_ENTRY_NOT_USED";
    private static final String PUT_REJECTED_FULL_QUEUE = "PUT_REJECTED_FULL_QUEUE";
    private static final String PUT_REJECTED_SECONDARY_CACHE = "PUT_REJECTED_SECONDARY_CACHE";

    private final StatisticsProvider statisticsProvider;
    private final String cacheName;

    private final MeterStats hitMeter;
    private final TimeSeries hitRateHistory;

    private final MeterStats requestMeter;
    private final TimeSeries requestRateHistory;

    private final MeterStats loadExceptionMeter;
    private final TimeSeries loadExceptionRateHistory;

    private final TimerStats loadTimer;
    private final TimeSeries loadRateHistory;

    private final MeterStats putMeter;
    private final TimeSeries putRateHistory;

    private final MeterStats broadcastRecvMeter;
    private final TimeSeries broadcastRecvRateHistory;

    private final MeterStats invalidateOneMeter;
    private final TimeSeries invalidateOneRateHistory;

    private final MeterStats invalidateAllMeter;
    private final TimeSeries invalidateAllRateHistory;

    private final MeterStats putRejectedAlreadyPersistedMeter;
    private final TimeSeries putRejectedAlreadyPersistedHistory;

    private final MeterStats putRejectedEntryNotUsedMeter;
    private final TimeSeries putRejectedEntryNotUseHistory;

    private final MeterStats putRejectedByFullQueueMeter;
    private final TimeSeries putRejectedByFullQueueHistory;

    private final MeterStats putRejectedAsCachedInSecMeter;
    private final TimeSeries putRejectedAsCachedInSecHistory;

    private final TimerStats readTimer;

    private final CounterStats usedSpaceByteCounter;
    private final TimeSeries usedSpaceByteCounterHistory;

    private final UsedSpaceTracker diskStats;

    private final TimeSeries hitPercentageHistory;


    public PersistentCacheStats(CacheType cacheType, StatisticsProvider provider) {
        super(PersistentCacheStatsMBean.class);

        if (provider == null) {
            statisticsProvider = StatisticsProvider.NOOP;
        } else {
            statisticsProvider = provider;
        }

        // Configure cache name
        cacheName = "PersistentCache.NodeCache." + cacheType.name().toLowerCase();

        // Fetch stats and time series
        String statName;

        statName = getStatName(HITS, cacheName);
        hitMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        hitRateHistory = getTimeSeries(statName);

        statName = getStatName(REQUESTS, cacheName);
        requestMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        requestRateHistory = getTimeSeries(statName);
        hitPercentageHistory = new PercentageTimeSeries(hitRateHistory, requestRateHistory);

        statName = getStatName(LOAD_TIMER, cacheName);
        loadRateHistory = new DifferenceTimeSeries(requestRateHistory, hitRateHistory);
        if (ENABLE_LOAD_TIMER) {
            loadTimer = statisticsProvider.getTimer(statName, StatsOptions.METRICS_ONLY);
        }
        else {
            loadTimer = StatisticsProvider.NOOP.getTimer(statName, StatsOptions.METRICS_ONLY);
        }

        statName = getStatName(LOAD_EXCEPTIONS, cacheName);
        loadExceptionMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        loadExceptionRateHistory = getTimeSeries(statName);

        statName = getStatName(PUT_ONE, cacheName);
        putMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        putRateHistory = getTimeSeries(statName);

        statName = getStatName(BROADCAST_RECV, cacheName);
        broadcastRecvMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        broadcastRecvRateHistory = getTimeSeries(statName);

        statName = getStatName(INVALIDATE_ONE, cacheName);
        invalidateOneMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        invalidateOneRateHistory = getTimeSeries(statName);

        statName = getStatName(INVALIDATE_ALL, cacheName);
        invalidateAllMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        invalidateAllRateHistory = getTimeSeries(statName);

        statName = getStatName(USED_DISK_SPACE, cacheName);
        usedSpaceByteCounter = statisticsProvider.getCounterStats(statName, StatsOptions.DEFAULT);
        usedSpaceByteCounterHistory = getTimeSeries(statName, false);

        statName = getStatName(READ_TIMER, cacheName);
        if (ENABLE_READ_TIMER) {
            readTimer = statisticsProvider.getTimer(statName, StatsOptions.METRICS_ONLY);
        }
        else {
            readTimer = StatisticsProvider.NOOP.getTimer(statName, StatsOptions.METRICS_ONLY);
        }

        statName = getStatName(PUT_REJECTED_ALREADY_PERSISTED, cacheName);
        if (ENABLE_REJECTED_PUT) {
            putRejectedAlreadyPersistedMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
            putRejectedAlreadyPersistedHistory = getTimeSeries(statName);
        } else {
            putRejectedAlreadyPersistedMeter = StatisticsProvider.NOOP.getMeter(statName, StatsOptions.DEFAULT);
            putRejectedAlreadyPersistedHistory = StatisticsProvider.NOOP.getStats().getTimeSeries(statName, false);
        }

        statName = getStatName(PUT_REJECTED_ENTRY_NOT_USED, cacheName);
        if (ENABLE_REJECTED_PUT) {
            putRejectedEntryNotUsedMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
            putRejectedEntryNotUseHistory = getTimeSeries(statName);
        } else {
            putRejectedEntryNotUsedMeter = StatisticsProvider.NOOP.getMeter(statName, StatsOptions.DEFAULT);
            putRejectedEntryNotUseHistory = StatisticsProvider.NOOP.getStats().getTimeSeries(statName, false);
        }

        statName = getStatName(PUT_REJECTED_FULL_QUEUE, cacheName);
        if (ENABLE_REJECTED_PUT) {
            putRejectedByFullQueueMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
            putRejectedByFullQueueHistory = getTimeSeries(statName);
        } else {
            putRejectedByFullQueueMeter = StatisticsProvider.NOOP.getMeter(statName, StatsOptions.DEFAULT);
            putRejectedByFullQueueHistory = StatisticsProvider.NOOP.getStats().getTimeSeries(statName, false);
        }

        statName = getStatName(PUT_REJECTED_SECONDARY_CACHE, cacheName);
        putRejectedAsCachedInSecMeter = statisticsProvider.getMeter(statName, StatsOptions.DEFAULT);
        putRejectedAsCachedInSecHistory = getTimeSeries(statName);

        diskStats = new UsedSpaceTracker(usedSpaceByteCounter);
    }

    //~--------------------------------------< stats update methods

    public void markHit() {
        hitMeter.mark();
    }

    public void markRequest() {
        requestMeter.mark();
    }

    public void markException() {
        loadExceptionMeter.mark();
    }

    public void markPut() {
        putMeter.mark();
    }

    public void markRecvBroadcast() {
        broadcastRecvMeter.mark();
    }

    public void markInvalidateOne() {
        invalidateOneMeter.mark();
    }

    public void markInvalidateAll() {
        invalidateAllMeter.mark();
    }

    public void markPutRejectedAlreadyPersisted() {
        putRejectedAlreadyPersistedMeter.mark();
    }

    public void markPutRejectedEntryNotUsed() {
        putRejectedEntryNotUsedMeter.mark();
    }

    public void markPutRejectedAsCachedInSecondary() {
        putRejectedAsCachedInSecMeter.mark();
    }

    public void markPutRejectedQueueFull() {
        putRejectedByFullQueueMeter.mark();
    }

    public TimerStats.Context startReadTimer() {
        return this.readTimer.time();
    }

    public TimerStats.Context startLoaderTimer() {
        return this.loadTimer.time();
    }

    // Update disk space

    public void addWriteGeneration(int generation) {
        diskStats.addWriteGeneration(generation);
    }

    public void removeReadGeneration(int generation) {
        diskStats.removeReadGeneration(generation);
    }

    public void markBytesWritten(long numBytes) {
        diskStats.markBytesWritten(numBytes);
    }

    //~--------------------------------------< diskspace usage helper

    static class UsedSpaceTracker {
        private final CounterStats byteCounter;

        private final Map<Integer, AtomicLong> generationByteCounters;

        private AtomicLong currentGenCounter;

        UsedSpaceTracker(CounterStats usageCounter) {
            this.byteCounter = usageCounter;
            this.generationByteCounters = new ConcurrentHashMap<Integer, AtomicLong>();
            this.currentGenCounter = new AtomicLong();
        }

        void addWriteGeneration(int generation) {
            currentGenCounter = new AtomicLong(0L);
            generationByteCounters.put(generation, currentGenCounter);
        }

        void removeReadGeneration(int generation) {
            AtomicLong genCounter = generationByteCounters.remove(generation);
            byteCounter.dec(genCounter == null ? 0L : genCounter.get());
        }

        void markBytesWritten(long bytes) {
            currentGenCounter.addAndGet(bytes);
            byteCounter.inc(bytes);
        }
    }

    //~--------------------------------------< CacheStatsMbean

    @Override
    public String getName() {
        return cacheName;
    }

    @Override
    public long getRequestCount() {
        return requestMeter.getCount();
    }

    @Override
    public long getHitCount() {
        return hitMeter.getCount();
    }

    @Override
    public double getHitRate() {
        long hitCount = hitMeter.getCount();
        long requestCount = requestMeter.getCount();
        return (requestCount == 0L ? 0L : (double)hitCount/requestCount);
    }

    @Override
    public long getMissCount() {
        return requestMeter.getCount() - hitMeter.getCount();
    }

    @Override
    public double getMissRate() {
        long missCount = getMissCount();
        long requestCount = requestMeter.getCount();
        return (requestCount == 0L ? 0L : (double)missCount/requestCount);
    }

    @Override
    public long getLoadCount() {
        return getMissCount();
    }

    @Override
    public long getLoadSuccessCount() {
        return getLoadCount() - getLoadExceptionCount();
    }

    @Override
    public long getLoadExceptionCount() {
        return loadExceptionMeter.getCount();
    }

    @Override
    public double getLoadExceptionRate() {
        long exceptionCount = loadExceptionMeter.getCount();
        long loadCount = loadTimer.getCount();
        return (loadCount == 0L ? 0L : (double)exceptionCount/loadCount);
    }

    @Override
    public long estimateCurrentWeight() {
        return usedSpaceByteCounter.getCount();
    }


    @Override
    public CompositeData getRequestRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(requestRateHistory, "Persistent cache requests");
    }

    @Override
    public CompositeData getHitRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(hitRateHistory, "Persistent cache hits");
    }

    @Override
    public CompositeData getLoadRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(loadRateHistory, "Persistent cache loads/misses");
    }

    @Override
    public CompositeData getLoadExceptionRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(loadExceptionRateHistory, "Persistent cache load exceptions");
    }

    @Override
    public CompositeData getHitPercentageHistory() {
        return TimeSeriesStatsUtil.asCompositeData(hitPercentageHistory, "Persistent cache hit percentage");
    }

    @Override
    public CompositeData getPutRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(putRateHistory, "Persistent cache manual put entry");
    }

    @Override
    public CompositeData getPutRejectedAlreadyPersistedRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(putRejectedAlreadyPersistedHistory, "Persistent cache put rejected (already persisted)");
    }

    @Override
    public CompositeData getPutRejectedEntryNotUsedRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(putRejectedEntryNotUseHistory, "Persistent cache put rejected (entry not used)");
    }

    @Override
    public CompositeData getPutRejectedQueueFullRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(putRejectedByFullQueueHistory, "Persistent cache put rejected (queue is full)");
    }

    @Override
    public CompositeData getPutRejectedAsCachedInSecRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(putRejectedAsCachedInSecHistory, "Persistent cache put rejected " +
                "(entry is covered by secondary)");
    }

    @Override
    public CompositeData getInvalidateOneRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(invalidateOneRateHistory, "Persistent cache invalidate one entry");
    }

    @Override
    public CompositeData getInvalidateAllRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(invalidateAllRateHistory, "Persistent cache invalidate all entries");
    }

    @Override
    public CompositeData getBroadcastRecvRateHistory() {
        return TimeSeriesStatsUtil.asCompositeData(broadcastRecvRateHistory, "Persistent cache entries received from broadcast");
    }

    @Override
    public CompositeData getUsedSpaceHistory() {
        return TimeSeriesStatsUtil.asCompositeData(usedSpaceByteCounterHistory, "Persistent cache estimated size (bytes)");
    }

    @Override
    public String cacheInfoAsString() {
        return Objects.toStringHelper("PersistentCacheStats")
                .add("requestCount", getRequestCount())
                .add("hitCount", getHitCount())
                .add("hitRate", String.format("%1.2f", getHitRate()))
                .add("missCount", getMissCount())
                .add("missRate", String.format("%1.2f", getMissRate()))
                .add("loadCount", getLoadCount())
                .add("loadSuccessCount", getLoadSuccessCount())
                .add("loadExceptionCount", getLoadExceptionCount())
                .add("totalWeight", IOUtils.humanReadableByteCount(estimateCurrentWeight()))
                .toString();
    }

    //~--------------------------------------< CacheStatsMbean - stats that are not (yet) available
    @Override
    public long getTotalLoadTime() {
        return 0;
    }

    @Override
    public double getAverageLoadPenalty() {
        return 0;
    }

    @Override
    public long getEvictionCount() {
        return 0;
    }

    @Override
    public long getElementCount() {
        return 0;
    }

    @Override
    public long getMaxTotalWeight() {
        return Long.MAX_VALUE;
    }

    @Override
    public void resetStats() {
        // ignored
    }

    Counting getPutRejectedAsCachedInSecCounter() {
        return putRejectedAsCachedInSecMeter;
    }

    //~--------------------------------------< private helpers

    private static String getStatName(String meter, String cacheName) {
        return cacheName + "." + meter;
    }

    private TimeSeries getTimeSeries(String name) {
        return statisticsProvider.getStats().getTimeSeries(name, true);
    }

    private TimeSeries getTimeSeries(String name, boolean resetValues) {
        return statisticsProvider.getStats().getTimeSeries(name, resetValues);
    }


    /**
     * TimeSeries that computes the hit ratio in percentages. ( hit/total * 100 )
     */
    private static class PercentageTimeSeries implements TimeSeries {

        private TimeSeries hit, total;

        PercentageTimeSeries(TimeSeries hit, TimeSeries total) {
            this.hit = hit;
            this.total = total;
        }

        @Override
        public long[] getValuePerSecond() {
            return percentage(hit.getValuePerSecond(), total.getValuePerSecond());
        }

        @Override
        public long[] getValuePerMinute() {
            return percentage(hit.getValuePerMinute(), total.getValuePerMinute());
        }

        @Override
        public long[] getValuePerHour() {
            return percentage(hit.getValuePerHour(), total.getValuePerHour());
        }

        @Override
        public long[] getValuePerWeek() {
            return percentage(hit.getValuePerWeek(), total.getValuePerWeek());
        }

        @Override
        public long getMissingValue() {
            return 0;
        }

        private static long[] percentage(long[] a, long[] b) {
            long[] result = new long[a.length];
            for (int i=0; i<a.length; ++i) {
                if (b[i] == 0) {
                    result[i] = 0;
                }
                else {
                    result[i] = (a[i] * 100) / b[i];
                }
            }
            return result;
        }
    }

    /**
     * TimeSeries as a difference between two other TimeSeries
     */
    private static class DifferenceTimeSeries implements TimeSeries {

        private TimeSeries tsA, tsB;

        DifferenceTimeSeries(TimeSeries tsA, TimeSeries tsB) {
            this.tsA = tsA;
            this.tsB = tsB;
        }

        @Override
        public long[] getValuePerSecond() {
            return difference(tsA.getValuePerSecond(), tsB.getValuePerSecond());
        }

        @Override
        public long[] getValuePerMinute() {
            return difference(tsA.getValuePerMinute(), tsB.getValuePerMinute());
        }

        @Override
        public long[] getValuePerHour() {
            return difference(tsA.getValuePerHour(), tsB.getValuePerHour());
        }

        @Override
        public long[] getValuePerWeek() {
            return difference(tsA.getValuePerWeek(), tsB.getValuePerWeek());
        }

        @Override
        public long getMissingValue() {
            return 0;
        }

        private static long[] difference(long[] a, long[] b) {
            long[] result = new long[a.length];
            for (int i=0; i<a.length; ++i) {
                result[i] = a[i] - b[i];
            }
            return result;
        }
    }
}