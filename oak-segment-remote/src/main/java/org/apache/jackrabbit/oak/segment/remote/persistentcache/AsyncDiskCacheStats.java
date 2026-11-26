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

package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.GaugeStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

final class AsyncDiskCacheStats {
    static final String OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_EVICTED_PENDING = "oak.segment.cache.disk.async.segment-evicted-pending";
    static final String OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_SKIPPED_EVICTED = "oak.segment.cache.disk.async.segment-write-skipped-evicted";
    static final String OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_SKIPPED_SUPERSEDED = "oak.segment.cache.disk.async.segment-write-skipped-superseded";
    static final String OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_ORPHAN_CLEANUPS = "oak.segment.cache.disk.async.segment-write-orphan-cleanups";
    static final String OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_ORPHAN_BYTES = "oak.segment.cache.disk.async.segment-write-orphan-bytes";
    static final String ASYNC_QUEUED_ENTRIES = "oak.segment.cache.disk.async.queued-entries";
    static final String ASYNC_QUEUED_BYTES = "oak.segment.cache.disk.async.queued-bytes";
    static final String ASYNC_ENQUEUE_TIME = "oak.segment.cache.disk.async.enqueue-time";
    static final String ASYNC_QUEUE_FULL = "oak.segment.cache.disk.async.queue-full";
    static final String ASYNC_QUEUE_RESIDENCE_TIME = "oak.segment.cache.disk.async.queue-residence-time";
    static final String ASYNC_RELEASE_AGE = "oak.segment.cache.disk.async.release-age";
    static final String ASYNC_RELEASE_PRESSURE = "oak.segment.cache.disk.async.release-pressure";
    static final String ASYNC_RELEASE_SHUTDOWN = "oak.segment.cache.disk.async.release-shutdown";

    private final CounterStats evictedPending;
    private final CounterStats writesSkippedEvicted;
    private final CounterStats writesSkippedSuperseded;
    private final CounterStats orphanCleanups;
    private final CounterStats orphanBytes;
    private final AtomicLong queuedEntries;
    private final AtomicLong queuedBytes;
    private final TimerStats enqueueTime;
    private final TimerStats queueResidenceTime;
    private final CounterStats queueFull;
    private final CounterStats releaseAge;
    private final CounterStats releasePressure;
    private final CounterStats releaseShutdown;

    AsyncDiskCacheStats(StatisticsProvider statisticsProvider) {
        evictedPending = statisticsProvider.getCounterStats(
                OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_EVICTED_PENDING, StatsOptions.METRICS_ONLY);
        writesSkippedEvicted = statisticsProvider.getCounterStats(
                OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_SKIPPED_EVICTED, StatsOptions.METRICS_ONLY);
        writesSkippedSuperseded = statisticsProvider.getCounterStats(
                OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_SKIPPED_SUPERSEDED, StatsOptions.METRICS_ONLY);
        orphanCleanups = statisticsProvider.getCounterStats(
                OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_ORPHAN_CLEANUPS, StatsOptions.METRICS_ONLY);
        orphanBytes = statisticsProvider.getCounterStats(
                OAK_SEGMENT_CACHE_DISK_ASYNC_SEGMENT_WRITE_ORPHAN_BYTES, StatsOptions.METRICS_ONLY);
        queuedEntries = registerGauge(statisticsProvider, ASYNC_QUEUED_ENTRIES);
        queuedBytes = registerGauge(statisticsProvider, ASYNC_QUEUED_BYTES);
        enqueueTime = statisticsProvider.getTimer(ASYNC_ENQUEUE_TIME, StatsOptions.METRICS_ONLY);
        queueResidenceTime = statisticsProvider.getTimer(ASYNC_QUEUE_RESIDENCE_TIME, StatsOptions.METRICS_ONLY);
        queueFull = statisticsProvider.getCounterStats(ASYNC_QUEUE_FULL, StatsOptions.METRICS_ONLY);
        releaseAge = statisticsProvider.getCounterStats(ASYNC_RELEASE_AGE, StatsOptions.METRICS_ONLY);
        releasePressure = statisticsProvider.getCounterStats(ASYNC_RELEASE_PRESSURE, StatsOptions.METRICS_ONLY);
        releaseShutdown = statisticsProvider.getCounterStats(ASYNC_RELEASE_SHUTDOWN, StatsOptions.METRICS_ONLY);
    }

    private static AtomicLong registerGauge(StatisticsProvider provider, String name) {
        AtomicLong value = new AtomicLong();
        GaugeStats<AtomicLong> gauge = provider.getGauge(name, () -> value);
        // Providers retain the first registration. Reuse its value when a cache is reopened.
        return gauge == null ? value : gauge.getValue();
    }

    void recordPendingEviction() {
        evictedPending.inc();
    }

    void recordWriteSkipped(boolean evicted) {
        if (evicted) {
            writesSkippedEvicted.inc();
        } else {
            writesSkippedSuperseded.inc();
        }
    }

    void recordEnqueue(long elapsedNanos) {
        enqueueTime.update(elapsedNanos, NANOSECONDS);
    }

    void recordQueueFull() {
        queueFull.inc();
    }

    void recordQueued(int bytes) {
        queuedBytes.addAndGet(bytes);
        queuedEntries.incrementAndGet();
    }

    void recordRemovedFromQueue(int bytes) {
        queuedEntries.decrementAndGet();
        queuedBytes.addAndGet(-bytes);
    }

    void recordDequeued(int bytes, long residenceNanos, PendingWriteQueue.ReleaseReason reason) {
        recordRemovedFromQueue(bytes);
        queueResidenceTime.update(residenceNanos, NANOSECONDS);
        switch (reason) {
            case SHUTDOWN:
                releaseShutdown.inc();
                break;
            case AGE:
                releaseAge.inc();
                break;
            case PRESSURE:
                releasePressure.inc();
                break;
        }
    }

    /** Records a completed write requiring orphan cleanup, regardless of the deletion outcome. */
    void recordOrphanCleanup(int bytes) {
        orphanBytes.inc(bytes);
        orphanCleanups.inc();
    }

    long getEvictedPendingCount() {
        return evictedPending.getCount();
    }

    long getWritesSkippedEvictedCount() {
        return writesSkippedEvicted.getCount();
    }

    long getWritesSkippedSupersededCount() {
        return writesSkippedSuperseded.getCount();
    }

    long getOrphanCleanupsCount() {
        return orphanCleanups.getCount();
    }

    long getOrphanBytesCount() {
        return orphanBytes.getCount();
    }
}
