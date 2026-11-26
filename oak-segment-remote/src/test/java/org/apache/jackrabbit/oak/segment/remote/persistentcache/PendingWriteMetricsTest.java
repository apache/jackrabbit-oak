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
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.segment.remote.persistentcache.PersistentDiskCacheV2Async.PendingSegment;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.GaugeStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_ENQUEUE_TIME;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_QUEUED_BYTES;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_QUEUED_ENTRIES;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_QUEUE_FULL;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_QUEUE_RESIDENCE_TIME;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_RELEASE_AGE;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_RELEASE_PRESSURE;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.AsyncDiskCacheStats.ASYNC_RELEASE_SHUTDOWN;
import static org.apache.jackrabbit.oak.segment.remote.persistentcache.PersistentDiskCacheV2AsyncRaceTest.ControllablePersistentDiskCacheV2Async;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PendingWriteMetricsTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final ScheduledExecutorService statisticsExecutor = Executors.newSingleThreadScheduledExecutor();
    private final RecordingStatisticsProvider statistics = new RecordingStatisticsProvider();
    private final DiskCacheIOMonitor monitor = new DiskCacheIOMonitor(statistics);
    private final ExecutorService producers = Executors.newCachedThreadPool();
    private ControllablePersistentDiskCacheV2Async cache;

    @After
    public void tearDown() throws InterruptedException {
        producers.shutdownNow();
        assertTrue(producers.awaitTermination(5, TimeUnit.SECONDS));
        if (cache != null) {
            cache.close();
        }
        statisticsExecutor.shutdownNow();
    }

    @Test
    public void creatingDiskMonitorDoesNotRegisterAsyncMetrics() {
        assertTrue(statistics.gauges.isEmpty());
        assertFalse(statistics.timers.containsKey(ASYNC_ENQUEUE_TIME));
        monitor.asyncStats();
        assertQueue(0, 0);
        assertTrue(statistics.timers.containsKey(ASYNC_ENQUEUE_TIME));
    }

    @Test
    public void pressureAndShutdownReleasesBalanceQueueMetricsAndExcludeControlEntries() throws Exception {
        PendingWriteQueue queue = queue(4, 3, 60_000);
        assertQueue(0, 0);
        queue.put(segment(3));
        queue.put(segment(5));
        queue.put(segment(7));
        assertQueue(3, 15);
        assertEquals(3, queue.take().buffer.remaining());
        assertQueue(2, 12);
        assertEquals(1, count(ASYNC_RELEASE_PRESSURE));
        assertEquals(0, count(ASYNC_RELEASE_AGE));
        queue.startDraining();
        assertEquals(5, queue.take().buffer.remaining());
        assertEquals(7, queue.take().buffer.remaining());
        // Successful and timed-out shutdown offers must not look like segment enqueues.
        PendingSegment sentinel = new PendingSegment(new UUID(0, 0), null);
        assertTrue(queue.offer(sentinel, 0, TimeUnit.SECONDS));
        assertQueue(0, 0);
        assertSame(sentinel, queue.take());
        assertEquals(2, count(ASYNC_RELEASE_SHUTDOWN));
        assertTimerCount(ASYNC_ENQUEUE_TIME, 3);
        assertTimerCount(ASYNC_QUEUE_RESIDENCE_TIME, 3);
        assertEquals(0, count(ASYNC_QUEUE_FULL));
        assertQueue(0, 0);
    }

    @Test
    public void ageReleaseRecordsActualResidenceIncludingZeroDelay() throws Exception {
        PendingWriteQueue delayed = queue(4, 3, 100);
        delayed.put(segment(2));
        assertEquals(2, delayed.take().buffer.remaining());
        ArgumentCaptor<Long> residence = ArgumentCaptor.forClass(Long.class);
        verify(statistics.timers.get(ASYNC_QUEUE_RESIDENCE_TIME)).update(residence.capture(), eq(TimeUnit.NANOSECONDS));
        assertTrue(residence.getValue() >= TimeUnit.MILLISECONDS.toNanos(100));
        PendingWriteQueue immediate = queue(1, 1, 0);
        immediate.put(segment(0));
        assertQueue(1, 0);
        assertEquals(0, immediate.take().buffer.remaining());
        // Age wins over pressure when both conditions hold.
        assertEquals(2, count(ASYNC_RELEASE_AGE));
        assertEquals(0, count(ASYNC_RELEASE_PRESSURE));
        assertTimerCount(ASYNC_QUEUE_RESIDENCE_TIME, 2);
        assertQueue(0, 0);
    }

    @Test
    public void fullCapacityIsCountedOncePerProducerAndEnqueueTimeIncludesWaiting() throws Exception {
        PendingWriteQueue queue = queue(1, 1, 0);
        queue.put(segment(3));
        Future<?> producer = producers.submit(() -> {
            queue.put(segment(5));
            return null;
        });
        await().atMost(5, TimeUnit.SECONDS).until(() -> count(ASYNC_QUEUE_FULL) == 1);
        assertFalse(producer.isDone());
        assertQueue(1, 3); // Blocked producers are not queued yet.
        assertThrows(TimeoutException.class, () -> producer.get(100, TimeUnit.MILLISECONDS));
        assertEquals(3, queue.take().buffer.remaining());
        producer.get(5, TimeUnit.SECONDS);
        assertQueue(1, 5);
        assertEquals(5, queue.take().buffer.remaining());
        ArgumentCaptor<Long> elapsed = ArgumentCaptor.forClass(Long.class);
        verify(statistics.timers.get(ASYNC_ENQUEUE_TIME), times(2)).update(elapsed.capture(), eq(TimeUnit.NANOSECONDS));
        assertTrue(elapsed.getAllValues().get(1) >= TimeUnit.MILLISECONDS.toNanos(100));
        assertEquals(1, count(ASYNC_QUEUE_FULL));
        assertQueue(0, 0);
    }

    @Test
    public void interruptedEnqueueIsTimedWithoutChangingOccupancy() throws Exception {
        PendingWriteQueue queue = queue(1, 1, 0);
        queue.put(segment(3));
        CountDownLatch interrupted = new CountDownLatch(1);
        Future<?> producer = producers.submit(() -> {
            try {
                queue.put(segment(5));
                fail("Expected interruption");
            } catch (InterruptedException expected) {
                interrupted.countDown();
            }
        });
        await().atMost(5, TimeUnit.SECONDS).until(() -> count(ASYNC_QUEUE_FULL) == 1);
        producer.cancel(true);
        assertTrue(interrupted.await(5, TimeUnit.SECONDS));
        assertTimerCount(ASYNC_ENQUEUE_TIME, 2);
        assertQueue(1, 3);
        assertFalse(queue.offer(segment(7), 0, TimeUnit.SECONDS));
        assertTimerCount(ASYNC_ENQUEUE_TIME, 2);
        assertEquals(1, count(ASYNC_QUEUE_FULL));
        assertEquals(3, queue.take().buffer.remaining());
        assertQueue(0, 0);
    }

    @Test
    public void cancelledBuffersRemainAccountedUntilDequeuedAndSkipsAreCountedOnce() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 8, 0, 6);
        cache.blockNext("blocker");
        cache.enqueue("blocker", ByteBuffer.wrap(new byte[2]));
        cache.awaitEntered("blocker");
        cache.enqueue("evicted", ByteBuffer.wrap(new byte[3]));
        assertTrue(cache.discard("evicted"));
        cache.enqueue("superseded", ByteBuffer.wrap(new byte[4]));
        cache.enqueue("superseded", ByteBuffer.wrap(new byte[5]));
        assertQueue(3, 12);
        assertEquals(0, monitor.asyncStats().getWritesSkippedEvictedCount());
        assertEquals(0, monitor.asyncStats().getWritesSkippedSupersededCount());
        cache.release("blocker");
        cache.close();
        assertQueue(0, 0);
        assertEquals(1, monitor.asyncStats().getWritesSkippedEvictedCount());
        assertEquals(1, monitor.asyncStats().getWritesSkippedSupersededCount());
        assertTimerCount(ASYNC_ENQUEUE_TIME, 4);
        assertTimerCount(ASYNC_QUEUE_RESIDENCE_TIME, 4);
        assertEquals(4, count(ASYNC_RELEASE_AGE) + count(ASYNC_RELEASE_SHUTDOWN));
    }

    @Test
    public void concurrentWriteFailuresLeaveQueueEmptyWithoutCountingSkips() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 2, 8, 0, 6);
        cache.blockNext("first");
        cache.blockNext("second");
        cache.enqueue("first", ByteBuffer.wrap(new byte[2]));
        cache.enqueue("second", ByteBuffer.wrap(new byte[3]));
        cache.awaitEntered("first");
        cache.awaitEntered("second");
        assertQueue(0, 0);
        cache.throwOnWrite = true;
        cache.release("first");
        cache.release("second");
        cache.close();
        assertQueue(0, 0);
        assertEquals(0, monitor.asyncStats().getWritesSkippedEvictedCount());
        assertEquals(0, monitor.asyncStats().getWritesSkippedSupersededCount());
        assertTimerCount(ASYNC_ENQUEUE_TIME, 2);
        assertTimerCount(ASYNC_QUEUE_RESIDENCE_TIME, 2);
    }

    @Test
    public void gaugesRemainLiveWhenMonitorIsRecreatedWithSameProvider() {
        DiskCacheIOMonitor reopened = new DiskCacheIOMonitor(statistics);
        reopened.asyncStats().recordQueued(3);
        assertQueue(1, 3);
        monitor.asyncStats().recordQueued(5);
        assertQueue(2, 8);
        reopened.asyncStats().recordDequeued(3, 0, PendingWriteQueue.ReleaseReason.AGE);
        monitor.asyncStats().recordDequeued(5, 0, PendingWriteQueue.ReleaseReason.AGE);
        assertQueue(0, 0);
    }

    @Test
    public void saturatedCloseDiscardsBacklogAndPreservesOtherQueuesOnReopen() throws Exception {
        assertForcedCloseBalancesGauges(false);
    }

    @Test
    public void discardingPendingEntriesExcludesControlEntriesAndIsIdempotent() throws Exception {
        PendingWriteQueue queue = queue(4, 3, 60_000);
        PendingSegment pending = segment(3);
        queue.put(pending);
        assertTrue(queue.offer(new PendingSegment(new UUID(0, 0), null), 0, TimeUnit.SECONDS));
        assertQueue(1, 3);
        assertEquals(List.of(pending), queue.discardPending());
        assertQueue(0, 0);
        assertEquals(0, queue.size());
        assertTrue(queue.discardPending().isEmpty());
        assertQueue(0, 0);
        assertThrows(IllegalStateException.class, () -> queue.put(segment(5)));
        assertTimerCount(ASYNC_QUEUE_RESIDENCE_TIME, 0);
        assertEquals(0, count(ASYNC_RELEASE_SHUTDOWN));
    }

    @Test
    public void interruptedCloseDiscardsBacklogAndPreservesOtherQueuesOnReopen() throws Exception {
        assertForcedCloseBalancesGauges(true);
    }

    private void assertForcedCloseBalancesGauges(boolean interruptClose) throws Exception {
        PendingWriteQueue otherQueue = queue(4, 3, 60_000);
        otherQueue.put(segment(7));
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 1, 0, 1);
        cache.blockNext("blocker");
        cache.enqueue("blocker", ByteBuffer.allocate(2));
        cache.awaitEntered("blocker");
        cache.enqueue("abandoned", ByteBuffer.allocate(3));
        Future<?> producer = producers.submit(() ->
                assertThrows(IllegalStateException.class, () -> cache.enqueue("blocked", ByteBuffer.allocate(5))));
        await().atMost(5, TimeUnit.SECONDS).until(() -> count(ASYNC_QUEUE_FULL) == 1);
        assertQueue(2, 10);
        if (interruptClose) {
            Future<Boolean> closer = producers.submit(() -> {
                Thread.currentThread().interrupt();
                try {
                    cache.close();
                    return Thread.currentThread().isInterrupted();
                } finally {
                    Thread.interrupted();
                }
            });
            assertTrue(closer.get(5, TimeUnit.SECONDS));
        } else {
            cache.close();
        }
        producer.get(5, TimeUnit.SECONDS);
        assertQueue(1, 7);
        assertEquals(0, cache.queueSize());
        await().atMost(5, TimeUnit.SECONDS).until(() -> cache.pendingSegmentsSize() == 0);
        assertTimerCount(ASYNC_QUEUE_RESIDENCE_TIME, 1);
        assertEquals(0, count(ASYNC_RELEASE_SHUTDOWN));

        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(),
                new DiskCacheIOMonitor(statistics), 1, 4, 0, 3);
        assertQueue(1, 7);
        cache.enqueue("reopened", ByteBuffer.allocate(11));
        await().atMost(5, TimeUnit.SECONDS).until(() -> cache.pendingSegmentsSize() == 0);
        assertQueue(1, 7);
        cache.close();
        otherQueue.startDraining();
        assertEquals(7, otherQueue.take().buffer.remaining());
        assertQueue(0, 0);
    }

    private PendingWriteQueue queue(int capacity, int threshold, long delayMillis) {
        return new PendingWriteQueue(capacity, threshold, delayMillis, monitor.asyncStats());
    }

    private static PendingSegment segment(int bytes) {
        return new PendingSegment(new UUID(0, bytes), ByteBuffer.allocate(bytes));
    }

    private long count(String name) {
        return statistics.getCounterStats(name, StatsOptions.METRICS_ONLY).getCount();
    }

    private long gauge(String name) {
        return ((Number) statistics.gauges.get(name).get()).longValue();
    }

    private void assertQueue(long entries, long bytes) {
        assertEquals("queued entries", entries, gauge(ASYNC_QUEUED_ENTRIES));
        assertEquals("queued bytes", bytes, gauge(ASYNC_QUEUED_BYTES));
    }

    private void assertTimerCount(String name, int count) {
        verify(statistics.timers.get(name), times(count)).update(anyLong(), eq(TimeUnit.NANOSECONDS));
    }

    private class RecordingStatisticsProvider implements StatisticsProvider {
        private final StatisticsProvider delegate = new DefaultStatisticsProvider(statisticsExecutor);
        final Map<String, Supplier<?>> gauges = new HashMap<>();
        final Map<String, TimerStats> timers = new HashMap<>();

        @Override
        public RepositoryStatistics getStats() {
            return delegate.getStats();
        }

        @Override
        public MeterStats getMeter(String name, StatsOptions options) {
            return delegate.getMeter(name, options);
        }

        @Override
        public CounterStats getCounterStats(String name, StatsOptions options) {
            return delegate.getCounterStats(name, options);
        }

        @Override
        public HistogramStats getHistogram(String name, StatsOptions options) {
            return delegate.getHistogram(name, options);
        }

        @Override
        public <T> GaugeStats<T> getGauge(String name, Supplier<T> supplier) {
            GaugeStats<T> gauge = delegate.getGauge(name, supplier);
            gauges.put(name, gauge::getValue);
            return gauge;
        }

        @Override
        public TimerStats getTimer(String name, StatsOptions options) {
            return timers.computeIfAbsent(name, key -> mock(TimerStats.class));
        }
    }
}
