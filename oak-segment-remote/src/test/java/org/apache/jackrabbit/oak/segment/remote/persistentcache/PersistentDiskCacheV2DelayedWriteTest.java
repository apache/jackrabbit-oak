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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.segment.remote.persistentcache.PersistentDiskCacheV2AsyncRaceTest.ControllablePersistentDiskCacheV2Async;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PersistentDiskCacheV2DelayedWriteTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final String previousDelay = System.getProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP);
    private final String previousThreshold = System.getProperty(PersistentDiskCacheV2Async.PREEMPTIVE_FLUSH_SIZE_PROP);

    private final ScheduledExecutorService statisticsExecutor = Executors.newSingleThreadScheduledExecutor();
    private final DiskCacheIOMonitor monitor = new DiskCacheIOMonitor(new DefaultStatisticsProvider(statisticsExecutor));
    private ControllablePersistentDiskCacheV2Async cache;

    @After
    public void tearDown() {
        if (cache != null) {
            cache.close();
        }
        statisticsExecutor.shutdownNow();
        restoreProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP, previousDelay);
        restoreProperty(PersistentDiskCacheV2Async.PREEMPTIVE_FLUSH_SIZE_PROP, previousThreshold);
    }

    @Test
    public void pendingBufferRemainsReadableAndEventuallyPersistsWithoutMoreReads() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 8, 500, 6);
        cache.realWrite = true;
        UUID id = UUID.randomUUID();
        ByteBuffer data = ByteBuffer.wrap(new byte[]{1, 2, 3});
        long before = System.nanoTime();
        assertEquals(data, cache.readSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(), () -> data));
        assertEquals(data, cache.readSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(), () -> {
            fail("Pending buffer should satisfy the hit");
            return null;
        }));
        assertTrue(cache.doWriteCalls.isEmpty());
        assertFalse(Files.exists(cache.getSegmentPath(id.toString())));
        await().atMost(5, TimeUnit.SECONDS).until(() -> Files.exists(cache.getSegmentPath(id.toString())));
        assertTrue(System.nanoTime() - before >= TimeUnit.MILLISECONDS.toNanos(500));
        cache.close();
        assertEquals(List.of(id), cache.doWriteCalls);
        assertEquals(0, cache.pendingSegmentsSize());
    }

    @Test
    public void evictionDuringDelayAvoidsDiskWrite() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 8, 60_000, 2);
        cache.enqueue("discarded", ByteBuffer.wrap(new byte[]{1}));
        assertTrue(cache.discard("discarded"));
        cache.enqueue("survivor", ByteBuffer.wrap(new byte[]{2}));
        await().atMost(5, TimeUnit.SECONDS).until(() -> monitor.asyncStats().getWritesSkippedEvictedCount() == 1);
        assertTrue(cache.doWriteCalls.isEmpty());
        assertEquals(0, monitor.asyncStats().getOrphanBytesCount());
        assertEquals(1, cache.pendingSegmentsSize());
        cache.close();
        assertEquals(List.of(id("survivor")), cache.doWriteCalls);
    }

    @Test
    public void cacheEvictionBeforeDeadlineSkipsPersistence() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 8, 250, 6);
        UUID id = UUID.randomUUID();
        // The cache holds one MiB; this entry must be rejected by its actual eviction policy.
        ByteBuffer oversized = ByteBuffer.wrap(new byte[2 * 1024 * 1024]);
        assertEquals(oversized, cache.readSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(), () -> oversized));
        await().atMost(5, TimeUnit.SECONDS).until(() -> monitor.asyncStats().getWritesSkippedEvictedCount() == 1);
        assertFalse(cache.containsSegment(id.getMostSignificantBits(), id.getLeastSignificantBits()));
        assertTrue(cache.doWriteCalls.isEmpty());
        assertEquals(1, monitor.asyncStats().getEvictedPendingCount());
        assertEquals(0, monitor.asyncStats().getOrphanBytesCount());
    }

    @Test
    public void pressureWritesOldestBeforeCapacityAndThenResumesDelay() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 8, 60_000, 3);
        cache.blockNext("first");
        cache.enqueue("first", ByteBuffer.wrap(new byte[]{1}));
        cache.enqueue("second", ByteBuffer.wrap(new byte[]{2}));
        assertTrue(cache.doWriteCalls.isEmpty());
        cache.enqueue("third", ByteBuffer.wrap(new byte[]{3}));
        cache.awaitEntered("first");
        assertEquals(2, cache.queueSize());
        cache.release("first");
        await().atMost(5, TimeUnit.SECONDS).until(() -> cache.pendingSegmentsSize() == 2);
        assertEquals(List.of(id("first")), cache.doWriteCalls);
        cache.close();
        assertEquals(List.of(id("first"), id("second"), id("third")), cache.doWriteCalls);
    }

    @Test
    public void closeBypassesLongDelayForMultipleWriters() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 3, 8, 60_000, 6);
        cache.enqueue("first", ByteBuffer.wrap(new byte[]{1}));
        cache.enqueue("second", ByteBuffer.wrap(new byte[]{2}));
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            closer.submit(cache::close).get(5, TimeUnit.SECONDS);
        } finally {
            closer.shutdownNow();
        }
        assertEquals(2, cache.doWriteCalls.size());
        assertEquals(0, cache.queueSize());
        assertEquals(0, cache.pendingSegmentsSize());
    }

    @Test
    public void concurrentProducersAndWritersDrainEveryEntryExactlyOnce() throws Exception {
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 3, 8, 100, 6);
        ExecutorService producers = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                String name = "segment-" + i;
                futures.add(producers.submit(() -> cache.enqueue(name, ByteBuffer.wrap(new byte[]{1}))));
            }
            for (Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
            cache.close();
            assertEquals(100, cache.doWriteCalls.size());
            assertEquals(100, cache.doWriteCalls.stream().distinct().count());
            assertEquals(0, cache.queueSize());
            assertEquals(0, cache.pendingSegmentsSize());
        } finally {
            producers.shutdownNow();
        }
    }

    @Test
    public void defaultDelayIsOneHundredMillis() throws Exception {
        System.clearProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP);
        AtomicLong writeStarted = new AtomicLong();
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 8) {
            @Override
            protected void doWrite(UUID segmentId, ByteBuffer buffer) {
                writeStarted.set(System.nanoTime());
            }
        };
        long before = System.nanoTime();
        cache.enqueue("first", ByteBuffer.wrap(new byte[]{1}));
        await().atMost(5, TimeUnit.SECONDS).until(() -> writeStarted.get() != 0);
        assertTrue(writeStarted.get() - before >= TimeUnit.MILLISECONDS.toNanos(100));
    }

    @Test
    public void systemPropertiesEnableDelayWithDerivedThreshold() throws Exception {
        System.setProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP, "60000");
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 4);
        cache.enqueue("first", ByteBuffer.wrap(new byte[]{1}));
        assertTrue(cache.doWriteCalls.isEmpty());
        cache.enqueue("second", ByteBuffer.wrap(new byte[]{1}));
        await().atMost(5, TimeUnit.SECONDS).until(() -> cache.doWriteCalls.size() == 1);
        assertEquals(List.of(id("first")), cache.doWriteCalls);
    }

    @Test
    public void derivedThresholdRoundsDownForOddCapacity() throws Exception {
        System.setProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP, "60000");
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 5);
        cache.enqueue("first", ByteBuffer.wrap(new byte[]{1}));
        assertTrue(cache.doWriteCalls.isEmpty());
        cache.enqueue("second", ByteBuffer.wrap(new byte[]{1}));
        await().atMost(5, TimeUnit.SECONDS).until(() -> cache.doWriteCalls.size() == 1);
        assertEquals(List.of(id("first")), cache.doWriteCalls);
    }

    @Test
    public void derivedThresholdIsAtLeastOneForZeroDelayCapacityOne() throws Exception {
        System.setProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP, "0");
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 1);
        cache.enqueue("first", ByteBuffer.wrap(new byte[]{1}));
        await().atMost(5, TimeUnit.SECONDS).until(() -> cache.doWriteCalls.size() == 1);
        assertEquals(List.of(id("first")), cache.doWriteCalls);
    }

    @Test
    public void systemPropertyOverridesPreemptiveThreshold() throws Exception {
        System.setProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP, "60000");
        System.setProperty(PersistentDiskCacheV2Async.PREEMPTIVE_FLUSH_SIZE_PROP, "2");
        cache = new ControllablePersistentDiskCacheV2Async(folder.newFolder().toPath(), monitor, 1, 8);
        cache.enqueue("first", ByteBuffer.wrap(new byte[]{1}));
        cache.enqueue("second", ByteBuffer.wrap(new byte[]{1}));
        await().atMost(5, TimeUnit.SECONDS).until(() -> cache.doWriteCalls.size() == 1);
        assertEquals(List.of(id("first")), cache.doWriteCalls);
    }

    @Test
    public void rejectsThresholdAtCapacityWhenDelayEnabled() throws Exception {
        System.setProperty(PersistentDiskCacheV2Async.WRITE_DELAY_MILLIS_PROP, "1000");
        System.setProperty(PersistentDiskCacheV2Async.PREEMPTIVE_FLUSH_SIZE_PROP, "4");
        assertThrows(IllegalArgumentException.class, () -> new PersistentDiskCacheV2Async(
                folder.newFolder().toPath(), 1, monitor, 1, 4));
    }

    private static void restoreProperty(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    private static UUID id(String name) {
        return UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }
}
