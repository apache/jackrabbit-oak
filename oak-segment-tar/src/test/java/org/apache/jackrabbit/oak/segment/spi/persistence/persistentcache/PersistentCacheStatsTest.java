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
package org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentCacheStatsTest {

    @Test
    public void testCacheStats() {
        AbstractPersistentCache cache = new PersistentCacheImpl();

        cache.writeSegment(1, 1, Buffer.wrap(new byte[]{1}));

        //segment in cache
        Buffer segment = cache.readSegment(1, 1, () -> null);
        assertNotNull(segment);

        assertEquals(1, cache.getCacheStats().getHitCount());
        long loadTime = cache.getCacheStats().getTotalLoadTime();
        assertEquals(0, loadTime);


        //segment not in cache but loaded from remote store
        segment = cache.readSegment(0, 0, () -> Buffer.wrap(new byte[]{0}));
        assertNotNull(segment);
        assertEquals(1, cache.getCacheStats().getMissCount());
        loadTime = cache.getCacheStats().getTotalLoadTime();
        assertTrue(loadTime > 0);

        //segment not in cache and exception while loading from remote store
        segment = cache.readSegment(2, 2, () -> {
            throw new Exception();
        });
        assertNull(segment);
        long loadTime2 = cache.getCacheStats().getTotalLoadTime();
        assertTrue(loadTime2 > loadTime);
        assertEquals(2, cache.getCacheStats().getMissCount());
        assertEquals(1, cache.getCacheStats().getLoadExceptionCount());

        cache.close();
    }

    @Test
    public void testCacheStatsForLinkedCaches() {
        AbstractPersistentCache cache1 = new PersistentCacheImpl();
        AbstractPersistentCache cache2 = new PersistentCacheImpl();

        cache1.linkWith(cache2);

        //segment not in either cache
        Buffer segment = cache1.readSegment(0 ,0, () -> null);

        assertNull(segment);
        assertEquals(1, cache1.getCacheStats().getMissCount());
        assertEquals(1, cache2.getCacheStats().getMissCount());
        assertEquals(0, cache1.getCacheStats().getHitCount());
        assertEquals(0, cache2.getCacheStats().getHitCount());
        assertEquals(0, cache1.segmentCacheStats.getLoadCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadCount());
        assertEquals(0, cache1.segmentCacheStats.getLoadExceptionCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadExceptionCount());

        //segment in first cache
        cache1.writeSegment(1, 1, Buffer.wrap(new byte[]{1}));
        segment = cache1.readSegment(1 ,1, () -> null);

        assertNotNull(segment);
        assertEquals(1, cache1.getCacheStats().getMissCount());
        assertEquals(1, cache2.getCacheStats().getMissCount());
        assertEquals(1, cache1.getCacheStats().getHitCount());
        assertEquals(0, cache2.getCacheStats().getHitCount());
        assertEquals(0, cache1.segmentCacheStats.getLoadCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadCount());
        assertEquals(0, cache1.segmentCacheStats.getLoadExceptionCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadExceptionCount());

        //segment in second cache
        cache2.writeSegment(2, 2, Buffer.wrap(new byte[]{2}));
        segment = cache1.readSegment(2 ,2, () -> null);

        assertNotNull(segment);
        assertEquals(2, cache1.getCacheStats().getMissCount());
        assertEquals(1, cache2.getCacheStats().getMissCount());
        assertEquals(1, cache1.getCacheStats().getHitCount());
        assertEquals(1, cache2.getCacheStats().getHitCount());
        assertEquals(1, cache1.segmentCacheStats.getLoadCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadCount());
        assertEquals(0, cache1.segmentCacheStats.getLoadExceptionCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadExceptionCount());

        //segment loaded from the remote storage
        segment = cache1.readSegment(3 ,3, () -> Buffer.wrap(new byte[]{3}));

        assertNotNull(segment);
        assertEquals(3, cache1.getCacheStats().getMissCount());
        assertEquals(2, cache2.getCacheStats().getMissCount());
        assertEquals(1, cache1.getCacheStats().getHitCount());
        assertEquals(1, cache2.getCacheStats().getHitCount());
        assertEquals(2, cache1.segmentCacheStats.getLoadCount());
        assertEquals(1, cache2.segmentCacheStats.getLoadCount());
        assertEquals(0, cache1.segmentCacheStats.getLoadExceptionCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadExceptionCount());

        //exception while loading segment from the remote storage
        segment = cache1.readSegment(4 ,4, () -> {
            throw new Exception();
        });

        assertNull(segment);
        assertEquals(4, cache1.getCacheStats().getMissCount());
        assertEquals(3, cache2.getCacheStats().getMissCount());
        assertEquals(1, cache1.getCacheStats().getHitCount());
        assertEquals(1, cache2.getCacheStats().getHitCount());
        assertEquals(2, cache1.segmentCacheStats.getLoadCount());
        assertEquals(2, cache2.segmentCacheStats.getLoadCount());
        assertEquals(0, cache1.segmentCacheStats.getLoadExceptionCount());
        assertEquals(1, cache2.segmentCacheStats.getLoadExceptionCount());

        //linked cache throws exception
        cache2 = new PersistentCacheImpl(){
            @Override
            protected Buffer readSegmentInternal(long msb, long lsb) {
                throw new RuntimeException();
            }
        };
        cache1.linkWith(cache2);
        segment = cache1.readSegment(5 ,5, () -> Buffer.wrap(new byte[]{5}));

        assertNull(segment);
        assertEquals(5, cache1.getCacheStats().getMissCount());
        assertEquals(0, cache2.getCacheStats().getMissCount());
        assertEquals(1, cache1.getCacheStats().getHitCount());
        assertEquals(0, cache2.getCacheStats().getHitCount());
        assertEquals(3, cache1.segmentCacheStats.getLoadCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadCount());
        assertEquals(1, cache1.segmentCacheStats.getLoadExceptionCount());
        assertEquals(0, cache2.segmentCacheStats.getLoadExceptionCount());

        cache2.close();
        cache1.close();
    }

    class PersistentCacheImpl extends AbstractPersistentCache {
        HashMap<UUID, Buffer> segments = new HashMap<>();

        public PersistentCacheImpl() {
            segmentCacheStats = new SegmentCacheStats("stats", () -> maximumWeight, () -> elementCount.get(), () -> currentWeight.get(), () -> evictionCount.get());
        }

        long maximumWeight = Long.MAX_VALUE;
        AtomicLong elementCount = new AtomicLong();
        AtomicLong currentWeight = new AtomicLong();
        AtomicLong evictionCount = new AtomicLong();

        void AbstractPersistentCache() {
            segmentCacheStats = new SegmentCacheStats("stats", () -> maximumWeight, () -> elementCount.get(), () -> currentWeight.get(), () -> evictionCount.get());
        }

        @Override
        public boolean containsSegment(long msb, long lsb) {
            return segments.containsKey(new UUID(msb, lsb));
        }

        @Override
        public void writeSegment(long msb, long lsb, Buffer buffer) {
            segments.put(new UUID(msb, lsb), buffer);
        }

        @Override
        public void cleanUp() {

        }

        @Override
        protected Buffer readSegmentInternal(long msb, long lsb) {
            return segments.get(new UUID(msb, lsb));
        }
    }
}
