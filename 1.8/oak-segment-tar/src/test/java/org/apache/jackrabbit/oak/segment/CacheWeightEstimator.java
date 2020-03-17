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

import static org.apache.jackrabbit.oak.segment.Segment.GC_FULL_GENERATION_OFFSET;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.function.Supplier;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.segment.CacheWeights.StringCacheWeigher;
import org.apache.jackrabbit.oak.segment.file.PriorityCache;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;

/**
 * Test/Utility class to measure size in memory for common segment-tar objects.
 * <p>
 * The test is <b>disabled</b> by default, to run it you need to set the
 * {@code CacheWeightsTest} system property:<br>
 * {@code mv clean test -Dtest=CacheWeightsTest -DCacheWeightsTest=true -Dtest.opts.memory=-Xmx2G}
 * </p>
 * <p>
 * To collect the results check the
 * {@code org.apache.jackrabbit.oak.segment.CacheWeightsTest-output.txt} file:<br>
 * {@code cat target/surefire-reports/org.apache.jackrabbit.oak.segment.CacheWeightsTest-output.txt}
 * </p>
 */
public class CacheWeightEstimator {

    // http://www.javaworld.com/article/2077496/testing-debugging/java-tip-130--do-you-know-your-data-size-.html
    // http://www.javaspecialists.eu/archive/Issue029.html
    // http://www.slideshare.net/cnbailey/memory-efficient-java

    /*-
     * Open JDK's JOL report on various segment classes:

    org.apache.jackrabbit.oak.segment.RecordId object internals:
    OFFSET  SIZE      TYPE DESCRIPTION                    VALUE
      0    12           (object header)                N/A
     12     4       int RecordId.offset                N/A
     16     4 SegmentId RecordId.segmentId             N/A
     20     4           (loss due to the next object alignment)
    Instance size: 24 bytes
    Space losses: 0 bytes internal + 4 bytes external = 4 bytes total

    org.apache.jackrabbit.oak.segment.SegmentId object internals:
    OFFSET  SIZE         TYPE DESCRIPTION                    VALUE
      0    12              (object header)                N/A
     12     4          int SegmentId.gcGeneration         N/A
     16     8         long SegmentId.msb                  N/A
     24     8         long SegmentId.lsb                  N/A
     32     8         long SegmentId.creationTime         N/A
     40     4 SegmentStore SegmentId.store                N/A
     44     4       String SegmentId.gcInfo               N/A
     48     4      Segment SegmentId.segment              N/A
     52     4              (loss due to the next object alignment)
    Instance size: 56 bytes
    Space losses: 0 bytes internal + 4 bytes external = 4 bytes total

    org.apache.jackrabbit.oak.segment.Segment object internals:
    OFFSET  SIZE              TYPE DESCRIPTION                    VALUE
      0    12                   (object header)                N/A
     12     4      SegmentStore Segment.store                  N/A
     16     4     SegmentReader Segment.reader                 N/A
     20     4         SegmentId Segment.id                     N/A
     24     4        ByteBuffer Segment.data                   N/A
     28     4    SegmentVersion Segment.version                N/A
     32     4     RecordNumbers Segment.recordNumbers          N/A
     36     4 SegmentReferences Segment.segmentReferences      N/A
     40     4            String Segment.info                   N/A
     44     4                   (loss due to the next object alignment)
    Instance size: 48 bytes
    Space losses: 0 bytes internal + 4 bytes external = 4 bytes total

    org.apache.jackrabbit.oak.segment.Template object internals:
    OFFSET  SIZE               TYPE DESCRIPTION                    VALUE
      0    12                    (object header)                N/A
     12     4      SegmentReader Template.reader                N/A
     16     4      PropertyState Template.primaryType           N/A
     20     4      PropertyState Template.mixinTypes            N/A
     24     4 PropertyTemplate[] Template.properties            N/A
     28     4             String Template.childName             N/A
    Instance size: 32 bytes
    Space losses: 0 bytes internal + 0 bytes external = 0 bytes total

     */

    private static MemoryStore store;

    public static void main(String... args) throws Exception {
        run(CacheWeightEstimator::testObjects);
        run(CacheWeightEstimator::testSegmentIds);
        run(CacheWeightEstimator::testSegmentIdsWGc);
        run(CacheWeightEstimator::testRecordIds);
        run(CacheWeightEstimator::testRecordIdsWGc);
        run(CacheWeightEstimator::testStringCache);
        run(CacheWeightEstimator::testNodeCache);
        run(CacheWeightEstimator::testSegments);
        run(CacheWeightEstimator::testSegmentCache);
        run(CacheWeightEstimator::testStrings);
    }

    private static void run(Runnable runnable) throws Exception {
        store = new MemoryStore();
        try {
            runnable.run();
        } finally {
            store = null;
        }
    }

    private static void testObjects() {
        final int count = 1000000;
        Supplier<Entry<Object, Long[]>> factory = () -> {
            Object[] objects = new Object[count];
            for (int i = 0; i < count; ++i) {
                Object o = new Object();
                objects[i] = o;
            }
            long weight = CacheWeights.OBJECT_HEADER_SIZE * count;
            return new SimpleImmutableEntry<>(objects, new Long[] {(long) count, weight});
        };
        runTest(factory, "Object[x" + count + "]");
    }

    private static void testSegmentIds() {
        runSegmentIds(1000000, false);
    }

    private static void testSegmentIdsWGc() {
        runSegmentIds(1000000, true);
    }

    private static void runSegmentIds(final int count, final boolean gcInfo) {
        Supplier<Entry<Object, Long[]>> factory = () -> {
            long weight = 0;
            Object[] objects = new Object[count];
            for (int i = 0; i < count; ++i) {
                SegmentId o = randomSegmentId(gcInfo);
                weight += o.estimateMemoryUsage();
                objects[i] = o;
            }
            return new SimpleImmutableEntry<>(objects, new Long[] {(long) count, weight});
        };
        String name = "SegmentId";
        if (gcInfo) {
            name += "[x" + count + "|GCInfo]";
        } else {
            name += "[x" + count + "]";
        }
        runTest(factory, name);
    }

    private static void testRecordIds() {
        runRecordIds(1000000, false);
    }

    private static void testRecordIdsWGc() {
        runRecordIds(1000000, true);
    }

    private static void runRecordIds(final int count, final boolean gcInfo) {
        Supplier<Entry<Object, Long[]>> factory = () -> {
            long weight = 0;
            Object[] objects = new Object[count];
            for (int i = 0; i < count; ++i) {
                RecordId o = randomRecordId(gcInfo);
                weight += o.estimateMemoryUsage();
                objects[i] = o;
            }
            return new SimpleImmutableEntry<>(objects, new Long[] {(long) count, weight});
        };
        String name = "RecordId";
        if (gcInfo) {
            name += "[x" + count + "|GCInfo]";
        } else {
            name += "[x" + count + "]";
        }
        runTest(factory, name);
    }

    private static void testStringCache() {
        final int count = 1000000;
        final int keySize = 96;
        final boolean gcInfo = true;
        Supplier<Entry<Object, Long[]>> factory = () -> {
            RecordCache<String> cache = RecordCache.factory(count, new StringCacheWeigher()).get();
            for (int i = 0; i < count; ++i) {
                String k = randomString(keySize);
                RecordId v = randomRecordId(gcInfo);
                cache.put(k, v);
            }
            long weight = cache.estimateCurrentWeight();
            return new SimpleImmutableEntry<>(cache, new Long[] {(long) count, weight});
        };
        runTest(factory, "StringCache[x" + count
                + "|RecordCache<String, RecordId>]");
    }

    private static void testNodeCache() {
        final int count = 1000000;
        // key usually is a stableid, see SegmentNodeState#getStableId
        // 2fdd370e-423c-43d6-aad7-6e336c551a38:xxxxxx
        final int keySize = 43;
        final boolean gcInfo = true;
        Supplier<Entry<Object, Long[]>> factory = () -> {
            int size = (int) PriorityCache.nextPowerOfTwo(count);
            PriorityCache<String, RecordId> cache = PriorityCache.factory(size, new CacheWeights.NodeCacheWeigher()).get();
            for (int i = 0; i < count; ++i) {
                String k = randomString(keySize);
                RecordId v = randomRecordId(gcInfo);
                cache.put(k, v, 0, (byte) 0);
            }
            long weight = cache.estimateCurrentWeight();
            return new SimpleImmutableEntry<>(cache, new Long[] {(long) count, weight});
        };
        runTest(factory, "NodeCache[x" + count
                + "|PriorityCache<String, RecordId>]");
    }

    private static void testSegments() {
        final int count = 10000;
        final int bufferSize = 5 * 1024;
        Supplier<Entry<Object, Long[]>> factory = () -> {
            long weight = 0;
            Object[] objects = new Object[count];
            for (int i = 0; i < count; ++i) {
                Segment o = randomSegment(bufferSize);
                weight += o.estimateMemoryUsage();
                objects[i] = o;
            }
            return new SimpleImmutableEntry<>(objects, new Long[] {(long) count, weight});
        };
        runTest(factory, "Segment[x" + count + "|" + bufferSize + "]");
    }

    private static void testSegmentCache() {
        final int count = 10000;
        final int cacheSizeMB = 100;
        final int bufferSize = 5 * 1024;
        Supplier<Entry<Object, Long[]>> factory = () -> {
            SegmentCache cache = new SegmentCache(cacheSizeMB);
            for (int i = 0; i < count; ++i) {
                Segment segment = randomSegment(bufferSize);
                cache.putSegment(segment);
            }
            AbstractCacheStats stats = cache.getCacheStats();
            long elements = stats.getElementCount();
            long weight = stats.estimateCurrentWeight();
            return new SimpleImmutableEntry<>(cache, new Long[] {elements, weight});
        };
        runTest(factory, "SegmentCache[x" + cacheSizeMB + "MB|" + bufferSize + "|Cache<SegmentId, Segment>]");
    }

    private static void testStrings() {
        final int count = 10000;
        final int length = 256;
        Supplier<Entry<Object, Long[]>> factory = () -> {
            long weight = 0;
            Object[] objects = new Object[count];
            for (int i = 0; i < count; ++i) {
                String s = randomString(length);
                weight += StringUtils.estimateMemoryUsage(s);
                objects[i] = s;
            }
            return new SimpleImmutableEntry<>(objects, new Long[] {(long) count, weight});
        };
        runTest(factory, "String[x" + count + "|" + length + "]");
    }

    private static SegmentId randomSegmentId(boolean withGc) {
        UUID u = UUID.randomUUID();
        SegmentId id = new SegmentId(store, u.getMostSignificantBits(), u.getLeastSignificantBits());
        if (withGc) {
            id.reclaimed(randomString(80));
        }
        return id;
    }

    private static RecordId randomRecordId(boolean withGc) {
        return new RecordId(randomSegmentId(withGc), 128);
    }

    private static Segment randomSegment(int bufferSize) {
        byte[] buffer = new byte[bufferSize];
        buffer[0] = '0';
        buffer[1] = 'a';
        buffer[2] = 'K';
        buffer[3] = SegmentVersion.asByte(LATEST_VERSION);
        buffer[4] = 0; // reserved
        buffer[5] = 0; // refcount

        int generation = 0;
        buffer[GC_FULL_GENERATION_OFFSET] = (byte) (generation >> 24);
        buffer[GC_FULL_GENERATION_OFFSET + 1] = (byte) (generation >> 16);
        buffer[GC_FULL_GENERATION_OFFSET + 2] = (byte) (generation >> 8);
        buffer[GC_FULL_GENERATION_OFFSET + 3] = (byte) generation;

        ByteBuffer data = ByteBuffer.wrap(buffer);
        SegmentId id = randomSegmentId(false);
        Segment segment = new Segment(store.getSegmentIdProvider(), store.getReader(), id, data);

        //
        // TODO check impact of MutableRecordNumbers overhead of 65k bytes
        //
        // MutableRecordNumbers recordNumbers = new MutableRecordNumbers();
        // MutableSegmentReferences segmentReferences = new
        // MutableSegmentReferences();
        // String metaInfo = "{\"wid\":\"" + wid + '"' + ",\"sno\":" + 0
        // + ",\"t\":" + currentTimeMillis() + "}";
        // segment = new Segment(store, store.getReader(), buffer,
        // recordNumbers, segmentReferences, metaInfo);

        return segment;
    }

    private static String randomString(int lenght) {
        return RandomStringUtils.randomAlphanumeric(lenght);
    }

    @SuppressWarnings("unused")
    private static void runTest(Supplier<Entry<Object, Long[]>> factory, String name) {
        long start = memory();
        Entry<Object, Long[]> e = factory.get();
        Object object = e.getKey(); // prevent gc
        long count = e.getValue()[0];
        long weight = e.getValue()[1];
        long end = memory();

        long delta = end - start;
        long itemH = delta / count;
        long itemW = weight / count;

        System.out.printf(":: %s Test\n", name);
        System.out.printf("heap delta is       %d, %d bytes per item (%d -> %d)\n", delta, itemH, start, end);
        System.out.printf("estimated weight is %d, %d bytes per item\n", weight, itemW);
        if (itemW > itemH * 1.1) {
            System.out.printf("*warn* estimated weight is over 10%% bigger than heap based weight\n");
        }
        if (itemW * 1.1 < itemH) {
            System.out.printf("*warn* estimated weight is over 10%% smaller than heap based weight\n");
        }
    }

    private static long memory() {
        gc();
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    private static void gc() {
        for (int i = 0; i < 10; i++) {
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
