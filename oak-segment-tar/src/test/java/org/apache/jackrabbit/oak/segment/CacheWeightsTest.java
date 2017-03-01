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

import static org.apache.jackrabbit.oak.segment.Segment.GC_GENERATION_OFFSET;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.junit.Assume.assumeTrue;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.segment.CacheWeights.StringCacheWeigher;
import org.apache.jackrabbit.oak.segment.file.PriorityCache;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

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
public class CacheWeightsTest {

    private static final Logger LOG = LoggerFactory
            .getLogger(CacheWeightsTest.class);

    private static final boolean ENABLED = Boolean
            .getBoolean(CacheWeightsTest.class.getSimpleName());

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

    private MemoryStore store;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(ENABLED);
    }

    @Before
    public void setup() throws Exception {
        store = new MemoryStore();
    }

    @Test
    public void testObjects() {
        final int count = 1000000;

        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {

                Object[] objects = new Object[count];
                for (int i = 0; i < count; ++i) {
                    Object o = new Object();
                    objects[i] = o;
                }
                long weight = CacheWeights.OBJECT_HEADER_SIZE * count;
                return new SimpleImmutableEntry<Object, Long[]>(objects,
                        new Long[] { (long) count, weight });
            }
        };
        runTest(factory, "Object[x" + count + "]");
    }

    @Test
    public void testSegmentIds() {
        runSegmentIds(1000000, false);
    }

    @Test
    public void testSegmentIdsWGc() {
        runSegmentIds(1000000, true);
    }

    private void runSegmentIds(final int count, final boolean gcInfo) {
        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {
                long weight = 0;
                Object[] objects = new Object[count];
                for (int i = 0; i < count; ++i) {
                    SegmentId o = randomSegmentId(gcInfo);
                    weight += o.estimateMemoryUsage();
                    objects[i] = o;
                }
                return new SimpleImmutableEntry<Object, Long[]>(objects,
                        new Long[] { (long) count, weight });
            }
        };
        String name = "SegmentId";
        if (gcInfo) {
            name += "[x" + count + "|GCInfo]";
        } else {
            name += "[x" + count + "]";
        }
        runTest(factory, name);
    }

    @Test
    public void testRecordIds() {
        runRecordIds(1000000, false);
    }

    @Test
    public void testRecordIdsWGc() {
        runRecordIds(1000000, true);
    }

    private void runRecordIds(final int count, final boolean gcInfo) {
        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {
                long weight = 0;
                Object[] objects = new Object[count];
                for (int i = 0; i < count; ++i) {
                    RecordId o = randomRecordId(gcInfo);
                    weight += o.estimateMemoryUsage();
                    objects[i] = o;
                }
                return new SimpleImmutableEntry<Object, Long[]>(objects,
                        new Long[] { (long) count, weight });
            }
        };
        String name = "RecordId";
        if (gcInfo) {
            name += "[x" + count + "|GCInfo]";
        } else {
            name += "[x" + count + "]";
        }
        runTest(factory, name);
    }

    @Test
    public void testStringCache() {
        final int count = 1000000;
        final int keySize = 96;
        final boolean gcInfo = true;

        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {
                RecordCache<String> cache = RecordCache.factory(count,
                        new StringCacheWeigher()).get();
                for (int i = 0; i < count; ++i) {
                    String k = randomString(keySize);
                    RecordId v = randomRecordId(gcInfo);
                    cache.put(k, v);
                }
                long weight = cache.estimateCurrentWeight();
                return new SimpleImmutableEntry<Object, Long[]>(cache,
                        new Long[] { (long) count, weight });
            }
        };
        runTest(factory, "StringCache[x" + count
                + "|RecordCache<String, RecordId>]");
    }

    @Test
    public void testNodeCache() {
        final int count = 1000000;
        // key usually is a stableid, see SegmentNodeState#getStableId
        // 2fdd370e-423c-43d6-aad7-6e336c551a38:xxxxxx
        final int keySize = 43;
        final boolean gcInfo = true;

        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {
                int size = (int) PriorityCache.nextPowerOfTwo(count);
                PriorityCache<String, RecordId> cache = PriorityCache
                        .<String, RecordId> factory(size,
                                new CacheWeights.NodeCacheWeigher()).get();

                for (int i = 0; i < count; ++i) {
                    String k = randomString(keySize);
                    RecordId v = randomRecordId(gcInfo);
                    cache.put(k, v, 0, (byte) 0);
                }
                long weight = cache.estimateCurrentWeight();
                return new SimpleImmutableEntry<Object, Long[]>(cache,
                        new Long[] { (long) count, weight });
            }
        };
        runTest(factory, "NodeCache[x" + count
                + "|PriorityCache<String, RecordId>]");
    }

    @Test
    public void testSegments() {
        final int count = 10000;
        final int bufferSize = 5 * 1024;

        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {

                long weight = 0;
                Object[] objects = new Object[count];
                for (int i = 0; i < count; ++i) {
                    Segment o = randomSegment(bufferSize);
                    weight += o.estimateMemoryUsage();
                    objects[i] = o;
                }
                return new SimpleImmutableEntry<Object, Long[]>(objects,
                        new Long[] { (long) count, weight });
            }
        };
        runTest(factory, "Segment[x" + count + "|" + bufferSize + "]");
    }

    @Test
    public void testSegmentCache() {
        final int count = 10000;
        final int cacheSizeMB = 100;
        final int bufferSize = 5 * 1024;

        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {
                SegmentCache cache = new SegmentCache(cacheSizeMB);
                for (int i = 0; i < count; ++i) {
                    Segment segment = randomSegment(bufferSize);
                    cache.putSegment(segment);
                }
                CacheStats stats = cache.getCacheStats();
                long elements = stats.getElementCount();
                long weight = stats.estimateCurrentWeight();
                return new SimpleImmutableEntry<Object, Long[]>(cache,
                        new Long[] { elements, weight });
            }
        };
        runTest(factory, "SegmentCache[x" + cacheSizeMB + "MB|" + bufferSize
                + "|Cache<SegmentId, Segment>]");
    }

    @Test
    public void testStrings() {
        final int count = 10000;
        final int length = 256;

        Supplier<Entry<Object, Long[]>> factory = new Supplier<Entry<Object, Long[]>>() {

            @Override
            public Entry<Object, Long[]> get() {

                long weight = 0;
                Object[] objects = new Object[count];
                for (int i = 0; i < count; ++i) {
                    String s = randomString(length);
                    weight += StringUtils.estimateMemoryUsage(s);
                    objects[i] = s;
                }
                return new SimpleImmutableEntry<Object, Long[]>(objects,
                        new Long[] { (long) count, weight });
            }
        };
        runTest(factory, "String[x" + count + "|" + length + "]");
    }

    private SegmentId randomSegmentId(boolean withGc) {
        UUID u = UUID.randomUUID();
        SegmentId id = new SegmentId(store, u.getMostSignificantBits(),
                u.getLeastSignificantBits());
        if (withGc) {
            id.reclaimed(randomString(80));
        }
        return id;
    }

    private RecordId randomRecordId(boolean withGc) {
        return new RecordId(randomSegmentId(withGc), 128);
    }

    private Segment randomSegment(int bufferSize) {
        byte[] buffer = new byte[bufferSize];
        buffer[0] = '0';
        buffer[1] = 'a';
        buffer[2] = 'K';
        buffer[3] = SegmentVersion.asByte(LATEST_VERSION);
        buffer[4] = 0; // reserved
        buffer[5] = 0; // refcount

        int generation = 0;
        buffer[GC_GENERATION_OFFSET] = (byte) (generation >> 24);
        buffer[GC_GENERATION_OFFSET + 1] = (byte) (generation >> 16);
        buffer[GC_GENERATION_OFFSET + 2] = (byte) (generation >> 8);
        buffer[GC_GENERATION_OFFSET + 3] = (byte) generation;

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
    private static void runTest(Supplier<Entry<Object, Long[]>> factory,
            String name) {

        long start = memory();
        Entry<Object, Long[]> e = factory.get();
        Object object = e.getKey(); // prevent gc
        long count = e.getValue()[0];
        long weight = e.getValue()[1];
        long end = memory();

        long delta = end - start;
        long itemH = delta / count;
        long itemW = weight / count;

        LOG.info(":: {} Test", name);
        LOG.info("heap delta is       {}, {} bytes per item ({} -> {})", delta,
                itemH, start, end);
        LOG.info("estimated weight is {}, {} bytes per item", weight, itemW);
        if (itemW > itemH * 1.1) {
            LOG.info("*warn* estimated weight is over 10% bigger than heap based weight");
        }
        if (itemW * 1.1 < itemH) {
            LOG.info("*warn* estimated weight is over 10% smaller than heap based weight");
        }
    }

    private static long memory() {
        gc();
        return Runtime.getRuntime().totalMemory()
                - Runtime.getRuntime().freeMemory();
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
