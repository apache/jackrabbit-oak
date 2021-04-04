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

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.junit.Assert.*;

public abstract class AbstractPersistentCacheTest {

    protected static final int SEGMENTS = 750;
    protected static final int THREADS = 50;
    protected static final int SEGMENTS_PER_THREAD = SEGMENTS / THREADS;
    protected static final int TIMEOUT_COUNT = 50;

    protected static final Executor executor = Executors.newFixedThreadPool(THREADS);

    protected AbstractPersistentCache persistentCache;

    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicInteger done = new AtomicInteger(0);
    int count; // for checking timeouts

    protected static void runConcurrently(BiConsumer<Integer, Integer> threadAndSegmentConsumer) {
        for (int i = 0; i < THREADS; ++i) {
            int threadIdx = i;
            executor.execute(() -> {
                for (int segmentIdx = threadIdx * SEGMENTS_PER_THREAD; segmentIdx < (threadIdx + 1) * SEGMENTS_PER_THREAD; ++segmentIdx) {
                    threadAndSegmentConsumer.accept(threadIdx, segmentIdx);
                }
            });
        }
    }

    protected void waitWhile(Supplier<Boolean> condition) {
        for (count = 0; condition.get() && count < TIMEOUT_COUNT; ++count) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Test
    public void writeAndReadManySegments() {
        final List<TestSegment> testSegments = new ArrayList<>(SEGMENTS);
        final List<Map<String, Buffer>> segmentsRead = new ArrayList<>(THREADS);

        for (int i = 0; i < SEGMENTS; ++i) {
            testSegments.add(TestSegment.createSegment());
        }

        for (int i = 0; i < THREADS; ++i) {
            final Map<String, Buffer> segmentsReadThisThread = new HashMap<>(SEGMENTS_PER_THREAD);
            segmentsRead.add(segmentsReadThisThread);
        }

        runConcurrently((nThread, nSegment) -> {
            TestSegment segment = testSegments.get(nSegment);
            long[] id = segment.getSegmentId();
            try {
                persistentCache.writeSegment(id[0], id[1], segment.getSegmentBuffer());
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);
        waitWhile(() -> persistentCache.getWritesPending() > 0);

        assertEquals("Errors have occurred while writing", 0, errors.get());
        assertNoTimeout();

        done.set(0);
        runConcurrently((nThread, nSegment) -> {
            final Map<String, Buffer> segmentsReadThisThread = segmentsRead.get(nThread);
            final TestSegment segment = testSegments.get(nSegment);
            final long[] id = segment.getSegmentId();
            try {
                final Buffer segmentRead = persistentCache.readSegment(id[0], id[1], () -> null);
                segmentsReadThisThread.put(new UUID(id[0], id[1]).toString(), segmentRead);
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);

        assertNoTimeout();
        assertEquals("Errors have occurred while reading", 0, errors.get());

        for (int i = 0; i < THREADS; ++i) {
            for (int j = i * SEGMENTS_PER_THREAD; j < (i + 1) * SEGMENTS_PER_THREAD; ++j) {
                TestSegment testSegment = testSegments.get(j);
                Map<String, Buffer> segmentsReadThisThread = segmentsRead.get(i);
                long[] segmentReadId = testSegment.getSegmentId();
                Buffer segmentRead = segmentsReadThisThread.get(new UUID(segmentReadId[0], segmentReadId[1]).toString());
                if (segmentRead == null) {
                    errors.incrementAndGet();
                    continue;
                }
                assertSegmentBufferEquals(testSegment.getSegmentBuffer(), segmentRead);
            }
        }
        assertEquals("Segment(s) not found in cache", 0, errors.get());
    }

    @Test
    public void testNonExisting() {
        final Random random = new Random();
        final long[] segmentIds = random.longs(2 * SEGMENTS).toArray();
        final AtomicInteger containsFailures = new AtomicInteger(0);
        final AtomicInteger readFailures = new AtomicInteger(0);

        runConcurrently((nThread, nSegment) -> {
            try {
                long msb = segmentIds[2 * nSegment];
                long lsb = segmentIds[2 * nSegment + 1];
                if (persistentCache.containsSegment(msb, lsb)) {
                    containsFailures.incrementAndGet();
                }
                if (persistentCache.readSegment(msb, lsb, () -> null) != null) {
                    readFailures.incrementAndGet();
                }
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);

        assertEquals("exceptions occurred", 0, errors.get());
        assertNoTimeout();
        assertEquals("containsSegment failed", 0, containsFailures.get());
        assertEquals("readSegment failed", 0, readFailures.get());
    }

    @Test
    public void testExisting() throws Exception {
        final TestSegment testSegment = TestSegment.createSegment();
        final long[] segmentId = testSegment.getSegmentId();
        persistentCache.writeSegment(segmentId[0], segmentId[1], testSegment.getSegmentBuffer());
        final AtomicInteger containsFailures = new AtomicInteger(0);
        final AtomicInteger readFailures = new AtomicInteger(0);

        // We need this to give the cache's write thread pool time to start the thread
        Thread.sleep(1000);

        waitWhile(() -> persistentCache.getWritesPending() > 0);
        assertNoTimeout();
        assertEquals(0, persistentCache.getWritesPending());

        runConcurrently((nThread, nSegment) -> {
            try {
                if (!persistentCache.containsSegment(segmentId[0], segmentId[1])) {
                    containsFailures.incrementAndGet();
                }
                if (persistentCache.readSegment(segmentId[0], segmentId[1], () -> null) == null) {
                    readFailures.incrementAndGet();
                }
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);

        assertEquals("Exceptions occurred", 0, errors.get());
        assertNoTimeout();
        assertEquals("containsSegment failed", 0, containsFailures.get());
        assertEquals("readSegment failed", 0, readFailures.get());
    }

    @Test
    public void testConcurrentWritesSameSegment() {
        final TestSegment testSegment = TestSegment.createSegment();
        long[] segmentId = testSegment.getSegmentId();

        runConcurrently((nThread, nSegment) -> {
            try {
                persistentCache.writeSegment(segmentId[0], segmentId[1], testSegment.getSegmentBuffer());
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);

        Buffer segmentRead = persistentCache.readSegment(segmentId[0], segmentId[1], () -> null);
        assertNotNull("The segment was not found", segmentRead);
        assertSegmentBufferEquals(testSegment.getSegmentBuffer(), segmentRead);
    }

    protected static class TestSegment {
        public static int UUID_LEN = 2 * Long.SIZE;
        public static int SEGMENT_LEN = 256 * 1024;

        private static final Random random = new Random();

        private final byte[] segmentId;
        private final byte[] segmentBytes;

        protected static TestSegment createSegment() {
            return new TestSegment(createSegmentIdBytes(), createSegmentBytes());
        }

        private static byte[] createSegmentBytes() {
            byte[] ret = new byte[SEGMENT_LEN];
            random.nextBytes(ret);
            return ret;
        }

        private static byte[] createSegmentIdBytes() {
            byte[] ret = new byte[UUID_LEN];
            random.nextBytes(ret);
            return ret;
        }

        protected long[] getSegmentId() {
            final Buffer buffer = Buffer.allocate(segmentId.length);
            buffer.put(segmentId);
            long[] ret = new long[2];
            ret[0] = buffer.getLong(0);
            ret[1] = buffer.getLong(8);
            return ret;
        }

        protected Buffer getSegmentBuffer() {
            return Buffer.wrap(segmentBytes);
        }

        private TestSegment(byte[] segmentId, byte[] segmentBytes) {
            this.segmentId = segmentId;
            this.segmentBytes = segmentBytes;
        }

        protected byte[] getSegmentBytes() {
            return segmentBytes;
        }
    }

    protected static void assertSegmentBufferEquals(Buffer expected, Buffer actual) {
        expected.rewind();
        actual.rewind();
        assertEquals("Segment size is different", TestSegment.SEGMENT_LEN, actual.remaining());
        for (int i = 0; i < TestSegment.SEGMENT_LEN; ++i) {
            assertEquals("Difference in byte buffer", expected.get(i), actual.get(i));
        }
    }

    protected void assertNoTimeout() {
        assertTrue("Wait timeout reached", count < TIMEOUT_COUNT);
    }
}
