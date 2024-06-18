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
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PersistentDiskCacheTest extends AbstractPersistentCacheTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws Exception {
        persistentCache = new PersistentDiskCache(temporaryFolder.newFolder(), 10 * 1024, new IOMonitorAdapter());
    }

    @Test
    public void cleanupTest() throws Exception {
        persistentCache.close();
        persistentCache = new PersistentDiskCache(temporaryFolder.newFolder(), 0, new IOMonitorAdapter(), 500);
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
        waitWhile(() -> ((PersistentDiskCache) persistentCache).cleanupInProgress.get());

        persistentCache.cleanUp();

        runConcurrently((nThread, nSegment) -> {
            final TestSegment segment = testSegments.get(nSegment);
            final long[] id = segment.getSegmentId();
            try {
                final Map<String, Buffer> segmentsReadThisThread = segmentsRead.get(nThread);
                final Buffer segmentRead = persistentCache.readSegment(id[0], id[1], () -> null);
                segmentsReadThisThread.put(new UUID(id[0], id[1]).toString(), segmentRead);
            } catch (Throwable t) {
                errors.incrementAndGet();
            } finally {
                done.incrementAndGet();
            }
        });

        waitWhile(() -> done.get() < SEGMENTS);

        assertNoTimeout();
        assertEquals("Errors have occurred while reading", 0, errors.get());
        errors.set(0);

        for (int i = 0; i < THREADS; ++i) {
            for (int j = i * SEGMENTS_PER_THREAD; j < (i + 1) * SEGMENTS_PER_THREAD; ++j) {
                TestSegment testSegment = testSegments.get(j);
                Map<String, Buffer> segmentsReadThisThread = segmentsRead.get(i);
                long[] segmentReadId = testSegment.getSegmentId();
                Buffer segmentRead = segmentsReadThisThread.get(new UUID(segmentReadId[0], segmentReadId[1]).toString());
                if (segmentRead == null) {
                    errors.incrementAndGet();
                }
            }
        }
        assertEquals("Segment(s) not cleaned up in cache", 0, SEGMENTS - errors.get());
    }

    @Test
    public void testIOMonitor() throws IOException {
        IOMonitorAdapter ioMonitorAdapter = Mockito.mock(IOMonitorAdapter.class);

        persistentCache.close();
        File cacheFolder = temporaryFolder.newFolder();
        persistentCache = new PersistentDiskCache(cacheFolder, 0, ioMonitorAdapter);

        UUID segmentUUID = UUID.randomUUID();

        persistentCache.readSegment(segmentUUID.getMostSignificantBits(), segmentUUID.getLeastSignificantBits(), () -> null);

        //Segment not in cache, monitor methods not invoked
        verify(ioMonitorAdapter, never()).beforeSegmentRead(any(), anyLong(), anyLong(), anyInt());
        verify(ioMonitorAdapter, never()).afterSegmentRead(any(), anyLong(), anyLong(), anyInt(), anyLong());

        //place segment in disk cache
        File segmentFile = new File(cacheFolder, segmentUUID.toString());
        segmentFile.createNewFile();

        persistentCache.readSegment(segmentUUID.getMostSignificantBits(), segmentUUID.getLeastSignificantBits(), () -> null);

        verify(ioMonitorAdapter, times(1)).beforeSegmentRead(eq(segmentFile), eq(segmentUUID.getMostSignificantBits()), eq(segmentUUID.getLeastSignificantBits()), anyInt());
        verify(ioMonitorAdapter, times(1)).afterSegmentRead(eq(segmentFile), eq(segmentUUID.getMostSignificantBits()), eq(segmentUUID.getLeastSignificantBits()), anyInt(), anyLong());
    }
}