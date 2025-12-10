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
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarManager;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class CachingSegmentArchiveReaderTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp() throws Exception {
        temporaryFolder.newFolder();
    }

    @Test
    public void containsSegmentDoesNotUsePersistentDiskCache() throws IOException {
        PersistentCache persistentCache = new MemoryPersistentCache(false);
        // Create a CachingArchiveManager with 2 tar files and a shared persistence disk cache. Each tar file contains a
        // single segment. When looking up for a segment in a particular tar file, we should not find segments that are
        // present in the other tar files. That is, the shared cache should not expose segments from other tar files.
        SegmentTarManager archiveManager = new SegmentTarManager(temporaryFolder.newFolder(),
                new FileStoreMonitorAdapter(), new IOMonitorAdapter(), false, false);
        CachingArchiveManager cachingArchiveManager = new CachingArchiveManager(persistentCache, archiveManager);

        long msb1 = 1L;
        long lsb1 = 1L;
        byte[] data1 = "data1".getBytes(StandardCharsets.UTF_8);
        String archive1Name = "data0000a.tar";

        long msb2 = 2L;
        long lsb2 = 2L;
        byte[] data2 = "data2".getBytes(StandardCharsets.UTF_8);
        String archive2Name = "data0000b.tar";

        SegmentArchiveWriter archive1 = cachingArchiveManager.create(archive1Name);
        archive1.writeSegment(msb1, lsb1, data1, 0, data1.length, 0, 0, true);
        archive1.close();

        SegmentArchiveWriter archive2 = cachingArchiveManager.create(archive2Name);
        archive2.writeSegment(msb2, lsb2, data2, 0, data2.length, 0, 0, true);
        archive2.close();

        // Open the archives and read the segment. This will trigger caching of the segments.
        SegmentArchiveReader tar1 = cachingArchiveManager.open(archive1Name);
        assertEquals(Buffer.wrap(data1), tar1.readSegment(msb1, lsb1));

        SegmentArchiveReader tar2 = cachingArchiveManager.open(archive2Name);
        assertEquals(Buffer.wrap(data2), tar2.readSegment(msb2, lsb2));

        assertTrue(tar1.containsSegment(msb1, lsb1));
        assertFalse(tar1.containsSegment(msb2, lsb2));
        assertFalse(tar2.containsSegment(msb1, lsb1));
        assertTrue(tar2.containsSegment(msb2, lsb2));
    }
}