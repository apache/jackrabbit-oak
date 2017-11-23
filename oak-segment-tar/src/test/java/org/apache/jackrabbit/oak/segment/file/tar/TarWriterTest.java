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

package org.apache.jackrabbit.oak.segment.file.tar;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TarWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Test
    public void createNextGenerationTest() throws IOException {
        int counter = 2222;
        TarWriter t0 = new TarWriter(folder.newFolder(), new FileStoreMonitorAdapter(), counter, new IOMonitorAdapter());

        // not dirty, will not create a new writer
        TarWriter t1 = t0.createNextGeneration();
        assertEquals(t0, t1);
        assertTrue(t1.getFile().getName().contains("" + counter));

        // dirty, will create a new writer
        UUID id = UUID.randomUUID();
        long msb = id.getMostSignificantBits();
        long lsb = id.getLeastSignificantBits() & (-1 >>> 4); // OAK-1672
        byte[] data = "Hello, World!".getBytes(UTF_8);
        t1.writeEntry(msb, lsb, data, 0, data.length, newGCGeneration(0, 0, false));

        TarWriter t2 = t1.createNextGeneration();
        assertNotEquals(t1, t2);
        assertTrue(t1.isClosed());
        assertTrue(t2.getFile().getName().contains("" + (counter + 1)));
    }

    private static class TestFileStoreMonitor extends FileStoreMonitorAdapter {

        long written;

        @Override
        public void written(long bytes) {
            written += bytes;
        }

    }

    @Test
    public void testFileStoreMonitor() throws Exception {
        TestFileStoreMonitor monitor = new TestFileStoreMonitor();
        try (TarWriter writer = new TarWriter(folder.getRoot(), monitor, 0, new IOMonitorAdapter())) {
            long sizeBefore = writer.fileLength();
            long writtenBefore = monitor.written;
            writer.writeEntry(0, 0, new byte[42], 0, 42, newGCGeneration(0, 0, false));
            long sizeAfter = writer.fileLength();
            long writtenAfter = monitor.written;
            assertEquals(sizeAfter - sizeBefore, writtenAfter - writtenBefore);
        }
    }

}
