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
package org.apache.jackrabbit.oak.segment.remote;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.junit.Test;

public class AbstractRemoteSegmentArchiveWriterTest {

    private static final byte[] DATA = {1, 2, 3};

    @Test
    public void recoverSegmentReusesExistingEntry() throws Exception {
        TestWriter writer = new TestWriter(true);

        writer.recoverSegment(1, 2, DATA, 0, DATA.length, 3, 4, true);
        writer.close();

        assertEquals(1, writer.recoveredEntries.size());
        assertEquals(0, writer.writtenEntries.size());
        assertEquals(0, writer.recoveredEntries.get(0).getPosition());
        assertEquals(1, writer.getEntryCount());
        assertEquals(DATA.length, writer.getLength());
        assertTrue(writer.containsSegment(1, 2));
    }

    @Test
    public void recoverSegmentWritesMissingEntry() throws Exception {
        TestWriter writer = new TestWriter(false);

        writer.recoverSegment(1, 2, DATA, 0, DATA.length, 3, 4, true);
        writer.close();

        assertEquals(1, writer.recoveredEntries.size());
        assertEquals(1, writer.writtenEntries.size());
        assertEquals(0, writer.writtenEntries.get(0).getPosition());
    }

    @Test
    public void recoveredEntriesKeepArchiveOrder() throws Exception {
        TestWriter writer = new TestWriter(true);

        writer.recoverSegment(1, 2, DATA, 0, DATA.length, 0, 0, false);
        writer.recoverSegment(3, 4, DATA, 0, DATA.length, 0, 0, false);
        writer.close();

        assertEquals(0, writer.recoveredEntries.get(0).getPosition());
        assertEquals(1, writer.recoveredEntries.get(1).getPosition());
    }

    private static final class TestWriter extends AbstractRemoteSegmentArchiveWriter {

        private final boolean reuse;
        private final List<RemoteSegmentArchiveEntry> recoveredEntries = new ArrayList<>();
        private final List<RemoteSegmentArchiveEntry> writtenEntries = new ArrayList<>();

        private TestWriter(boolean reuse) {
            super(new IOMonitorAdapter(), new FileStoreMonitorAdapter());
            this.reuse = reuse;
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        protected boolean doTryReuseArchiveEntry(RemoteSegmentArchiveEntry indexEntry) {
            recoveredEntries.add(indexEntry);
            return reuse;
        }

        @Override
        protected void doWriteArchiveEntry(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) {
            writtenEntries.add(indexEntry);
        }

        @Override
        protected Buffer doReadArchiveEntry(RemoteSegmentArchiveEntry indexEntry) {
            return Buffer.wrap(DATA);
        }

        @Override
        protected void doWriteDataFile(byte[] data, String extension) {
        }

        @Override
        protected void afterQueueClosed() throws IOException {
        }

        @Override
        protected void afterQueueFlushed() throws IOException {
        }
    }
}
