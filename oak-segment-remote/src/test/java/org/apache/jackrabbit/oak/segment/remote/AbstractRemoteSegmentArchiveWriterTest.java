/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.jackrabbit.oak.segment.remote.queue.SegmentWriteQueue;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.junit.Assume;
import org.junit.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.InstanceField;

public class AbstractRemoteSegmentArchiveWriterTest {

    @Test(timeout = 5000)
    public void testCloseShutsDownQueueAfterFlushFailureWithoutClosingArchive() throws Exception {
        Assume.assumeTrue("Asynchronous segment queue is disabled", SegmentWriteQueue.THREADS > 0);
        TestWriter writer = new TestWriter();
        SegmentWriteQueue queue = writer.queue();
        assertNotNull(queue);
        new InstanceField(SegmentWriteQueue.class.getDeclaredField("flushTimeoutNanos"), queue)
                .set(TimeUnit.MILLISECONDS.toNanos(250));

        SegmentWriteQueue queueProxy = Mockito.mock(SegmentWriteQueue.class, AdditionalAnswers.delegatesTo(queue));
        IOException closeFailure = new IOException("Simulated queue close failure");
        Mockito.doAnswer(invocation -> {
            queue.close();
            throw closeFailure;
        }).when(queueProxy).close();
        new InstanceField(AbstractRemoteSegmentArchiveWriter.class.getDeclaredField("queue"), writer)
                .set(Optional.of(queueProxy));

        Field executorField = SegmentWriteQueue.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        ExecutorService executor = (ExecutorService) executorField.get(queue);

        writer.writeSegment(1, 2, new byte[] { 1 }, 0, 1, 0, 0, false);
        try {
            writer.close();
            fail("Expected flush to time out");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("No segment write completed"));
            assertEquals(1, e.getSuppressed().length);
            assertTrue(e.getSuppressed()[0] == closeFailure);
        }

        assertTrue("The queue executor should terminate after close", executor.isTerminated());
        assertFalse("The archive must not be marked closed after a flush failure", writer.closedMarkerWritten);
    }

    private static class TestWriter extends AbstractRemoteSegmentArchiveWriter {

        private boolean closedMarkerWritten;

        private TestWriter() {
            super(Mockito.mock(IOMonitor.class), Mockito.mock(FileStoreMonitor.class));
        }

        private SegmentWriteQueue queue() {
            return queue.orElse(null);
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        protected void doWriteArchiveEntry(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size)
                throws IOException {
            throw new IOException("Simulated persistence failure");
        }

        @Override
        protected ByteBuffer doReadArchiveEntry(RemoteSegmentArchiveEntry indexEntry) {
            return null;
        }

        @Override
        protected void doWriteDataFile(byte[] data, String extension) {
        }

        @Override
        protected void afterQueueClosed() {
            closedMarkerWritten = true;
        }

        @Override
        protected void afterQueueFlushed() {
        }
    }
}
