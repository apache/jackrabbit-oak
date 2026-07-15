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

package org.apache.jackrabbit.oak.spi.blob.stats;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.Test;

public class StatsCollectingStreamsTest {

    @Test
    public void downloadCallback() throws Exception {
        NullInputStream in = new NullInputStream(1042);
        TestCollector stats = new TestCollector();
        InputStream wrappedStream = StatsCollectingStreams.wrap(stats, "foo", in);
        assertEquals(0, stats.callbackCount);

        //Copy the content to get size
        CountingOutputStream cos = new CountingOutputStream(new NullOutputStream());
        IOUtils.copy(wrappedStream, cos);

        assertEquals(1042, cos.getCount());

        //Stream not closed so no callback
        assertEquals(0, stats.callbackCount);

        wrappedStream.close();
        assertEquals(1042, stats.size);
        assertEquals(1, stats.downloadCompletedCount);
    }

    private static class TestCollector implements BlobStatsCollector {
        long size = -1;
        int callbackCount;
        int downloadCompletedCount;

        @Override
        public void uploaded(long timeTaken, TimeUnit unit, long size) {

        }

        @Override
        public void uploadCompleted(String blobId) {

        }

        @Override
        public void uploadFailed() { }

        @Override
        public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) {
            callbackCount++;
            this.size = size;
        }

        @Override
        public void downloadCompleted(String blobId) {
            downloadCompletedCount++;
        }

        @Override
        public void downloadFailed(String blobId) { }

        @Override
        public void deleted(String blobId, long timeTaken, TimeUnit unit) { }

        @Override
        public void deleteCompleted(String blobId) { }

        @Override
        public void deleteFailed() { }

        @Override
        public void deletedAllOlderThan(long timeTaken, TimeUnit unit, long min) { }

        @Override
        public void deleteAllOlderThanCompleted(int deletedCount) { }

        @Override
        public void deleteAllOlderThanFailed(long min) { }

        @Override
        public void recordAdded(long timeTaken, TimeUnit unit, long size ) { }

        @Override
        public void addRecordCompleted(String blobId) { }

        @Override
        public void addRecordFailed() { }

        @Override
        public void getRecordCalled(long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void getRecordCompleted(String blobId) { }

        @Override
        public void getRecordFailed(String blobId) { }

        @Override
        public void getRecordIfStoredCalled(long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void getRecordIfStoredCompleted(String blobId) { }

        @Override
        public void getRecordIfStoredFailed(String blobId) { }

        @Override
        public void getRecordFromReferenceCalled(long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void getRecordFromReferenceCompleted(String reference) { }

        @Override
        public void getRecordFromReferenceFailed(String reference) { }

        @Override
        public void getAllIdentifiersCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getAllIdentifiersCompleted() { }

        @Override
        public void getAllIdentifiersFailed() { }
    }
}