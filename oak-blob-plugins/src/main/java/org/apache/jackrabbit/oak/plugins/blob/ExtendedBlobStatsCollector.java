/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.plugins.blob;

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;

public interface ExtendedBlobStatsCollector extends BlobStatsCollector {
    ExtendedBlobStatsCollector NOOP = new ExtendedBlobStatsCollector() {
        @Override
        public void uploaded(long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void uploadCompleted(String blobId) { }

        @Override
        public void downloadCompleted(String blobId) { }

        @Override
        public void uploadFailed() { }

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
        public void recordAdded(long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void addRecordCompleted(String blobId) { }

        @Override
        public void addRecordFailed() { }

        @Override
        public void getRecordCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getRecordCompleted(String blobId) { }

        @Override
        public void getRecordFailed(String blobId) { }

        @Override
        public void getRecordIfStoredCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getRecordIfStoredCompleted(String blobId) { }

        @Override
        public void getRecordIfStoredFailed(String blobId) { }

        @Override
        public void getRecordFromReferenceCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getRecordFromReferenceCompleted(String reference) { }

        @Override
        public void getRecordFromReferenceFailed(String reference) { }

        @Override
        public void getRecordForIdCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getRecordForIdCompleted(String blobId) { }

        @Override
        public void getRecordForIdFailed(String blobId) { }

        @Override
        public void getAllRecordsCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getAllRecordsCompleted() { }

        @Override
        public void getAllIdentifiersCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getAllIdentifiersCompleted() { }

        @Override
        public void getAllIdentifiersFailed() { }
    };


    /**
     * Called when a {@link org.apache.jackrabbit.core.data.DataRecord} is retrieved via
     * a call to {@link SharedDataStore#getRecordForId(DataIdentifier)}.
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void getRecordForIdCalled(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#getRecordForId(DataIdentifier)} is completed
     *
     * @param blobId id of the record retrieved
     */
    void getRecordForIdCompleted(String blobId);

    /**
     * Called when a call to {@link SharedDataStore#getRecordForId(DataIdentifier)} fails
     *
     * @param blobId id of the record
     */
    void getRecordForIdFailed(String blobId);

    /**
     * Called when a call to {@link SharedDataStore#getAllRecords()} is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void getAllRecordsCalled(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#getAllRecords()} is completed
     */
    void getAllRecordsCompleted();
}
