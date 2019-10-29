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

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.osgi.annotation.versioning.ConsumerType;

/**
 * BlobStoreStatsCollector receives callback when blobs are written and read
 * from BlobStore
 */
@ConsumerType
public interface BlobStatsCollector {
    BlobStatsCollector NOOP = new BlobStatsCollector() {
        @Override
        public void uploaded(long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void uploadCompleted(String blobId) { }

        @Override
        public void uploadFailed() { }

        @Override
        public void downloaded(String blobId, long timeTaken, TimeUnit unit, long size) { }

        @Override
        public void downloadCompleted(String blobId) { }

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
        public void recordAdded(long timeTaken, TimeUnit unit, long size) { }

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
    };

    /**
     * Called when a binary content is written to BlobStore
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of binary content being written
     */
    void uploaded(long timeTaken, TimeUnit unit, long size);

    /**
     * Invoked when upload for a binary file get completed. In case of chunked
     * BlobStore this invoked when all the chunks have been uploaded
     *
     * @param blobId id of the blob which got uploaded. Even in case of chunked
     *               blobStores its the id of main blob
     */
    void uploadCompleted(String blobId);

    /**
     * Invoked when an upload of a binary fails.
     */
    void uploadFailed();

    /**
     * Called when a binary content is read from BlobStore
     *
     * @param blobId id of blob whose content are being read. For BlobStore
     *               which break up file in chunks it would be chunkId
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of binary content being read
     */
    void downloaded(String blobId, long timeTaken, TimeUnit unit, long size);

    /**
     * Invoked when download for a binary file get completed. In case of chunked
     * BlobStore this invoked when all the chunks have been downloaded
     *
     * @param blobId id of the blob which got downloaded. Even in case of chunked
     *               blobStores its the id of main blob
     */
    void downloadCompleted(String blobId);

    /**
     * Called when an attempt to download a binary file fails.
     *
     * @param blobId id of the blob for which the download failed
     */
    void downloadFailed(String blobId);

    /**
     * Called when a binary is deleted from the BlobStore
     *
     * @param blobId id of blob being deleted
     * @param timeTaken time taken to perform the delete
     * @param unit unit of time taken
     */
    void deleted(String blobId, long timeTaken, TimeUnit unit);

    /**
     * Called when deletion of a binary is completed.
     *
     * @param blobId id of the blob which was deleted
     */
    void deleteCompleted(String blobId);

    /**
     * Called when deletion of a binary fails.
     */
    void deleteFailed();

    /**
     * Called when deleting binaries older than a specified date, via
     * {@link org.apache.jackrabbit.core.data.DataStore#deleteAllOlderThan(long)}.
     *
     * @param timeTaken time taken to perform the deletion
     * @param unit unit of time taken
     * @param min time used for determining what to delete - older than this time gets deleted
     */
    void deletedAllOlderThan(long timeTaken, TimeUnit unit, long min);

    /**
     * Called when {@link org.apache.jackrabbit.core.data.DataStore#deleteAllOlderThan(long)} is completed.
     *
     * @param deletedCount count of records deleted
     */
    void deleteAllOlderThanCompleted(int deletedCount);

    /**
     * Called when {@link org.apache.jackrabbit.core.data.DataStore#deleteAllOlderThan(long)} fails.
     *
     * @param min time used for determining what to delete
     */
    void deleteAllOlderThanFailed(long min);

    /**
     * Called when a binary is added via {@link org.apache.jackrabbit.core.data.DataStore#addRecord(InputStream)}.
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of binary content being read
     */
    void recordAdded(long timeTaken, TimeUnit unit, long size);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#addRecord(InputStream)} is completed.
     *
     * @param blobId id of the record which was added
     */
    void addRecordCompleted(String blobId);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#addRecord(InputStream)} fails.
     */
    void addRecordFailed();

    /**
     * Called when a {@link org.apache.jackrabbit.core.data.DataRecord} is retrieved via
     * {@link org.apache.jackrabbit.core.data.DataStore#getRecord(DataIdentifier)}.
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of the binary
     */
    void getRecordCalled(long timeTaken, TimeUnit unit, long size);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#getRecord(DataIdentifier)} is completed.
     *
     * @param blobId id of the record retrieved
     */
    void getRecordCompleted(String blobId);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#getRecord(DataIdentifier)} fails.
     *
     * @param blobId id of the record
     */
    void getRecordFailed(String blobId);

    /**
     * Called when a {@link org.apache.jackrabbit.core.data.DataRecord} is retrieved via
     * {@link org.apache.jackrabbit.core.data.DataStore#getRecordIfStored(DataIdentifier)}.
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of the binary
     */
    void getRecordIfStoredCalled(long timeTaken, TimeUnit unit, long size);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#getRecordIfStored(DataIdentifier)} is completed.
     *
     * @param blobId id of the record retrieved
     */
    void getRecordIfStoredCompleted(String blobId);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#getRecordIfStored(DataIdentifier)} fails.
     *
     * @param blobId id of the record
     */
    void getRecordIfStoredFailed(String blobId);

    /**
     * Called when a {@link org.apache.jackrabbit.core.data.DataRecord} is retrieved via
     * {@link org.apache.jackrabbit.core.data.DataStore#getRecordFromReference(String)}.
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of the binary
     */
    void getRecordFromReferenceCalled(long timeTaken, TimeUnit unit, long size);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#getRecordFromReference(String)} is completed.
     *
     * @param reference reference of the record retrieved
     */
    void getRecordFromReferenceCompleted(String reference);

    /**
     * Called when a call to {@link org.apache.jackrabbit.core.data.DataStore#getRecordFromReference(String)} fails.
     *
     * @param reference reference of the record
     */
    void getRecordFromReferenceFailed(String reference);

    /**
     * Called when {@link DataStore#getAllIdentifiers()} is called.
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void getAllIdentifiersCalled(long timeTaken, TimeUnit unit);

    /**
     * Called when {@link DataStore#getAllIdentifiers()} is completed.
     */
    void getAllIdentifiersCompleted();

    /**
     * Called when {@link DataStore#getAllIdentifiers()} fails.
     */
    void getAllIdentifiersFailed();
}
