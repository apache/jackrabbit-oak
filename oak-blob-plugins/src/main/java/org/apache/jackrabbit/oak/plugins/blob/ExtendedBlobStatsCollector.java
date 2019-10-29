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

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;

/**
 * Interface that adds stats to {@link BlobStatsCollector} for additional
 * capabilities in blob stores that are added via
 * {@link org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore}.
 */
public interface ExtendedBlobStatsCollector extends BlobStatsCollector {
    ExtendedBlobStatsCollector NOOP = new ExtendedBlobStatsCollector() {
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
        public void getRecordForIdCalled(long timeTaken, TimeUnit unit, long size) { }

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

        @Override
        public void metadataRecordAdded(long timeTaken, TimeUnit unit) { }

        @Override
        public void addMetadataRecordCompleted(String name) { }

        @Override
        public void addMetadataRecordFailed(String name) { }

        @Override
        public void getMetadataRecordCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getMetadataRecordCompleted(String name) { }

        @Override
        public void getMetadataRecordFailed(String name) { }

        @Override
        public void getAllMetadataRecordsCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void getAllMetadataRecordsCompleted(String prefix) { }

        @Override
        public void getAllMetadataRecordsFailed(String prefix) { }

        @Override
        public void metadataRecordExistsCalled(long timeTaken, TimeUnit unit) { }

        @Override
        public void metadataRecordExistsCompleted(String name) { }

        @Override
        public void metadataRecordExistsFailed(String name) { }

        @Override
        public void metadataRecordDeleted(long timeTaken, TimeUnit unit) { }

        @Override
        public void deleteMetadataRecordCompleted(String name) { }

        @Override
        public void deleteMetadataRecordFailed(String name) { }

        @Override
        public void allMetadataRecordsDeleted(long timeTaken, TimeUnit unit) { }

        @Override
        public void deleteAllMetadataRecordsCompleted(String prefix) { }

        @Override
        public void deleteAllMetadataRecordsFailed(String prefix) { }

        @Override
        public void initiateBlobUpload(long timeTaken, TimeUnit unit, long maxSize, int maxUris) { }

        @Override
        public void initiateBlobUploadCompleted() { }

        @Override
        public void initiateBlobUploadFailed() { }

        @Override
        public void completeBlobUpload(long timeTaken, TimeUnit unit) { }

        @Override
        public void completeBlobUploadCompleted(String id) { }

        @Override
        public void completeBlobUploadFailed() { }

        @Override
        public void getDownloadURICalled(long timeTaken, TimeUnit unit, String id) { }

        @Override
        public void getDownloadURICompleted(String uri) { }

        @Override
        public void getDownloadURIFailed() { }
    };


    /**
     * Called when a {@link org.apache.jackrabbit.core.data.DataRecord} is retrieved via
     * a call to {@link SharedDataStore#getRecordForId(DataIdentifier)}.
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param size size of the binary
     */
    void getRecordForIdCalled(long timeTaken, TimeUnit unit, long size);

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

    /**
     * Called when a call to {@link SharedDataStore#addMetadataRecord(File, String)} is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void metadataRecordAdded(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#addMetadataRecord(File, String)} is completed
     *
     * @param name name of the metadata record added
     */
    void addMetadataRecordCompleted(String name);

    /**
     * Called when a call to {@link SharedDataStore#addMetadataRecord(File, String)} fails
     *
     * @param name name of the metadata record
     */
    void addMetadataRecordFailed(String name);

    /**
     * Called when a call to {@link SharedDataStore#getMetadataRecord(String)} is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void getMetadataRecordCalled(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#getMetadataRecord(String)} is completed
     *
     * @param name name of the metadata record retrieved
     */
    void getMetadataRecordCompleted(String name);

    /**
     * Called when a call to {@link SharedDataStore#getMetadataRecord(String)} fails
     *
     * @param name name of the metadata record
     */
    void getMetadataRecordFailed(String name);

    /**
     * Called when a call to {@link SharedDataStore#getAllMetadataRecords(String)} is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void getAllMetadataRecordsCalled(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#getAllMetadataRecords(String)} is completed
     *
     * @param prefix prefix of the metadata records retrieved
     */
    void getAllMetadataRecordsCompleted(String prefix);

    /**
     * Called when a call to {@link SharedDataStore#getAllMetadataRecords(String)} fails
     *
     * @param prefix prefix of the metadata records
     */
    void getAllMetadataRecordsFailed(String prefix);

    /**
     * Called when a call to {@link SharedDataStore#metadataRecordExists(String)} is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void metadataRecordExistsCalled(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#metadataRecordExists(String)} is completed
     *
     * @param name name of the metadata record checked
     */
    void metadataRecordExistsCompleted(String name);

    /**
     * Called when a call to {@link SharedDataStore#metadataRecordExists(String)} fails
     *
     * @param name name of the metadata record
     */
    void metadataRecordExistsFailed(String name);

    /**
     * Called when a call to {@link SharedDataStore#deleteMetadataRecord(String)} is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void metadataRecordDeleted(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#deleteMetadataRecord(String)} is completed
     *
     * @param name name of the metadata record deleted
     */
    void deleteMetadataRecordCompleted(String name);

    /**
     * Called when a call to {@link SharedDataStore#deleteMetadataRecord(String)} fails
     *
     * @param name name of the metadata record
     */
    void deleteMetadataRecordFailed(String name);

    /**
     * Called when a call to {@link SharedDataStore#deleteAllMetadataRecords(String)} is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void allMetadataRecordsDeleted(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link SharedDataStore#deleteAllMetadataRecords(String)} is made
     *
     * @param prefix prefix of the metadata records deleted
     */
    void deleteAllMetadataRecordsCompleted(String prefix);

    /**
     * Called when a call to {@link SharedDataStore#deleteAllMetadataRecords(String)} is made
     *
     * @param prefix prefix of the metadata records
     */
    void deleteAllMetadataRecordsFailed(String prefix);

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#initiateBlobUpload(long, int)}
     * is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param maxSize size of binary to be uploaded
     * @param maxUris max number of uris requested
     */
    void initiateBlobUpload(long timeTaken, TimeUnit unit, long maxSize, int maxUris);

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#initiateBlobUpload(long, int)}
     * is completed
     */
    void initiateBlobUploadCompleted();

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#initiateBlobUpload(long, int)}
     * fails
     */
    void initiateBlobUploadFailed();

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#completeBlobUpload(String)} is
     * made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     */
    void completeBlobUpload(long timeTaken, TimeUnit unit);

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#completeBlobUpload(String)} is
     * completed
     *
     * @param id identifier of uploaded blob
     */
    void completeBlobUploadCompleted(String id);

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#completeBlobUpload(String)} fails
     */
    void completeBlobUploadFailed();

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#getDownloadURI(Blob, BlobDownloadOptions)}
     * is made
     *
     * @param timeTaken time taken to perform the operation
     * @param unit unit of time taken
     * @param id identifier of blob to be downloaded
     */
    void getDownloadURICalled(long timeTaken, TimeUnit unit, String id);

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#getDownloadURI(Blob, BlobDownloadOptions)}
     * is completed
     *
     * @param uri the uri generated for downloading
     */
    void getDownloadURICompleted(String uri);

    /**
     * Called when a call to {@link org.apache.jackrabbit.oak.api.blob.BlobAccessProvider#getDownloadURI(Blob, BlobDownloadOptions)}
     * fails
     */
    void getDownloadURIFailed();
}
