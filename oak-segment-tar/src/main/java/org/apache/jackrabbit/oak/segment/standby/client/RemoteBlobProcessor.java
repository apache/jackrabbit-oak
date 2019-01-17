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

package org.apache.jackrabbit.oak.segment.standby.client;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

class RemoteBlobProcessor implements BlobProcessor {

    interface BlobDownloader {

        InputStream downloadBlob(String blobId) throws InterruptedException;

    }

    private final BlobStore blobStore;

    private final BlobDownloader blobDownloader;

    RemoteBlobProcessor(BlobStore blobStore, BlobDownloader blobDownloader) {
        this.blobStore = blobStore;
        this.blobDownloader = blobDownloader;
    }

    @Override
    public void processBinary(Blob b) throws InterruptedException {
        if (b instanceof SegmentBlob) {
            fetchBinary((SegmentBlob) b);
        } else {
            throw new BlobTypeUnknownException();
        }
    }

    private void fetchBinary(SegmentBlob blob) throws InterruptedException {
        if (shouldFetchBinary(blob)) {
            fetchAndStoreBlob(blob.getBlobId());
        }
    }

    private boolean shouldFetchBinary(SegmentBlob blob) {
        return blob.isExternal() && blob.getReference() == null && blob.getBlobId() != null;
    }

    private void fetchAndStoreBlob(String blobId) throws InterruptedException {
        try (InputStream in = downloadBlob(blobId)) {
            writeBlob(blobId, in);
        } catch (IOException e) {
            throw new BlobWriteException(blobId, e);
        }
    }

    private void writeBlob(String blobId, InputStream stream) {
        try {
            blobStore.writeBlob(stream);
        } catch (IOException e) {
            throw new BlobWriteException(blobId, e);
        }
    }

    private InputStream downloadBlob(String blobId) throws InterruptedException {
        return blobDownloader.downloadBlob(blobId);
    }

}
