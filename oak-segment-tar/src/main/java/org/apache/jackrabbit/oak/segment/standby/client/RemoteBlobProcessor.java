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

import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RemoteBlobProcessor implements BlobProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RemoteBlobProcessor.class);

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

        // Shortcut: If the Blob ID is null, this is an inline binary and we
        // don't have to fetch it.

        String blobId = blob.getBlobId();

        if (blobId == null) {
            return false;
        }

        // Shortcut: If the Blob Store is able to retrieve a non-null reference
        // to the Blob, we can be sure that the Blob is already stored locally.
        // We don't have to download it.

        String reference;

        try {
            reference = blob.getReference();
        } catch (Exception e) {
            logger.warn("Unable to read a reference for blob {}", blobId, e);
            reference = null;
        }

        if (reference != null) {
            return false;
        }

        // Worst case: A null reference to the Blob might just mean that the
        // Blob Store doesn't support references. The Blob might still be stored
        // locally. We have to retrieve an InputStream for the Blob, and
        // perform a tentative read in order to overcome a possible lazy
        // implementation of the returned InputStream.

        InputStream data;

        try {
            data = blobStore.getInputStream(blobId);
        } catch (Exception e) {
            logger.warn("Unable to open a stream for blob {}, the blob will be downloaded", blobId, e);
            return true;
        }

        if (data == null) {
            return true;
        }

        try {
            data.read();
        } catch (Exception e) {
            logger.warn("Unable to read the content for blob {}, the blob will be downloaded", blobId, e);
            return true;
        } finally {
            closeQuietly(data);
        }

        return false;
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
