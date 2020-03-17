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

package org.apache.jackrabbit.oak.spi.blob.split;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSplitBlobStore implements SplitBlobStore {

    private static final Logger log = LoggerFactory.getLogger(DefaultSplitBlobStore.class);

    private static final String OLD_BLOBSTORE_PREFIX = "o_";

    private static final String NEW_BLOBSTORE_PREFIX = "n_";

    private final BlobStore oldBlobStore;

    private final BlobStore newBlobStore;

    private final BlobIdSet migratedBlobs;

    public DefaultSplitBlobStore(String repositoryDir, BlobStore oldBlobStore, BlobStore newBlobStore) {
        this.oldBlobStore = oldBlobStore;
        this.newBlobStore = newBlobStore;
        this.migratedBlobs = new BlobIdSet(repositoryDir, "migrated_blobs.txt");
    }

    public boolean isMigrated(String blobId) throws IOException {
        return migratedBlobs.contains(blobId);
    }

    @Override
    public String writeBlob(InputStream in) throws IOException {
        String blobId = newBlobStore.writeBlob(in);
        migratedBlobs.add(blobId);
        return blobId;
    }

    /**
     * Ignores the options provided and delegates to {@link #writeBlob(InputStream)}.
     *
     * @param in the input stream to write
     * @param options the options to use
     * @return
     * @throws IOException
     */
    @Override
    public String writeBlob(InputStream in, BlobOptions options) throws IOException {
        return writeBlob(in);
    }

    @Override
    public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
        return chooseBlobStoreByBlobId(blobId).readBlob(blobId, pos, buff, off, length);
    }

    @Override
    public long getBlobLength(String blobId) throws IOException {
        return chooseBlobStoreByBlobId(blobId).getBlobLength(blobId);
    }

    @Override
    public InputStream getInputStream(String blobId) throws IOException {
        return chooseBlobStoreByBlobId(blobId).getInputStream(blobId);
    }

    @Override
    public String getBlobId(String reference) {
        if (reference.startsWith(NEW_BLOBSTORE_PREFIX)) {
            return newBlobStore.getBlobId(reference.substring(NEW_BLOBSTORE_PREFIX.length()));
        } else if (reference.startsWith(OLD_BLOBSTORE_PREFIX)) {
            return oldBlobStore.getBlobId(reference.substring(OLD_BLOBSTORE_PREFIX.length()));
        } else {
            log.error("Invalid reference: {}", reference);
            return null;
        }
    }

    @Override
    public String getReference(String blobId) {
        try {
            if (isMigrated(blobId)) {
                return NEW_BLOBSTORE_PREFIX + newBlobStore.getReference(blobId);
            } else {
                return OLD_BLOBSTORE_PREFIX + oldBlobStore.getReference(blobId);
            }
        } catch (IOException e) {
            log.error("Can't get reference", e);
            return null;
        }
    }

    private BlobStore chooseBlobStoreByBlobId(String blobId) throws IOException {
        if (isMigrated(blobId) || oldBlobStore == null) {
            return newBlobStore;
        } else {
            return oldBlobStore;
        }
    }

    @Override
    public String toString() {
        return String.format("SplitBlobStore[old=%s, new=%s]", oldBlobStore, newBlobStore);
    }
}
