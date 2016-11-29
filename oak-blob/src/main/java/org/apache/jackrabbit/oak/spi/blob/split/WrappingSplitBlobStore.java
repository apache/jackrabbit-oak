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
import org.apache.jackrabbit.oak.spi.blob.BlobStoreWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WrappingSplitBlobStore implements BlobStoreWrapper, SplitBlobStore {

    private static final Logger log = LoggerFactory.getLogger(WrappingSplitBlobStore.class);

    private DefaultSplitBlobStore splitBlobStore;

    private final String repositoryDir;

    private final BlobStore newBlobStore;

    public WrappingSplitBlobStore(String repositoryDir, BlobStore newBlobStore) {
        this.repositoryDir = repositoryDir;
        this.newBlobStore = newBlobStore;
    }

    @Override
    public void setBlobStore(BlobStore blobStore) {
        log.info("Internal blob store set: {}", blobStore);
        splitBlobStore = new DefaultSplitBlobStore(repositoryDir, blobStore, newBlobStore);
    }

    private SplitBlobStore getSplitBlobStore() {
        if (splitBlobStore == null) {
            throw new IllegalStateException("The old blob store hasn't been set yet.");
        }
        return splitBlobStore;
    }

    @Override
    public String writeBlob(InputStream in) throws IOException {
        return getSplitBlobStore().writeBlob(in);
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
        return getSplitBlobStore().readBlob(blobId, pos, buff, off, length);
    }

    @Override
    public long getBlobLength(String blobId) throws IOException {
        return getSplitBlobStore().getBlobLength(blobId);
    }

    @Override
    public InputStream getInputStream(String blobId) throws IOException {
        return getSplitBlobStore().getInputStream(blobId);
    }

    @Override
    public String getBlobId(String reference) {
        return getSplitBlobStore().getBlobId(reference);
    }

    @Override
    public String getReference(String blobId) {
        return getSplitBlobStore().getReference(blobId);
    }

    @Override
    public boolean isMigrated(String blobId) throws IOException {
        return getSplitBlobStore().isMigrated(blobId);
    }

}
