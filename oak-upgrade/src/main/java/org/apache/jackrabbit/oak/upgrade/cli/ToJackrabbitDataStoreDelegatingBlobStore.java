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
package org.apache.jackrabbit.oak.upgrade.cli;

import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;

import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps An Oak BlobStore around a Jackrabbit Datastore
 */
public class ToJackrabbitDataStoreDelegatingBlobStore implements BlobStore {

    private org.apache.jackrabbit.core.data.DataStore delegate;

    public ToJackrabbitDataStoreDelegatingBlobStore(
            org.apache.jackrabbit.core.data.DataStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public String writeBlob(InputStream inputStream) throws IOException {
        try {
            org.apache.jackrabbit.core.data.DataRecord record = delegate.addRecord(inputStream);
            return record.getIdentifier().toString();
        } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
            throw new IOException("Failed to write blob", ex);
        }
    }

    @Override
    public String writeBlob(InputStream inputStream, BlobOptions options) throws IOException {
        try {
            org.apache.jackrabbit.core.data.DataRecord record = delegate.addRecord(inputStream);
            return record.getIdentifier().toString();
        } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
            throw new IOException("Failed to write blob", ex);
        }
    }

    @Override
    public int readBlob(String blobId, long pos, byte[] buff, int off, int length)
            throws IOException {

        try (InputStream is = getInputStream(blobId)) {
            return is.readNBytes(buff, off, length);
        }
    }

    @Override
    public long getBlobLength(String blobId) throws IOException {
        try {
            org.apache.jackrabbit.core.data.DataRecord record = delegate.getRecord(new org.apache.jackrabbit.core.data.DataIdentifier(blobId));
            return record.getLength();
        } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
            throw new IOException("Failed to get blob length", ex);
        }
    }

    @Override
    public InputStream getInputStream(String blobId) throws IOException {
        try {
            org.apache.jackrabbit.core.data.DataRecord record = delegate.getRecord(new org.apache.jackrabbit.core.data.DataIdentifier(blobId));
            return record.getStream();
        } catch (org.apache.jackrabbit.core.data.DataStoreException ex) {
            throw new IOException("Failed to get input stream", ex);
        }
    }

    @Override
    public String getBlobId(String reference) {
        // Usually same as blobId for Jackrabbit datastore
        return reference;
    }

    @Override
    public String getReference(String blobId) {
        // Jackrabbit DataStore doesn't distinguish strongly here
        return blobId;
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }
}
