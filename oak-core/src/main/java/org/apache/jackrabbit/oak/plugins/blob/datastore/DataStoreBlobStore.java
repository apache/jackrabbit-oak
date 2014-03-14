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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Iterator;

import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;

/**
 * BlobStore wrapper for DataStore. Wraps Jackrabbit 2 DataStore and expose them as BlobStores
 * It also handles inlining binaries if there size is smaller than
 * {@link org.apache.jackrabbit.core.data.DataStore#getMinRecordLength()}
 */
public class DataStoreBlobStore implements DataStore, BlobStore, GarbageCollectableBlobStore {
    private final DataStore delegate;

    public DataStoreBlobStore(DataStore delegate) {
        this.delegate = delegate;
    }

    //~----------------------------------< DataStore >

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        if(isInMemoryRecord(identifier)){
            return getDataRecord(identifier.toString());
        }
        return delegate.getRecordIfStored(identifier);
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        if(isInMemoryRecord(identifier)){
            return getDataRecord(identifier.toString());
        }
        return delegate.getRecord(identifier);
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        return delegate.getRecordFromReference(reference);
    }

    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        try {
            return writeStream(stream);
        } catch (IOException e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        delegate.updateModifiedDateOnAccess(before);
    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        return delegate.deleteAllOlderThan(min);
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return delegate.getAllIdentifiers();
    }

    @Override
    public void init(String homeDir) throws RepositoryException {
        throw new UnsupportedOperationException("DataStore cannot be initialized again");
    }

    @Override
    public int getMinRecordLength() {
        return delegate.getMinRecordLength();
    }

    @Override
    public void close() throws DataStoreException {
        delegate.close();
    }

    //~-------------------------------------------< BlobStore >

    @Override
    public String writeBlob(InputStream stream) throws IOException {
        boolean threw = true;
        try {
            String id = writeStream(stream).getIdentifier().toString();
            threw = false;
            return id;
        } catch (DataStoreException e) {
            throw new IOException(e);
        } finally {
            //DataStore does not closes the stream internally
            //So close the stream explicitly
            Closeables.close(stream, threw);
        }
    }

    @Override
    public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
        //This is inefficient as repeated calls for same blobId would involve opening new Stream
        //instead clients should directly access the stream from DataRecord by special casing for
        //BlobStore which implements DataStore
        InputStream stream = getStream(blobId);
        boolean threw = true;
        try {
            ByteStreams.skipFully(stream, pos);
            int readCount = stream.read(buff, off, length);
            threw = false;
            return readCount;
        } finally {
            Closeables.close(stream, threw);
        }
    }

    @Override
    public long getBlobLength(String blobId) throws IOException {
        try {
            return getDataRecord(blobId).getLength();
        } catch (DataStoreException e) {
            throw new IOException(e);
        }
    }

    @Override
    public InputStream getInputStream(String blobId) throws IOException {
        return getStream(blobId);
    }

    //~-------------------------------------------< GarbageCollectableBlobStore >

    @Override
    public void setBlockSize(int x) {

    }

    @Override
    public String writeBlob(String tempFileName) throws IOException {
        File file = new File(tempFileName);
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            return writeBlob(in);
        } finally {
            org.apache.commons.io.IOUtils.closeQuietly(in);
            FileUtils.forceDelete(file);
        }
    }

    @Override
    public int sweep() throws IOException {
        return 0;
    }

    @Override
    public void startMark() throws IOException {

    }

    @Override
    public void clearInUse() {
        delegate.clearInUse();
    }

    @Override
    public void clearCache() {

    }

    @Override
    public long getBlockSizeMin() {
        return 0;
    }

    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        //TODO Ignores the maxLastModifiedTime currently.
        return Iterators.transform(delegate.getAllIdentifiers(), new Function<DataIdentifier, String>() {
            @Nullable
            @Override
            public String apply(@Nullable DataIdentifier input) {
                return input.toString();
            }
        });
    }

    @Override
    public boolean deleteChunk(String chunkId, long maxLastModifiedTime) throws Exception {
        if (delegate instanceof MultiDataStoreAware) {
            DataIdentifier identifier = new DataIdentifier(chunkId);
            DataRecord dataRecord = delegate.getRecord(identifier);
            if ((maxLastModifiedTime <= 0)
                    || dataRecord.getLastModified() <= maxLastModifiedTime) {
                ((MultiDataStoreAware) delegate).deleteRecord(identifier);
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<String> resolveChunks(String blobId) throws IOException {
        return Iterators.singletonIterator(blobId);
    }

    @Override
    public String toString() {
        return String.format("DataStore backed BlobStore [%s]", delegate.getClass().getName());
    }

    public DataStore getDataStore() {
        return delegate;
    }

    private InputStream getStream(String blobId) throws IOException {
        try {
            return getDataRecord(blobId).getStream();
        } catch (DataStoreException e) {
            throw new IOException(e);
        }
    }

    private DataRecord getDataRecord(String blobId) throws DataStoreException {
        DataRecord id;
        if(InMemoryDataRecord.isInstance(blobId)){
            id = InMemoryDataRecord.getInstance(blobId);
        }else{
            id = delegate.getRecord(new DataIdentifier(blobId));
            Preconditions.checkNotNull(id, "No DataRecord found for blodId [%s]", blobId);
        }
        return id;
    }

    private boolean isInMemoryRecord(DataIdentifier identifier){
        return InMemoryDataRecord.isInstance(identifier.toString());
    }

    /**
     * Create a BLOB value from in input stream. Small objects will create an in-memory object,
     * while large objects are stored in the data store
     *
     * @param in the input stream
     * @return the value
     */
    private DataRecord writeStream(InputStream in) throws IOException, DataStoreException {
        int maxMemorySize = Math.max(0, delegate.getMinRecordLength() + 1);
        byte[] buffer = new byte[maxMemorySize];
        int pos = 0, len = maxMemorySize;
        while (pos < maxMemorySize) {
            int l = in.read(buffer, pos, len);
            if (l < 0) {
                break;
            }
            pos += l;
            len -= l;
        }
        DataRecord record;
        if (pos < maxMemorySize) {
            // shrink the buffer
            byte[] data = new byte[pos];
            System.arraycopy(buffer, 0, data, 0, pos);
            record = InMemoryDataRecord.getInstance(data);
        } else {
            // a few bytes are already read, need to re-build the input stream
            in = new SequenceInputStream(new ByteArrayInputStream(buffer, 0, pos), in);
            record = delegate.addRecord(in);
        }
        return record;
    }
}
