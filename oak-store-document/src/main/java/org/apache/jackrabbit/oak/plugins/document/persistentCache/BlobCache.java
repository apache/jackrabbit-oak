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
package org.apache.jackrabbit.oak.plugins.document.persistentCache;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache.GenerationCache;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.StreamStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A persistent blob cache. Only blobs that are smaller than 10% of the maximum
 * cache size are stored.
 */
public class BlobCache implements GarbageCollectableBlobStore, GenerationCache, Closeable {

    static final Logger LOG = LoggerFactory.getLogger(BlobCache.class);

    private final GarbageCollectableBlobStore base;
    private final PersistentCache cache;
    private final MultiGenerationMap<String, byte[]> meta;
    private MultiGenerationMap<Long, byte[]> data;
    private StreamStore streamStore;
    private long maxEntrySize;
    
    public BlobCache(
            PersistentCache cache, 
            GarbageCollectableBlobStore base) {
        this.cache = cache;
        this.base = base;
        data = new MultiGenerationMap<Long, byte[]>();
        meta = new MultiGenerationMap<String, byte[]>();
        maxEntrySize = cache.getMaxBinaryEntrySize();
    }
    
    @Override
    public CacheType getType() {
        return CacheType.BLOB;
    }
    
    @Override
    public void addGeneration(int generation, boolean readOnly) {
        CacheMap<Long, byte[]> d = cache.openMap(generation, "data", 
                new MVMap.Builder<Long, byte[]>());
        data.addReadMap(generation, d);
        CacheMap<String, byte[]> m = cache.openMap(generation, "meta", 
                new MVMap.Builder<String, byte[]>());
        meta.addReadMap(generation, m);
        if (!readOnly) {
            // the order is important:
            // if we switch the data first,
            // we could end up with the data in store 1
            // but the metadata in store 2 - which could
            // result in a data block not found if store 1
            // is removed later on
            meta.setWriteMap(m);
            data.setWriteMap(d);
        }
        if (streamStore == null) {
            streamStore = new StreamStore(data);
        }
    }    
    
    @Override
    public void removeGeneration(int generation) {
        data.removeReadMap(generation);
        meta.removeReadMap(generation);
    }
    
    @Override
    public InputStream getInputStream(String blobId) throws IOException {
        if (streamStore == null) {
            return base.getInputStream(blobId);
        }
        cache.switchGenerationIfNeeded();
        byte[] id = meta.get(blobId);
        if (id == null) {
            long length = base.getBlobLength(blobId);
            InputStream in = base.getInputStream(blobId);
            if (length < base.getBlockSizeMin()) {
                // in-place
                return in;
            }
            if (length > maxEntrySize) {
                // too large, don't cache
                return in;
            }
            id = streamStore.put(in);
            in.close();
            meta.put(blobId, id);
        }
        return streamStore.get(id);
    }

    @Override
    public String writeBlob(InputStream in) throws IOException {
        // TODO maybe copy the binary to the cache in a background thread
        return base.writeBlob(in);
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
    public int readBlob(String blobId, long pos, byte[] buff, int off,
            int length) throws IOException {
        InputStream in = getInputStream(blobId);
        long remainingSkip = pos;
        while (remainingSkip > 0) {
            remainingSkip -= in.skip(remainingSkip);
        }
        return in.read(buff, off, length);
    }

    @Override
    public long getBlobLength(String blobId) throws IOException {
        return base.getBlobLength(blobId);
    }

    @Override
    @CheckForNull
    public String getBlobId(@Nonnull String reference) {
        return base.getBlobId(reference);
    }

    @Override
    @CheckForNull
    public String getReference(@Nonnull String blobId) {
        return base.getReference(blobId);
    }

    @Override
    public void clearCache() {
        base.clearCache();
    }

    @Override
    public void clearInUse() {
        base.clearInUse();
    }

    @Override
    @Deprecated
    public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        return base.deleteChunks(chunkIds, maxLastModifiedTime);
    }

    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        return base.countDeleteChunks(chunkIds, maxLastModifiedTime);
    }
    
    @Override
    public Iterator<String> getAllChunkIds(long maxLastModifiedTime) throws Exception {
        return base.getAllChunkIds(maxLastModifiedTime);
    }

    @Override
    public long getBlockSizeMin() {
        return base.getBlockSizeMin();
    }

    @Override
    public Iterator<String> resolveChunks(String blobId) throws IOException {
        return base.resolveChunks(blobId);
    }

    @Override
    public void setBlockSize(int x) {
        base.setBlockSize(x);
    }

    @Override
    public void startMark() throws IOException {
        base.startMark();
    }

    @Override
    public int sweep() throws IOException {
        return base.sweep();
    }

    @Override
    public String writeBlob(String tempFileName) throws IOException {
        return base.writeBlob(tempFileName);
    }

    @Override
    public void receive(ByteBuffer buff) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (base instanceof Closeable) {
            ((Closeable)base).close();
        }
    }
    
}
