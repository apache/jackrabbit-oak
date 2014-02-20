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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import org.apache.jackrabbit.core.data.CachingDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.mk.blobs.GarbageCollectableBlobStore;
import org.apache.jackrabbit.mk.util.Cache;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;

/**
 * A {@link BlobStore} implementation which is a compatibility wrapper for
 * Jackrabbit {@link DataStore}.
 * <p>
 * Uses a 2 level cache to improve random read performance.
 * 
 * Caches the {@link InputStream} until fully read or closed. Number of streams
 * cached are controlled by the
 * {@link DataStoreConfiguration#getStreamCacheSize()} parameter
 * 
 * Also, uses a 16MB bytes[] cache.
 * 
 */
public class DataStoreBlobStore implements GarbageCollectableBlobStore,
        Cache.Backend<DataStoreBlobStore.LogicalBlockId, DataStoreBlobStore.Data> {

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(DataStoreBlobStore.class);

    protected static final int BLOCK_SIZE_LIMIT = 40;

    private static final int DEFAULT_STREAM_CACHE_SIZE = 256;

    /**
     * The size of a block. 128 KB has been found to be as fast as larger
     * values, and faster than smaller values. 2 MB results in less files.
     */
    private int blockSize = 2 * 1024 * 1024;

    /**
     * The block cache (16 MB). Caches blocks up to blockSize.
     */
    private Cache<LogicalBlockId, Data> blockCache = Cache.newInstance(this, 16 * 1024 * 1024);

    /** The stream cache size. */
    protected int streamCacheSize;

    /**
     * The stream cache caches a number of streams to avoid opening a new stream
     * on every random access read.
     */
    private LoadingCache<String, InputStream> streamCache;

    private LoadingCache<String, Long> fileLengthCache;

    /** The data store. */
    private DataStore dataStore;

    /**
     * Gets the stream cache size.
     * 
     * @return the stream cache size
     */
    protected int getStreamCacheSize() {
        return streamCacheSize;
    }

    /**
     * Sets the stream cache size.
     * 
     * @param streamCacheSize
     *            the new stream cache size
     */
    protected void setStreamCacheSize(int streamCacheSize) {
        this.streamCacheSize = streamCacheSize;
    }

    /**
     * Sets the block size.
     * 
     * @param x
     *            the new block size
     */
    public final void setBlockSize(final int x) {
        validateBlockSize(x);
        this.blockSize = x;
    }

    /**
     * Validate block size.
     * 
     * @param x
     *            the x
     */
    private static void validateBlockSize(final int x) {
        if (x < BLOCK_SIZE_LIMIT) {
            throw new IllegalArgumentException("The minimum size must be bigger "
                    + "than a content hash itself; limit = " + BLOCK_SIZE_LIMIT);
        }
    }

    /**
     * Initialized the blob store.
     * 
     * @param dataStore
     *            the data store
     * @param streamCacheSize
     *            the stream cache size
     */
    public void init(DataStore dataStore) {
        if (streamCacheSize <= 0) {
            streamCacheSize = DEFAULT_STREAM_CACHE_SIZE;
        }

        streamCache = CacheBuilder.newBuilder().maximumSize(streamCacheSize)
                .removalListener(new RemovalListener<String, InputStream>() {
                    public void onRemoval(RemovalNotification<String, InputStream> removal) {
                        InputStream stream = removal.getValue();
                        IOUtils.closeQuietly(stream);
                    }
                }).build(new CacheLoader<String, InputStream>() {
                    public InputStream load(String key) throws Exception {
                        return loadStream(key);
                    }
                });
        fileLengthCache = CacheBuilder.newBuilder().maximumSize(streamCacheSize)
                .build(new CacheLoader<String, Long>() {
                    @Override
                    public Long load(String key) throws Exception {
                        return getBlobLength(key);
                    }
                });
        this.dataStore = dataStore;
    }

    /**
     * Writes the input stream to the data store.
     */
    @Override
    public String writeBlob(InputStream in) throws IOException {
        try {
            // add the record in the data store
            DataRecord dataRec = dataStore.addRecord(in);
            return dataRec.getIdentifier().toString();
        } catch (DataStoreException e) {
            throw new IOException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    /**
     * Reads the blob with the given blob id and range.
     */
    @Override
    public int readBlob(String blobId, long pos, byte[] buff, int off, int length) throws IOException {
        if (Strings.isNullOrEmpty(blobId)) {
            return -1;
        }

        long blobLength;
        try {
            blobLength = fileLengthCache.get(blobId);
        } catch (ExecutionException e) {
            LOG.debug("File length cache error", e);
            blobLength = getBlobLength(blobId);
        }
        LOG.debug("read {" + blobId + "}, {" + blobLength + "}");

        long position = pos;
        int offset = off;

        if (position < blobLength) {
            int totalLength = 0;
            long bytesLeft = ((position + length) > blobLength ? blobLength - position : length);

            // Reads all the logical blocks satisfying the required range
            while (bytesLeft > 0) {
                long posBlockStart = position / blockSize;
                int posOffsetInBlock = (int) (position - posBlockStart * blockSize);

                byte[] block = readBlock(blobId, posBlockStart);

                long bytesToRead = Math.min(bytesLeft,
                        Math.min((blobLength - posOffsetInBlock), (blockSize - posOffsetInBlock)));
                System.arraycopy(block, posOffsetInBlock, buff, offset, (int) bytesToRead);

                position += bytesToRead;
                offset += bytesToRead;
                totalLength += bytesToRead;
                bytesLeft -= bytesToRead;
            }
            return totalLength;
        } else {
            LOG.trace("Blob read for pos " + pos + "," + (pos + length - 1) + " out of range");
            return -1;
        }
    }

    /**
     * Gets the data store.
     * 
     * @return the data store
     */
    public DataStore getDataStore() {
        return dataStore;
    }

    /**
     * Sets the data store.
     * 
     * @param dataStore
     *            the data store
     */
    protected void setDataStore(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Load the block to the cache.
     */
    @Override
    public final Data load(final LogicalBlockId id) {
        byte[] data;
        try {
            data = readBlockFromBackend(id);
        } catch (Exception e) {
            throw new RuntimeException("failed to read block from backend, id " + id, e);
        }
        if (data == null) {
            throw new IllegalArgumentException("The block with id " + id + " was not found");
        }
        LOG.debug("Read from backend (Cache Miss): " + id);
        return new Data(data);
    }

    /**
     * Gets the length of the blob identified by the blobId.
     */
    @Override
    public final long getBlobLength(final String blobId) throws IOException {
        if (Strings.isNullOrEmpty(blobId)) {
            return 0;
        }

        Long length = null;
        try {
            if (dataStore instanceof CachingDataStore) {
                length = ((CachingDataStore) dataStore).getLength(new DataIdentifier(blobId));
            } else {
                length = dataStore.getRecord(new DataIdentifier(blobId)).getLength();
            }
            return length;
        } catch (DataStoreException e) {
            throw new IOException("Could not get length of blob for id " + blobId, e);
        }
    }

    /**
     * Reads block from backend.
     * 
     * @param id
     *            the id
     * @return the byte[]
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private byte[] readBlockFromBackend(final LogicalBlockId id) throws IOException {
        String key = StringUtils.convertBytesToHex(id.digest);
        InputStream stream = null;
        try {
            stream = streamCache.get(key);
        } catch (ExecutionException e) {
            LOG.debug("Error retrieving from stream cache : " + key, e);
        }

        byte[] block = new byte[blockSize];
        org.apache.commons.io.IOUtils.read(stream, block, 0, blockSize);

        if ((stream != null) && (stream.available() <= 0)) {
            streamCache.invalidate(key);
        }
        return block;
    }

    /**
     * Loads the stream from the data store.
     * 
     * @param key
     *            the key
     * @return the input stream
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private InputStream loadStream(String key) throws IOException {
        InputStream stream = null;
        try {
            stream = dataStore.getRecord(new DataIdentifier(key)).getStream();
        } catch (DataStoreException e) {
            throw new IOException("Could not read blob for id " + key, e);
        }
        return stream;
    }

    /**
     * Reads block.
     * 
     * @param blobId
     *            the blob id
     * @param posStart
     *            the pos start
     * @return the byte[]
     * @throws Exception
     *             the exception
     */
    private byte[] readBlock(final String blobId, final long posStart) throws IOException {
        byte[] digest = StringUtils.convertHexToBytes(blobId);
        LogicalBlockId id = new LogicalBlockId(digest, posStart);

        LOG.debug("Trying to read from cache : " + blobId + ", " + posStart);

        return blockCache.get(id).data;
    }

    /**
     * Delete all blobs older than.
     * 
     * @param time
     *            the time
     * @return the int
     * @throws Exception
     *             the exception
     */
    public int deleteAllOlderThan(long time) throws Exception {
        return dataStore.deleteAllOlderThan(time);
    }

    /**
     * A file is divided into logical chunks. Blocks are small enough to fit in
     * memory, so they can be cached.
     */
    public static class LogicalBlockId {

        /** The digest. */
        final byte[] digest;

        /** The starting pos. */
        final long pos;

        /**
         * Instantiates a new logical block id.
         * 
         * @param digest
         *            the digest
         * @param pos
         *            the starting position of the block
         */
        LogicalBlockId(final byte[] digest, final long pos) {
            this.digest = digest;
            this.pos = pos;
        }

        @Override
        public final boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || !(other instanceof LogicalBlockId)) {
                return false;
            }
            LogicalBlockId o = (LogicalBlockId) other;
            return Arrays.equals(digest, o.digest) && pos == o.pos;
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(digest) ^ (int) (pos >> 32) ^ (int) pos;
        }

        @Override
        public final String toString() {
            return StringUtils.convertBytesToHex(digest) + "@" + pos;
        }

        /**
         * Gets the digest.
         * 
         * @return the digest
         */
        public final byte[] getDigest() {
            return digest;
        }

        /**
         * Gets the starting position.
         * 
         * @return the starting position
         */
        public final long getPos() {
            return pos;
        }
    }

    /**
     * The data for a block.
     */
    public static class Data implements Cache.Value {

        /** The data. */
        final byte[] data;

        /**
         * Instantiates a new data.
         * 
         * @param data
         *            the data
         */
        Data(final byte[] data) {
            this.data = data;
        }

        @Override
        public final String toString() {
            String s = StringUtils.convertBytesToHex(data);
            return s.length() > 100 ? s.substring(0, 100) + ".. (len=" + data.length + ")" : s;
        }

        @Override
        public final int getMemory() {
            return data.length;
        }
    }

    @Override
    public String writeBlob(String tempFileName) throws IOException {
        File file = new File(tempFileName);
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            return writeBlob(in);
        } finally {
            if (in != null) {
                in.close();
            }
            file.delete();
        }
    }

    @Override
    public int sweep() throws IOException {
        // no-op
        return 0;
    }

    @Override
    public void startMark() throws IOException {
    }

    @Override
    public void clearInUse() {
        dataStore.clearInUse();
    }

    @Override
    public void clearCache() {
        // no-op
    }

    @Override
    public long getBlockSizeMin() {
        // no-op
        return 0;
    }

    /**
     * Ignores the maxLastModifiedTime currently.
     */
    @Override
    public Iterator<String> getAllChunkIds(
            long maxLastModifiedTime) throws Exception {
        return new DataStoreIterator(dataStore.getAllIdentifiers());
    }

    @Override
    public boolean deleteChunk(String blobId) throws Exception {
        ((MultiDataStoreAware) dataStore).deleteRecord(new DataIdentifier(blobId));
        return true;
    }

    @Override
    public Iterator<String> resolveChunks(String blobId) throws IOException {
        return Lists.newArrayList(blobId).iterator();
    }

    class DataStoreIterator implements Iterator<String> {
        Iterator<DataIdentifier> backingIterator;

        public DataStoreIterator(Iterator<DataIdentifier> backingIterator) {
            this.backingIterator = backingIterator;
        }

        @Override
        public boolean hasNext() {
            return backingIterator.hasNext();
        }

        @Override
        public String next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more elements");
            }
            return backingIterator.next().toString();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
