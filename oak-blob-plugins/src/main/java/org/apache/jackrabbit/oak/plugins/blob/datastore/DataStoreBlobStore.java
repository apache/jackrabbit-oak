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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;
import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.stats.StatsCollectingStreams;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BlobStore wrapper for DataStore. Wraps Jackrabbit 2 DataStore and expose them as BlobStores
 * It also handles inlining binaries if there size is smaller than
 * {@link org.apache.jackrabbit.core.data.DataStore#getMinRecordLength()}
 */
public class DataStoreBlobStore
    implements DataStore, BlobStore, GarbageCollectableBlobStore, BlobTrackingStore, TypedDataStore {
    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final DataStore delegate;

    protected BlobStatsCollector stats = BlobStatsCollector.NOOP;

    private BlobTracker tracker;

    /**
     * If set to true then the blob length information would be encoded as part of blobId
     * and thus no extra call would be made to DataStore to determine the length
     *
     * <b>Implementation Note</b>If enabled the length would be encoded in blobid by appending it at the end.
     * This would be done for the methods which are part of BlobStore and GarbageCollectableBlobStore interface
     *
     * DataIdentifiers which are part of DataStore would not be affected by this as DataStore interface
     * is not used in Oak and all access is via BlobStore interface
     */
    private final boolean encodeLengthInId;

    protected final LoadingCache<String, byte[]> cache;

    public static final int DEFAULT_CACHE_SIZE = 16;

    /**
     * Max size of binary whose content would be cached. We keep it greater than
     * Lucene blob size OakDirectory#BLOB_SIZE such that Lucene index blobs are cached
     */
    private int maxCachedBinarySize = 1024 * 1024;

    private final Weigher<String, byte[]> weigher = new Weigher<String, byte[]>() {
        @Override
        public int weigh(@Nonnull String key, @Nonnull byte[] value) {
            long weight = (long)StringUtils.estimateMemoryUsage(key) + value.length;
            if (weight > Integer.MAX_VALUE) {
                log.debug("Calculated weight larger than Integer.MAX_VALUE: {}.", weight);
                weight = Integer.MAX_VALUE;
            }
            return (int) weight;
        }
    };

    private final CacheStats cacheStats;

    public static final String MEM_CACHE_NAME = "BlobStore-MemCache";

    public DataStoreBlobStore(DataStore delegate) {
        this(delegate, true, DEFAULT_CACHE_SIZE);
    }

    public DataStoreBlobStore(DataStore delegate, boolean encodeLengthInId) {
        this(delegate, encodeLengthInId, DEFAULT_CACHE_SIZE);
    }

    public DataStoreBlobStore(DataStore delegate, boolean encodeLengthInId, int cacheSizeInMB) {
        this.delegate = delegate;
        this.encodeLengthInId = encodeLengthInId;

        long cacheSize = (long) cacheSizeInMB * FileUtils.ONE_MB;
        this.cache = CacheLIRS.<String, byte[]>newBuilder()
                .module(MEM_CACHE_NAME)
                .recordStats()
                .maximumWeight(cacheSize)
                .weigher(weigher)
                .build();
        this.cacheStats = new CacheStats(cache, MEM_CACHE_NAME, weigher, cacheSize);
    }

    //~----------------------------------< DataStore >

    @Override
    public DataRecord getRecordIfStored(DataIdentifier identifier) throws DataStoreException {
        if (isInMemoryRecord(identifier)) {
            return getDataRecord(identifier.toString());
        }
        return delegate.getRecordIfStored(identifier);
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        if (isInMemoryRecord(identifier)) {
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
            return writeStream(stream, new BlobOptions());
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
        cache.invalidateAll();
        closeQuietly(tracker);
    }

    //~-------------------------------------------< BlobStore >

    @Override
    public String writeBlob(InputStream stream) throws IOException {
        return writeBlob(stream, new BlobOptions());
    }

    @Override
    public String writeBlob(InputStream stream, BlobOptions options) throws IOException {
        boolean threw = true;
        try {
            long start = System.nanoTime();
            checkNotNull(stream);
            DataRecord dr = writeStream(stream, options);
            String id = getBlobId(dr);
            if (tracker != null && !InMemoryDataRecord.isInstance(id)) {
                try {
                    tracker.add(id);
                    log.trace("Tracked Id {}", id);
                } catch (Exception e) {
                    log.warn("Could not add track id", e);
                }
            }
            threw = false;
            stats.uploaded(System.nanoTime() - start, TimeUnit.NANOSECONDS, dr.getLength());
            stats.uploadCompleted(id);
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
    public int readBlob(String encodedBlobId, long pos, byte[] buff, int off, int length) throws IOException {
        //This is inefficient as repeated calls for same blobId would involve opening new Stream
        //instead clients should directly access the stream from DataRecord by special casing for
        //BlobStore which implements DataStore
        InputStream stream = getInputStream(encodedBlobId);
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
    public long getBlobLength(String encodedBlobId) throws IOException {
        try {
            checkNotNull(encodedBlobId, "BlobId must be specified");
            BlobId id = BlobId.of(encodedBlobId);
            if (encodeLengthInId && id.hasLengthInfo()) {
                return id.length;
            }
            return getDataRecord(id.blobId).getLength();
        } catch (DataStoreException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getBlobId(@Nonnull String reference) {
        checkNotNull(reference);
        DataRecord record;
        try {
            record = delegate.getRecordFromReference(reference);
            if (record != null) {
                return getBlobId(record);
            }
        } catch (DataStoreException e) {
            log.warn("Unable to access the blobId for  [{}]", reference, e);
        }
        return null;
    }

    @Override
    public String getReference(@Nonnull String encodedBlobId) {
        checkNotNull(encodedBlobId);
        String blobId = extractBlobId(encodedBlobId);
        //Reference are not created for in memory record
        if (InMemoryDataRecord.isInstance(blobId)) {
            return null;
        }

        DataRecord record;
        try {
            record = delegate.getRecordIfStored(new DataIdentifier(blobId));
            if (record != null) {
                return record.getReference();
            } else {
                log.debug("No blob found for id [{}]", blobId);
            }
        } catch (DataStoreException e) {
            log.warn("Unable to access the blobId for  [{}]", blobId, e);
        }
        return  null;
    }

    @Override
    public InputStream getInputStream(final String encodedBlobId) throws IOException {
        final BlobId blobId = BlobId.of(encodedBlobId);
        if (encodeLengthInId
                && blobId.hasLengthInfo()
                && blobId.length <= maxCachedBinarySize) {
            try {
                byte[] content = cache.get(blobId.blobId, new Callable<byte[]>() {
                    @Override
                    public byte[] call() throws Exception {
                        boolean threw = true;
                        InputStream stream = getStream(blobId.blobId);
                        try {
                            byte[] result = IOUtils.toByteArray(stream);
                            threw = false;
                            return result;
                        } finally {
                            Closeables.close(stream, threw);
                        }
                    }
                });
                return new ByteArrayInputStream(content);
            } catch (ExecutionException e) {
                log.warn("Error occurred while loading bytes from steam while fetching for id {}", encodedBlobId, e);
            }
        }
        return getStream(blobId.blobId);
    }

    //~-------------------------------------------< GarbageCollectableBlobStore >

    @Override
    public void setBlockSize(int x) {
        // nothing to do
    }

    @Override
    public String writeBlob(String tempFileName) throws IOException {
        File file = new File(tempFileName);
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            return writeBlob(in);
        } finally {
            closeQuietly(in);
            FileUtils.forceDelete(file);
        }
    }

    @Override
    public int sweep() throws IOException {
        return 0;
    }

    @Override
    public void startMark() throws IOException {
        // nothing to do
    }

    @Override
    public void clearInUse() {
        delegate.clearInUse();
    }

    @Override
    public void clearCache() {
        // nothing to do
    }

    @Override
    public long getBlockSizeMin() {
        return 0;
    }

    @Override
    public Iterator<String> getAllChunkIds(final long maxLastModifiedTime) throws Exception {
        return transform(filter(getAllRecords(), new Predicate<DataRecord>() {
            @Override
            public boolean apply(@Nullable DataRecord input) {
                if (input != null && (maxLastModifiedTime <= 0
                        || input.getLastModified() < maxLastModifiedTime)) {
                    return true;
                }
                return false;
            }
        }), new Function<DataRecord, String>() {
            @Override
            public String apply(DataRecord input) {
                if (encodeLengthInId) {
                    return BlobId.of(input).encodedValue();
                }
                return input.getIdentifier().toString();
            }
        });
    }

    @Override
    public boolean deleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        return (chunkIds.size() == countDeleteChunks(chunkIds, maxLastModifiedTime));
    }    
    
    @Override
    public long countDeleteChunks(List<String> chunkIds, long maxLastModifiedTime) throws Exception {
        int count = 0;
        if (delegate instanceof MultiDataStoreAware) {
            List<String> deleted = Lists.newArrayListWithExpectedSize(512);
            for (String chunkId : chunkIds) {
                String blobId = extractBlobId(chunkId);
                DataIdentifier identifier = new DataIdentifier(blobId);
                DataRecord dataRecord = getRecordForId(identifier);
                boolean success = (maxLastModifiedTime <= 0)
                        || dataRecord.getLastModified() <= maxLastModifiedTime;
                log.trace("Deleting blob [{}] with last modified date [{}] : [{}]", blobId,
                    dataRecord.getLastModified(), success);
                if (success) {
                    ((MultiDataStoreAware) delegate).deleteRecord(identifier);
                    deleted.add(blobId);
                    count++;
                    if (count % 512 == 0) {
                        log.info("Deleted blobs {}", deleted);
                        deleted.clear();
                    }
                }
            }
            if (!deleted.isEmpty()) {
                log.info("Deleted blobs {}", deleted);
            }
        }
        return count;
    }

    @Override
    public Iterator<String> resolveChunks(String blobId) throws IOException {
        if (!InMemoryDataRecord.isInstance(blobId)) {
            return Iterators.singletonIterator(blobId);
        }
        return Iterators.emptyIterator();
    }

    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        if (delegate instanceof SharedDataStore) {
            ((SharedDataStore) delegate).addMetadataRecord(stream, name);
        }
    }

    @Override
    public void addMetadataRecord(File f, String name) throws DataStoreException {
        if (delegate instanceof SharedDataStore) {
            ((SharedDataStore) delegate).addMetadataRecord(f, name);
        }
    }

    @Override public DataRecord getMetadataRecord(String name) {
        if (delegate instanceof SharedDataStore) {
            return ((SharedDataStore) delegate).getMetadataRecord(name);
        }
        return null;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        if (delegate instanceof SharedDataStore) {
            return ((SharedDataStore) delegate).getAllMetadataRecords(prefix);
        }
        return null;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        return delegate instanceof SharedDataStore && ((SharedDataStore) delegate).deleteMetadataRecord(name);
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        if (delegate instanceof SharedDataStore) {
            ((SharedDataStore) delegate).deleteAllMetadataRecords(prefix);
        }
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        if (delegate instanceof SharedDataStore) {
            return ((SharedDataStore) delegate).getAllRecords();
        } else {
            return Iterators.transform(delegate.getAllIdentifiers(),
                new Function<DataIdentifier, DataRecord>() {
                    @Nullable @Override
                    public DataRecord apply(@Nullable DataIdentifier input) {
                        try {
                            return delegate.getRecord(input);
                        } catch (DataStoreException e) {
                            log.warn("Error occurred while fetching DataRecord for identifier {}", input, e);
                        }
                        return null;
                    }
            });
        }
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
        if (delegate instanceof SharedDataStore) {
            return ((SharedDataStore) delegate).getRecordForId(identifier);
        }
        return delegate.getRecord(identifier);
    }

    @Override
    public Type getType() {
        if (delegate instanceof SharedDataStore) {
            return Type.SHARED;
        }
        return Type.DEFAULT;
    }


    @Override
    public DataRecord addRecord(InputStream input, BlobOptions options) throws DataStoreException {
        if (delegate instanceof TypedDataStore) {
            return ((TypedDataStore) delegate).addRecord(input, options);
        }
        return delegate.addRecord(input);
    }

    //~---------------------------------------------< Object >

    @Override
    public String toString() {
        return String.format("DataStore backed BlobStore [%s]", delegate.getClass().getName());
    }

    //~---------------------------------------------< Properties >

    public DataStore getDataStore() {
        return delegate;
    }

    public CacheStats getCacheStats() {
        return cacheStats;
    }

    public void setMaxCachedBinarySize(int maxCachedBinarySize) {
        this.maxCachedBinarySize = maxCachedBinarySize;
    }

    public void setBlobStatsCollector(BlobStatsCollector stats) {
        this.stats = stats;
    }


    @Override
    public void addTracker(BlobTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    @Nullable
    public BlobTracker getTracker() {
        return tracker;
    }


    //~---------------------------------------------< Internal >

    protected InputStream getStream(String blobId) throws IOException {
        try {
            InputStream in = getDataRecord(blobId).getStream();
            if (!(in instanceof BufferedInputStream)){
                in = new BufferedInputStream(in);
            }
            return StatsCollectingStreams.wrap(stats, blobId, in);
        } catch (DataStoreException e) {
            throw new IOException(e);
        }
    }

    protected DataRecord getDataRecord(String blobId) throws DataStoreException {
        DataRecord id;
        if (InMemoryDataRecord.isInstance(blobId)) {
            id = InMemoryDataRecord.getInstance(blobId);
        } else {
            id = delegate.getRecord(new DataIdentifier(blobId));
        }
        checkNotNull(id, "No DataRecord found for blobId [%s]", blobId);
        return id;
    }

    private static boolean isInMemoryRecord(DataIdentifier identifier) {
        return InMemoryDataRecord.isInstance(identifier.toString());
    }

    /**
     * Create a BLOB value from in input stream. Small objects will create an in-memory object,
     * while large objects are stored in the data store
     *
     * @param in the input stream
     * @param options
     * @return the value
     */
    private DataRecord writeStream(InputStream in, BlobOptions options) throws IOException, DataStoreException {
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
            record = addRecord(in, options);
        }
        return record;
    }

    private String getBlobId(DataRecord dr) {
        if (encodeLengthInId) {
            return BlobId.of(dr).encodedValue();
        }
        return dr.getIdentifier().toString();
    }

    protected String extractBlobId(String encodedBlobId) {
        if (encodeLengthInId) {
            return BlobId.of(encodedBlobId).blobId;
        }
        return encodedBlobId;
    }

    public static class BlobId {
        static final String SEP = "#";

        public String getBlobId() {
            return blobId;
        }

        public long getLength() {
            return length;
        }

        final String blobId;
        final long length;

        BlobId(String blobId, long length) {
            this.blobId = blobId;
            this.length = length;
        }

        BlobId(DataRecord dr) {
            this.blobId = dr.getIdentifier().toString();
            long len;
            try {
                len = dr.getLength();
            } catch (DataStoreException e) {
                //Cannot determine length
                len = -1;
            }
            this.length = len;
        }

        BlobId(String encodedBlobId) {
            int indexOfSep = encodedBlobId.lastIndexOf(SEP);
            if (indexOfSep != -1) {
                this.blobId = encodedBlobId.substring(0, indexOfSep);
                this.length = Long.valueOf(encodedBlobId.substring(indexOfSep+SEP.length()));
            } else {
                this.blobId = encodedBlobId;
                this.length = -1;
            }
        }

        String encodedValue() {
            if (hasLengthInfo()) {
                return blobId + SEP + String.valueOf(length);
            } else {
                return blobId;
            }
        }

        boolean hasLengthInfo() {
            return length != -1;
        }

        static boolean isEncoded(String encodedBlobId) {
            return encodedBlobId.contains(SEP);
        }

        public static BlobId of(String encodedValue) {
            return new BlobId(encodedValue);
        }

        static BlobId of(DataRecord dr) {
            return new BlobId(dr);
        }
    }
}
