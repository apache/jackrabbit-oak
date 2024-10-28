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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Iterators.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterators.transform;
import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.jcr.RepositoryException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.guava.common.cache.LoadingCache;
import org.apache.jackrabbit.guava.common.cache.Weigher;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.api.blob.BlobUpload;
import org.apache.jackrabbit.oak.api.blob.BlobUploadOptions;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.BlobTrackingStore;
import org.apache.jackrabbit.oak.plugins.blob.ExtendedBlobStatsCollector;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStatsCollector;
import org.apache.jackrabbit.oak.spi.blob.stats.StatsCollectingStreams;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.io.ByteStreams;
import org.apache.jackrabbit.guava.common.io.Closeables;

/**
 * BlobStore wrapper for DataStore. Wraps Jackrabbit 2 DataStore and expose them as BlobStores
 * It also handles inlining binaries if there size is smaller than
 * {@link org.apache.jackrabbit.core.data.DataStore#getMinRecordLength()}
 */
public class DataStoreBlobStore
    implements DataStore, BlobStore, GarbageCollectableBlobStore, BlobTrackingStore, TypedDataStore,
        BlobAccessProvider {
    private final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Flag to determine whether to remove repository id from DataStore on close.
     */
    private final boolean SHARED_TRANSIENT = Boolean.parseBoolean(
        System.getProperty("oak.datastore.sharedTransient"));

    protected final DataStore delegate;

    protected BlobStatsCollector stats = ExtendedBlobStatsCollector.NOOP;

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
        public int weigh(@NotNull String key, @NotNull byte[] value) {
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

    private String repositoryId;

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
        try {
            long start = System.nanoTime();

            DataRecord rec = isInMemoryRecord(identifier) ?
                    getDataRecord(identifier.toString()) :
                    delegate.getRecordIfStored(identifier);

            long elapsed = System.nanoTime() - start;
            stats.getRecordIfStoredCalled(elapsed, TimeUnit.NANOSECONDS, rec.getLength());
            stats.getRecordIfStoredCompleted(identifier.toString());

            return rec;
        }
        catch (DataStoreException e) {
            stats.getRecordIfStoredFailed(identifier.toString());
            throw e;
        }
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        try {
            long start = System.nanoTime();

            DataRecord rec = isInMemoryRecord(identifier) ?
                    getDataRecord(identifier.toString()) :
                    delegate.getRecord(identifier);

            long elapsed = System.nanoTime() - start;
            stats.getRecordCalled(elapsed, TimeUnit.NANOSECONDS, rec.getLength());
            stats.getRecordCompleted(identifier.toString());

            return rec;
        }
        catch (DataStoreException e) {
            stats.getRecordFailed(identifier.toString());
            throw e;
        }
    }

    @Override
    public DataRecord getRecordFromReference(String reference) throws DataStoreException {
        try {
            long start = System.nanoTime();

            DataRecord rec = delegate.getRecordFromReference(reference);

            long elapsed = System.nanoTime() - start;
            stats.getRecordFromReferenceCalled(elapsed, TimeUnit.NANOSECONDS, rec.getLength());
            stats.getRecordFromReferenceCompleted(reference);

            return rec;
        }
        catch (DataStoreException e) {
            stats.getRecordFromReferenceFailed(reference);
            throw e;
        }
    }

    @Override
    public DataRecord addRecord(InputStream stream) throws DataStoreException {
        try {
            long start = System.nanoTime();

            DataRecord rec = writeStream(stream, new BlobOptions());

            stats.recordAdded(System.nanoTime() - start, TimeUnit.NANOSECONDS, rec.getLength());
            stats.addRecordCompleted(rec.getIdentifier().toString());

            return rec;
        }
        catch (IOException e) {
            stats.addRecordFailed();
            throw new DataStoreException(e);
        }
        catch (DataStoreException e) {
            stats.addRecordFailed();
            throw e;
        }
    }

    @Override
    public void updateModifiedDateOnAccess(long before) {
        delegate.updateModifiedDateOnAccess(before);
    }

    @Override
    public int deleteAllOlderThan(long min) throws DataStoreException {
        try {
            long start = System.nanoTime();

            int deletedCount = delegate.deleteAllOlderThan(min);

            stats.deletedAllOlderThan(System.nanoTime() - start, TimeUnit.NANOSECONDS, min);
            stats.deleteAllOlderThanCompleted(deletedCount);

            return deletedCount;
        }
        catch (Exception e) {
            stats.deleteAllOlderThanFailed(min);
            throw e;
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        try {
            long start = System.nanoTime();

            Iterator<DataIdentifier> allIdentifiersIterator = delegate.getAllIdentifiers();

            stats.getAllIdentifiersCalled(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            stats.getAllIdentifiersCompleted();

            return allIdentifiersIterator;
        }
        catch (Exception e) {
            stats.getAllIdentifiersFailed();
            throw e;
        }
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
        // If marked as shared transient then delete the repository marker in close
        if (SHARED_TRANSIENT) {
            if (!Strings.isNullOrEmpty(getRepositoryId())) {
                deleteMetadataRecord(SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getNameFromId(getRepositoryId()));
            }
        }
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

            requireNonNull(stream);
            DataRecord dr = writeStream(stream, options);
            String id = getBlobId(dr);
            updateTracker(id);
            threw = false;

            stats.uploaded(System.nanoTime() - start, TimeUnit.NANOSECONDS, dr.getLength());
            stats.uploadCompleted(id);

            return id;
        } catch (DataStoreException e) {
            stats.uploadFailed();
            throw new IOException(e);
        } finally {
            //DataStore does not closes the stream internally
            //So close the stream explicitly
            Closeables.close(stream, threw);
        }
    }

    private void updateTracker(String id) {
        if (tracker != null && !InMemoryDataRecord.isInstance(id)) {
            try {
                tracker.add(id);
                log.trace("Tracked Id {}", id);
            }
            catch (Exception e) {
                log.warn("Could not add track id", e);
            }
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
            requireNonNull(encodedBlobId, "BlobId must be specified");
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
    public String getBlobId(@NotNull String reference) {
        requireNonNull(reference);
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
    public String getReference(@NotNull String encodedBlobId) {
        requireNonNull(encodedBlobId);
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
        try {
            return getStream(blobId.blobId);
        }
        catch (IOException e) {
            stats.downloadFailed(blobId.blobId);
            throw e;
        }
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
        return transform(filter(getAllRecords(), input -> {
                if (input != null && (maxLastModifiedTime <= 0
                        || input.getLastModified() < maxLastModifiedTime)) {
                    return true;
                }
                return false;
            }), input -> {
                if (encodeLengthInId) {
                    return BlobId.of(input).encodedValue();
                }
                return input.getIdentifier().toString();
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
            try {
                List<String> deleted = new ArrayList<>(512);
                for (String chunkId : chunkIds) {
                    long start = System.nanoTime();

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

                    stats.deleted(blobId, System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    stats.deleteCompleted(blobId);
                }
                if (!deleted.isEmpty()) {
                    log.info("Deleted blobs {}", deleted);
                }
            }
            catch (Exception e) {
                stats.deleteFailed();
                throw e;
            }
        }
        return count;
    }

    @Override
    public Iterator<String> resolveChunks(String blobId) throws IOException {
        if (!InMemoryDataRecord.isInstance(blobId)) {
            return Iterators.singletonIterator(blobId);
        }
        return Collections.emptyIterator();
    }

    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        if (delegate instanceof SharedDataStore) {
            try {
                long start = System.nanoTime();

                ((SharedDataStore) delegate).addMetadataRecord(stream, name);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).metadataRecordAdded(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).addMetadataRecordCompleted(name);
                }
            }
            catch (DataStoreException e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).addMetadataRecordFailed(name);
                }
                throw e;
            }
        }
    }

    @Override
    public void addMetadataRecord(File f, String name) throws DataStoreException {
        if (delegate instanceof SharedDataStore) {
            try {
                long start = System.nanoTime();

                ((SharedDataStore) delegate).addMetadataRecord(f, name);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).metadataRecordAdded(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).addMetadataRecordCompleted(name);
                }
            }
            catch (DataStoreException e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).addMetadataRecordFailed(name);
                }
                throw e;
            }
        }
    }

    @Override public DataRecord getMetadataRecord(String name) {
        if (delegate instanceof SharedDataStore) {
            try {
                long start = System.nanoTime();

                DataRecord record = ((SharedDataStore) delegate).getMetadataRecord(name);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).getMetadataRecordCalled(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).getMetadataRecordCompleted(name);
                }

                return record;
            }
            catch (Exception e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).getMetadataRecordFailed(name);
                }
                throw e;
            }
        }
        return null;
    }

    @Override
    public boolean metadataRecordExists(String name) {
        if (delegate instanceof SharedDataStore) {
            try {
                long start = System.nanoTime();

                boolean exists = ((SharedDataStore) delegate).metadataRecordExists(name);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).metadataRecordExistsCalled(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).metadataRecordExistsCompleted(name);
                }

                return exists;
            }
            catch (Exception e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).metadataRecordExistsFailed(name);
                }
                throw e;
            }
        }
        return false;
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        if (delegate instanceof SharedDataStore) {
            try {
                long start = System.nanoTime();

                List<DataRecord> records = ((SharedDataStore) delegate).getAllMetadataRecords(prefix);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).getAllMetadataRecordsCalled(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).getAllMetadataRecordsCompleted(prefix);
                }

                return records;
            }
            catch (Exception e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).getAllMetadataRecordsFailed(prefix);
                }
                throw e;
            }
        }
        return null;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        if (delegate instanceof SharedDataStore) {
            try {
                long start = System.nanoTime();

                boolean deleted = ((SharedDataStore) delegate).deleteMetadataRecord(name);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).metadataRecordDeleted(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).deleteMetadataRecordCompleted(name);
                }

                return deleted;
            }
            catch (Exception e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).deleteMetadataRecordFailed(name);
                }
                throw e;
            }
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        if (delegate instanceof SharedDataStore) {
            try {
                long start = System.nanoTime();

                ((SharedDataStore) delegate).deleteAllMetadataRecords(prefix);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).allMetadataRecordsDeleted(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).deleteAllMetadataRecordsCompleted(prefix);
                }
            }
            catch (Exception e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).deleteAllMetadataRecordsFailed(prefix);
                }
                throw e;
            }
        }
    }

    @Override
    public void setRepositoryId(String repositoryId) throws DataStoreException {
        this.repositoryId = repositoryId;
        addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getNameFromId(repositoryId));
        log.info("repositoryId registered in blobstore - [{}]", repositoryId);
    }

    @Override
    public String getRepositoryId() {
        return repositoryId;
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        long start = System.nanoTime();

        Iterator<DataRecord> result = delegate instanceof SharedDataStore ?
                ((SharedDataStore) delegate).getAllRecords() :
                Iterators.transform(delegate.getAllIdentifiers(),
                        input -> {
                                try {
                                    return delegate.getRecord(input);
                                } catch (DataStoreException e) {
                                    log.warn("Error occurred while fetching DataRecord for identifier {}", input, e);
                                }
                                return null;
                            });

        if (stats instanceof ExtendedBlobStatsCollector) {
            ((ExtendedBlobStatsCollector) stats).getAllRecordsCalled(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            ((ExtendedBlobStatsCollector) stats).getAllRecordsCompleted();
        }

        return result;
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
        try {
            long start = System.nanoTime();

            DataRecord record = delegate instanceof SharedDataStore ?
                    ((SharedDataStore) delegate).getRecordForId(identifier) :
                    delegate.getRecord(identifier);

            if (stats instanceof ExtendedBlobStatsCollector) {
                long elapsed = System.nanoTime() - start;
                ((ExtendedBlobStatsCollector) stats).getRecordForIdCalled(elapsed, TimeUnit.NANOSECONDS, record.getLength());
                ((ExtendedBlobStatsCollector) stats).getRecordForIdCompleted(identifier.toString());
            }

            return record;
        }
        catch (DataStoreException e) {
            if (stats instanceof ExtendedBlobStatsCollector) {
                ((ExtendedBlobStatsCollector) stats).getRecordForIdFailed(identifier.toString());
            }
            throw e;
        }
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
        try {
            long start = System.nanoTime();
            DataRecord result = addRecordInternal(input, options);
            stats.recordAdded(System.nanoTime() - start, TimeUnit.NANOSECONDS, result.getLength());
            stats.addRecordCompleted(result.getIdentifier().toString());
            return result;
        }
        catch (DataStoreException e) {
            stats.addRecordFailed();
            throw e;
        }
    }

    private DataRecord addRecordInternal(InputStream input, BlobOptions options) throws DataStoreException {
        return delegate instanceof TypedDataStore ?
                ((TypedDataStore) delegate).addRecord(input, options) :
                delegate.addRecord(input);
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
            long startTime = System.nanoTime();
            InputStream in = getDataRecord(blobId).getStream();
            if (!(in instanceof BufferedInputStream)){
                in = new BufferedInputStream(in);
            }
            return StatsCollectingStreams.wrap(stats, blobId, in, startTime);
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
        requireNonNull(id, String.format("No DataRecord found for blobId [%s]", blobId));
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
            record = addRecordInternal(in, options);
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


    // <--------------- BlobAccessProvider implementation - Direct binary access feature --------------->

    @Nullable
    @Override
    public BlobUpload initiateBlobUpload(long maxUploadSizeInBytes, int maxNumberOfURIs)
            throws IllegalArgumentException {
        return initiateBlobUpload(maxUploadSizeInBytes, maxNumberOfURIs, BlobUploadOptions.DEFAULT);
    }

    @Nullable
    @Override
    public BlobUpload initiateBlobUpload(long maxUploadSizeInBytes, int maxNumberOfURIs, @NotNull final BlobUploadOptions options)
            throws IllegalArgumentException {
        if (delegate instanceof DataRecordAccessProvider) {
            try {
                long start = System.nanoTime();

                DataRecordAccessProvider provider = (DataRecordAccessProvider) this.delegate;

                DataRecordUpload upload = provider.initiateDataRecordUpload(maxUploadSizeInBytes, maxNumberOfURIs,
                        DataRecordUploadOptions.fromBlobUploadOptions(options));

                if (upload == null) {
                    if (stats instanceof ExtendedBlobStatsCollector) {
                        ((ExtendedBlobStatsCollector) stats).initiateBlobUploadFailed();
                    }
                    return null;
                }

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).initiateBlobUpload(System.nanoTime() - start, TimeUnit.NANOSECONDS, maxUploadSizeInBytes, maxNumberOfURIs);
                    ((ExtendedBlobStatsCollector) stats).initiateBlobUploadCompleted();
                }

                return new BlobUpload() {
                    @Override
                    @NotNull
                    public String getUploadToken() {
                        return upload.getUploadToken();
                    }

                    @Override
                    public long getMinPartSize() {
                        return upload.getMinPartSize();
                    }

                    @Override
                    public long getMaxPartSize() {
                        return upload.getMaxPartSize();
                    }

                    @Override
                    @NotNull
                    public Collection<URI> getUploadURIs() {
                        return upload.getUploadURIs();
                    }
                };
            }
            catch (DataRecordUploadException e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).initiateBlobUploadFailed();
                }
                log.warn("Unable to initiate direct upload", e);
            }
            catch (IllegalArgumentException e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).initiateBlobUploadFailed();
                }
                throw e;
            }
        }
        else if (stats instanceof ExtendedBlobStatsCollector) {
            ((ExtendedBlobStatsCollector) stats).initiateBlobUploadFailed();
        }
        return null;
    }

    @Nullable
    @Override
    public Blob completeBlobUpload(@NotNull String uploadToken) throws IllegalArgumentException {
        if (delegate instanceof DataRecordAccessProvider) {
            try {
                long start = System.nanoTime();

                DataRecord record = ((DataRecordAccessProvider) delegate).completeDataRecordUpload(uploadToken);
                String id = getBlobId(record);
                updateTracker(id);

                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).completeBlobUpload(System.nanoTime() - start, TimeUnit.NANOSECONDS);
                    ((ExtendedBlobStatsCollector) stats).completeBlobUploadCompleted(id);
                }

                return new BlobStoreBlob(this, id);
            }
            catch (DataStoreException | DataRecordUploadException e) {
                log.warn("Unable to complete direct upload for upload token {}", uploadToken, e);
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).completeBlobUploadFailed();
                }
            }
            catch (IllegalArgumentException e) {
                if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).completeBlobUploadFailed();
                }
                throw e;
            }
        }
        else if (stats instanceof ExtendedBlobStatsCollector) {
            ((ExtendedBlobStatsCollector) stats).completeBlobUploadFailed();
        }
        return null;
    }

    @Nullable
    @Override
    public URI getDownloadURI(@NotNull Blob blob, @NotNull BlobDownloadOptions downloadOptions) {
        if (delegate instanceof DataRecordAccessProvider) {
            long start = System.nanoTime();

            String blobId = blob.getContentIdentity();
            if (blobId != null) {
                String extractedBlobId = extractBlobId(blobId);
                URI uri = ((DataRecordAccessProvider) delegate).getDownloadURI(
                        new DataIdentifier(extractedBlobId),
                        DataRecordDownloadOptions.fromBlobDownloadOptions(downloadOptions)
                );

                if (null != uri) {
                    if (stats instanceof ExtendedBlobStatsCollector) {
                        ((ExtendedBlobStatsCollector) stats).getDownloadURICalled(System.nanoTime() - start, TimeUnit.NANOSECONDS, extractedBlobId);
                        ((ExtendedBlobStatsCollector) stats).getDownloadURICompleted(uri.toString());
                    }
                }
                else if (stats instanceof ExtendedBlobStatsCollector) {
                    ((ExtendedBlobStatsCollector) stats).getDownloadURIFailed();
                }

                return uri;
            }
        }
        else if (stats instanceof ExtendedBlobStatsCollector) {
            ((ExtendedBlobStatsCollector) stats).getDownloadURIFailed();
        }
        return null;
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
