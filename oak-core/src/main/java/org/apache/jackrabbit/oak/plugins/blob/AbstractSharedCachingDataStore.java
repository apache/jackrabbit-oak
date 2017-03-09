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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.io.Closeables;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.AbstractDataStore;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TypedDataStore;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.util.TransientFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.spi.blob.BlobOptions.UploadType.SYNCHRONOUS;

/**
 * Cache files locally and stage files locally for async uploads.
 * Configuration:
 *
 * <pre>
 * &lt;DataStore class="org.apache.jackrabbit.oak.plugins.blob.AbstractCachingDataStore"&gt;
 *     &lt;param name="{@link #setPath(String) path}"/&gt;
 *     &lt;param name="{@link #setCacheSize(long) cacheSize}" value="68719476736"/&gt;
 *     &lt;param name="{@link #setStagingSplitPercentage(int) stagingSplitPercentage}" value="10"/&gt;
 *     &lt;param name="{@link #setUploadThreads(int) uploadThreads}" value="10"/&gt;
 *     &lt;param name="{@link #setStagingPurgeInterval(int) stagingPurgeInterval}" value="300"/&gt;
 *     &lt;param name="{@link #setStagingRetryInterval(int) stagingRetryInterval} " value="600"/&gt;
 * &lt;/DataStore&gt;
 * </pre>
 */
public abstract class AbstractSharedCachingDataStore extends AbstractDataStore
    implements MultiDataStoreAware, SharedDataStore, TypedDataStore {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSharedCachingDataStore.class);

    /**
     * The root path
     */
    private String path;

    /**
     * The number of bytes in the cache. The default value is 64 GB.
     */
    private long cacheSize = 64L * 1024 * 1024 * 1024;

    /**
     * The % of cache utilized for upload staging.
     */
    private int stagingSplitPercentage = 10;

    /**
     * The number of upload threads used for asynchronous uploads from staging.
     */
    private int uploadThreads = 10;

    /**
     * The interval for remove job in seconds.
     */
    private int stagingPurgeInterval = 300;

    /**
     * The interval for retry job in seconds.
     */
    private int stagingRetryInterval = 600;

    /**
     * The root rootDirectory where the files are created.
     */
    private File rootDirectory;

    /**
     * The rootDirectory where tmp files are created.
     */
    private File tmp;

    /**
     * Statistics provider.
     */
    private StatisticsProvider statisticsProvider;

    /**
     * DataStore cache
     */
    private CompositeDataStoreCache cache;

    /**
     * The delegate backend
     */
    protected AbstractSharedBackend backend;

    protected ListeningExecutorService listeningExecutor;

    protected ScheduledExecutorService schedulerExecutor;

    protected ExecutorService executor;

    public void init(String homeDir) throws DataStoreException {
        if (path == null) {
            path = homeDir + "/repository/datastore";
        }
        path = FilenameUtils.normalizeNoEndSeparator(new File(path).getAbsolutePath());
        checkArgument(stagingSplitPercentage >= 0 && stagingSplitPercentage <= 50,
            "Staging percentage cache should be between 0 and 50");

        this.rootDirectory = new File(path);
        this.tmp = new File(rootDirectory, "tmp");
        LOG.trace("Temporary file created [{}]", tmp.mkdirs());

        this.backend = createBackend();
        backend.init();

        String home = FilenameUtils.normalizeNoEndSeparator(new File(homeDir).getAbsolutePath());
        this.cache =
            new CompositeDataStoreCache(path, new File(home), cacheSize, stagingSplitPercentage,
                uploadThreads,
                new CacheLoader<String, InputStream>() {
                    @Override public InputStream load(String key) throws Exception {
                        InputStream is = null;
                        boolean threw = true;
                        try {
                            is = backend.read(new DataIdentifier(key));
                            threw = false;
                        } finally {
                            Closeables.close(is, threw);
                        }
                        return is;
                    }
                }, new StagingUploader() {
                    @Override public void write(String id, File file) throws DataStoreException {
                        backend.write(new DataIdentifier(id), file);
                    }
            }, statisticsProvider, listeningExecutor, schedulerExecutor, executor, stagingPurgeInterval,
                stagingRetryInterval);
    }

    protected abstract AbstractSharedBackend createBackend();

    @Override
    public DataRecord getRecord(DataIdentifier identifier)
        throws DataStoreException {
        DataRecord record = getRecordIfStored(identifier);
        if (record != null) {
            return record;
        } else {
            throw new DataStoreException(
                "Record " + identifier + " does not exist");
        }
    }

    @Override
    @Nullable
    public DataRecord getRecordIfStored(DataIdentifier dataIdentifier)
        throws DataStoreException {
        // Return file attributes from cache only if corresponding file is cached
        // This avoids downloading the file for just accessing the meta data
        File cached = cache.getIfPresent(dataIdentifier.toString());
        if (cached != null && cached.exists()) {
            return new FileCacheDataRecord(this, backend, dataIdentifier, cached.length(),
                cached.lastModified());
        }

        // File not in cache so, retrieve the meta data from the backend explicitly
        try {
            return backend.getRecord(dataIdentifier);
        } catch (Exception e) {
            LOG.error("Error retrieving record [{}] from backend", dataIdentifier, e);
        }
        return null;
    }

    @Override
    public DataRecord addRecord(InputStream inputStream) throws DataStoreException {
        return addRecord(inputStream, new BlobOptions());
    }

    @Override
    public DataRecord addRecord(InputStream inputStream, BlobOptions blobOptions)
        throws DataStoreException {
        Stopwatch watch = Stopwatch.createStarted();
        try {
            TransientFileFactory fileFactory = TransientFileFactory.getInstance();
            File tmpFile = fileFactory.createTransientFile("upload", null, tmp);

            // Copy the stream to the temporary file and calculate the
            // stream length and the message digest of the stream
            MessageDigest digest = MessageDigest.getInstance(DIGEST);
            OutputStream output = new DigestOutputStream(new FileOutputStream(tmpFile), digest);
            long length = 0;
            try {
                length = IOUtils.copyLarge(inputStream, output);
            } finally {
                output.close();
            }

            DataIdentifier identifier = new DataIdentifier(encodeHexString(digest.digest()));
            LOG.debug("SHA-256 of [{}], length =[{}] took [{}] ms ", identifier, length,
                watch.elapsed(TimeUnit.MILLISECONDS));

            // asynchronously stage for upload if the size limit of staging cache permits
            // otherwise add to backend
            if (blobOptions.getUpload() == SYNCHRONOUS
                || !cache.stage(identifier.toString(), tmpFile)) {
                backend.write(identifier, tmpFile);
                LOG.info("Added blob [{}] to backend", identifier);
                // offer to download cache
                cache.getDownloadCache().put(identifier.toString(), tmpFile);
            }

            return getRecordIfStored(identifier);
        } catch (Exception e) {
            LOG.error("Error in adding record");
            throw new DataStoreException("Error in adding record ", e);
        }
    }

    /**
     * In rare cases may include some duplicates in cases where async staged uploads complete
     * during iteration.
     *
     * @return Iterator over all ids available
     * @throws DataStoreException
     */
    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return Iterators.concat(Iterators.transform(cache.getStagingCache().getAllIdentifiers(),
            new Function<String, DataIdentifier>() {
                @Nullable @Override public DataIdentifier apply(@Nullable String id) {
                    return new DataIdentifier(id);
                }
            }), backend.getAllIdentifiers());
    }

    @Override
    public void deleteRecord(DataIdentifier dataIdentifier) throws DataStoreException {
        cache.invalidate(dataIdentifier.toString());
        backend.deleteRecord(dataIdentifier);
    }

    @Override
    public void close() throws DataStoreException {
        backend.close();
        cache.close();
    }

    /**
     * DataRecord implementation fetching the stream from the cache.
     */
    static class FileCacheDataRecord extends AbstractDataRecord {
        private final long length;
        private final long lastModified;
        private final AbstractSharedCachingDataStore store;

        public FileCacheDataRecord(AbstractSharedCachingDataStore store, AbstractSharedBackend backend,
            DataIdentifier identifier, long length,
            long lastModified) {
            super(backend, identifier);
            this.length = length;
            this.lastModified = lastModified;
            this.store = store;
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            try {
                return new FileInputStream(store.cache.get(getIdentifier().toString()));
            } catch (final Exception e) {
                throw new DataStoreException(
                    "Error opening input stream for identifier " + getIdentifier(), e);
            }
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }
    }

    /**
     * Look in the backend for a record matching the given identifier.  Returns true
     * if such a record exists.
     *
     * @param identifier - An identifier for the record.
     * @return true if a record for the provided identifier can be found.
     */
    public boolean exists(final DataIdentifier identifier) {
        try {
            if (identifier != null) {
                return backend.exists(identifier);
            }
        }
        catch (DataStoreException e) {
            LOG.warn(String.format("Data Store Exception caught checking for %s in pending uploads",
                identifier), e);
        }
        return false;
    }

    public List<DataStoreCacheStatsMBean> getStats() {
        return ImmutableList.of(cache.getCacheStats(), cache.getStagingCacheStats());
    }

    protected CompositeDataStoreCache getCache() {
        return cache;
    }

    /**------------------------- setters ----------------------------------------------**/

    public void setPath(String path) {
        this.path = path;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public void setStagingSplitPercentage(int stagingSplitPercentage) {
        this.stagingSplitPercentage = stagingSplitPercentage;
    }

    public void setUploadThreads(int uploadThreads) {
        this.uploadThreads = uploadThreads;
    }

    public void setStagingPurgeInterval(int stagingPurgeInterval) {
        this.stagingPurgeInterval = stagingPurgeInterval;
    }

    public void setStagingRetryInterval(int stagingRetryInterval) {
        this.stagingRetryInterval = stagingRetryInterval;
    }

    public void setStatisticsProvider(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
    }

    /**------------------------ SharedDataStore methods -----------------------------------------**/

    @Override
    public void addMetadataRecord(InputStream stream, String name) throws DataStoreException {
        backend.addMetadataRecord(stream, name);
    }

    @Override
    public void addMetadataRecord(File f, String name) throws DataStoreException {
        backend.addMetadataRecord(f, name);
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        return backend.getMetadataRecord(name);
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        return backend.getAllMetadataRecords(prefix);
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        return backend.deleteMetadataRecord(name);
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        backend.deleteAllMetadataRecords(prefix);
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        return backend.getAllRecords();
    }

    @Override
    public DataRecord getRecordForId(DataIdentifier identifier) throws DataStoreException {
        return backend.getRecord(identifier);
    }

    @Override
    public SharedDataStore.Type getType() {
        return SharedDataStore.Type.SHARED;
    }

    @Override
    protected byte[] getOrCreateReferenceKey() throws DataStoreException {
        return backend.getOrCreateReferenceKey();
    }

    /**------------------------ unimplemented methods -------------------------------------------**/

    @Override
    public void clearInUse() {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public void updateModifiedDateOnAccess(long l) {
        throw new UnsupportedOperationException("Operation not supported");
    }

    @Override
    public int deleteAllOlderThan(long l) throws DataStoreException {
        throw new UnsupportedOperationException("Operation not supported");
    }
}
