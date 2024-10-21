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
package org.apache.jackrabbit.oak.segment.file;

import static java.util.Objects.requireNonNull;
import static java.lang.Boolean.getBoolean;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_STRING_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_TEMPLATE_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener.LOG_SNFE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_NODE_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_STRING_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_TEMPLATE_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.CacheWeights.NodeCacheWeigher;
import org.apache.jackrabbit.oak.segment.CacheWeights.StringCacheWeigher;
import org.apache.jackrabbit.oak.segment.CacheWeights.TemplateCacheWeigher;
import org.apache.jackrabbit.oak.segment.RecordCache;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.*;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.iotrace.IOTraceLogWriter;
import org.apache.jackrabbit.oak.segment.tool.iotrace.IOTraceMonitor;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.LoggingGCMonitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for creating {@link FileStore} instances.
 */
public class FileStoreBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

    private static final boolean MEMORY_MAPPING_DEFAULT =
            "64".equals(System.getProperty("sun.arch.data.model", "32"));

    public static final int DEFAULT_MAX_FILE_SIZE = 256;

    @NotNull
    private final File directory;

    @Nullable
    private BlobStore blobStore;   // null ->  store blobs inline

    private int maxFileSize = DEFAULT_MAX_FILE_SIZE;

    private int segmentCacheSize = DEFAULT_SEGMENT_CACHE_MB;

    private int stringCacheSize = DEFAULT_STRING_CACHE_MB;

    private int templateCacheSize = DEFAULT_TEMPLATE_CACHE_MB;

    private int stringDeduplicationCacheSize = DEFAULT_STRING_CACHE_SIZE;

    private int templateDeduplicationCacheSize = DEFAULT_TEMPLATE_CACHE_SIZE;

    private int nodeDeduplicationCacheSize = DEFAULT_NODE_CACHE_SIZE;

    private boolean memoryMapping = MEMORY_MAPPING_DEFAULT;

    private boolean offHeapAccess = getBoolean("access.off.heap");
    
    private int binariesInlineThreshold = Segment.MEDIUM_LIMIT;

    private SegmentNodeStorePersistence persistence;

    @NotNull
    private StatisticsProvider statsProvider = StatisticsProvider.NOOP;

    @NotNull
    private SegmentGCOptions gcOptions = defaultGCOptions();

    @Nullable
    private EvictingWriteCacheManager cacheManager;

    private class FileStoreGCListener extends DelegatingGCMonitor implements GCListener {
        @Override
        public void compactionSucceeded(@NotNull GCGeneration newGeneration) {
            compacted();
            if (cacheManager != null) {
                cacheManager.evictOldGeneration(newGeneration.getGeneration());
            }
        }

        @Override
        public void compactionFailed(@NotNull GCGeneration failedGeneration) {
            if (cacheManager != null) {
                cacheManager.evictGeneration(failedGeneration.getGeneration());
            }
        }
    }

    @NotNull
    private final FileStoreGCListener gcListener = new FileStoreGCListener();

    @NotNull
    private SegmentNotFoundExceptionListener snfeListener = LOG_SNFE;

    @NotNull
    private final Set<IOMonitor> ioMonitors = new HashSet<>();

    @NotNull
    private RemoteStoreMonitor remoteStoreMonitor;

    private boolean strictVersionCheck;

    private boolean eagerSegmentCaching;

    private boolean built;

    /**
     * Create a new instance of a {@code FileStoreBuilder} for a file store.
     *
     * @param directory directory where the tar files are stored
     * @return a new {@code FileStoreBuilder} instance.
     */
    @NotNull
    public static FileStoreBuilder fileStoreBuilder(@NotNull File directory) {
        return new FileStoreBuilder(directory);
    }

    private FileStoreBuilder(@NotNull File directory) {
        this.directory = requireNonNull(directory);
        this.gcListener.registerGCMonitor(new LoggingGCMonitor(LOG));
        this.persistence = new TarPersistence(directory);
    }

    /**
     * Specify the {@link BlobStore}.
     *
     * @param blobStore
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withBlobStore(@NotNull BlobStore blobStore) {
        this.blobStore = requireNonNull(blobStore);
        return this;
    }

    /**
     * Maximal size of the generated tar files in MB.
     *
     * @param maxFileSize
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
        return this;
    }

    /**
     * Size of the segment cache in MB.
     *
     * @param segmentCacheSize None negative cache size
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withSegmentCacheSize(int segmentCacheSize) {
        this.segmentCacheSize = segmentCacheSize;
        return this;
    }

    /**
     * Size of the string cache in MB.
     *
     * @param stringCacheSize None negative cache size
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withStringCacheSize(int stringCacheSize) {
        this.stringCacheSize = stringCacheSize;
        return this;
    }

    /**
     * Size of the template cache in MB.
     *
     * @param templateCacheSize None negative cache size
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withTemplateCacheSize(int templateCacheSize) {
        this.templateCacheSize = templateCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the string deduplication cache
     *
     * @param stringDeduplicationCacheSize None negative cache size
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withStringDeduplicationCacheSize(int stringDeduplicationCacheSize) {
        this.stringDeduplicationCacheSize = stringDeduplicationCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the template deduplication cache
     *
     * @param templateDeduplicationCacheSize None negative cache size
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withTemplateDeduplicationCacheSize(int templateDeduplicationCacheSize) {
        this.templateDeduplicationCacheSize = templateDeduplicationCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the node deduplication cache
     *
     * @param nodeDeduplicationCacheSize None negative cache size. Must be a power of 2.
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withNodeDeduplicationCacheSize(int nodeDeduplicationCacheSize) {
        this.nodeDeduplicationCacheSize = nodeDeduplicationCacheSize;
        return this;
    }

    /**
     * Turn memory mapping on or off
     *
     * @param memoryMapping
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withMemoryMapping(boolean memoryMapping) {
        this.memoryMapping = memoryMapping;
        return this;
    }

    /**
     * Turn off heap access on or off
     *
     * @param offHeapAccess
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withOffHeapAccess(boolean offHeapAccess) {
        this.offHeapAccess = offHeapAccess;
        return this;
    }

    /**
     * Set memory mapping to the default value based on OS properties
     *
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withDefaultMemoryMapping() {
        this.memoryMapping = MEMORY_MAPPING_DEFAULT;
        return this;
    }

    /**
     * {@link GCMonitor} for monitoring this files store's gc process.
     *
     * @param gcMonitor
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withGCMonitor(@NotNull GCMonitor gcMonitor) {
        this.gcListener.registerGCMonitor(requireNonNull(gcMonitor));
        return this;
    }

    /**
     * {@link StatisticsProvider} for collecting statistics related to FileStore
     *
     * @param statisticsProvider
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withStatisticsProvider(@NotNull StatisticsProvider statisticsProvider) {
        this.statsProvider = requireNonNull(statisticsProvider);
        return this;
    }

    /**
     * {@link SegmentGCOptions} the garbage collection options of the store
     *
     * @param gcOptions
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withGCOptions(SegmentGCOptions gcOptions) {
        this.gcOptions = requireNonNull(gcOptions);
        return this;
    }

    /**
     * {@link SegmentNotFoundExceptionListener} listener for  {@code SegmentNotFoundException}
     *
     * @param snfeListener, the actual listener
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withSnfeListener(@NotNull SegmentNotFoundExceptionListener snfeListener) {
        this.snfeListener = requireNonNull(snfeListener);
        return this;
    }

    @NotNull
    public FileStoreBuilder withIOMonitor(@NotNull IOMonitor ioMonitor) {
        ioMonitors.add(requireNonNull(ioMonitor));
        return this;
    }

    @NotNull
    public FileStoreBuilder withRemoteStoreMonitor(@NotNull RemoteStoreMonitor remoteStoreMonitor) {
        this.remoteStoreMonitor = remoteStoreMonitor;
        return this;
    }

    /**
     * Log IO reads at debug level to the passed logger
     *
     * @param logger logger for logging IO reads
     * @return this.
     */
    @NotNull
    public FileStoreBuilder withIOLogging(@NotNull Logger logger) {
        if (logger.isDebugEnabled()) {
            ioMonitors.add(new IOTraceMonitor(new IOTraceLogWriter(logger)));
        }
        return this;
    }

    /**
     * Enable strict version checking. With strict version checking enabled Oak
     * will fail to start if the store version does not exactly match this Oak version.
     * This is useful to e.g. avoid inadvertent upgrades during when running offline
     * compaction accidentally against an older version of a store.
     *
     * @param strictVersionCheck enables strict version checking iff {@code true}.
     * @return this instance
     */
    @NotNull
    public FileStoreBuilder withStrictVersionCheck(boolean strictVersionCheck) {
        this.strictVersionCheck = strictVersionCheck;
        return this;
    }

    public FileStoreBuilder withCustomPersistence(SegmentNodeStorePersistence persistence) {
        this.persistence = persistence;
        return this;
    }

    /**
     * Enable eager segment caching. This proves useful when segments need to
     * be cached as soon as they are created, right before persisting them to disk.
     * One such scenario is the cold standby, see OAK-8006.
     *
     * @param eagerSegmentCaching enables eager segment caching iff {@code true}.
     * @return this instance
     */
    public FileStoreBuilder withEagerSegmentCaching(boolean eagerSegmentCaching) {
        this.eagerSegmentCaching = eagerSegmentCaching;
        return this;
    }
    
    /**
     * Sets the threshold under which binaries are inlined in data segments.
     * @param binariesInlineThreshold the threshold
     * @return this instance
     */
    public FileStoreBuilder withBinariesInlineThreshold(int binariesInlineThreshold) {
        this.binariesInlineThreshold = binariesInlineThreshold;
        return this;
    }

    public Backend buildProcBackend(AbstractFileStore fileStore) throws IOException {
        return new FileStoreProcBackend(fileStore, persistence);
    }

    /**
     * Create a new {@link FileStore} instance with the settings specified in this
     * builder. If none of the {@code with} methods have been called before calling
     * this method, a file store with the following default settings is returned:
     * <ul>
     * <li>blob store: inline</li>
     * <li>max file size: 256MB</li>
     * <li>cache size: 256MB</li>
     * <li>memory mapping: on for 64 bit JVMs off otherwise</li>
     * <li>whiteboard: none. No {@link GCMonitor} tracking</li>
     * <li>statsProvider: {@link StatisticsProvider#NOOP}</li>
     * <li>GC options: {@link SegmentGCOptions#defaultGCOptions()}</li>
     * </ul>
     *
     * @return a new file store instance
     * @throws IOException
     */
    @NotNull
    public FileStore build() throws InvalidFileStoreVersionException, IOException {
        Validate.checkState(!built, "Cannot re-use builder");
        built = true;
        directory.mkdirs();
        TarRevisions revisions = new TarRevisions(persistence);
        LOG.info("Creating file store {}", this);
        FileStore store;
        try {
            store = new FileStore(this);
        } catch (InvalidFileStoreVersionException | IOException e) {
            try {
                revisions.close();
            } catch (IOException re) {
                LOG.warn("Unable to close TarRevisions", re);
            }
            throw e;
        }
        store.bind(revisions);
        return store;
    }

    /**
     * Create a new {@link ReadOnlyFileStore} instance with the settings specified in this
     * builder. If none of the {@code with} methods have been called before calling
     * this method, a file store with the following default settings is returned:
     * <ul>
     * <li>blob store: inline</li>
     * <li>max file size: 256MB</li>
     * <li>cache size: 256MB</li>
     * <li>memory mapping: on for 64 bit JVMs off otherwise</li>
     * <li>whiteboard: none. No {@link GCMonitor} tracking</li>
     * <li>statsProvider: {@link StatisticsProvider#NOOP}</li>
     * <li>GC options: {@link SegmentGCOptions#defaultGCOptions()}</li>
     * </ul>
     *
     * @return a new file store instance
     * @throws IOException
     */
    @NotNull
    public ReadOnlyFileStore buildReadOnly() throws InvalidFileStoreVersionException, IOException {
        Validate.checkState(!built, "Cannot re-use builder");
        Validate.checkState(directory.exists() && directory.isDirectory(),
                "%s does not exist or is not a directory", directory);
        built = true;
        ReadOnlyRevisions revisions = new ReadOnlyRevisions(persistence);
        LOG.info("Creating file store {}", this);
        ReadOnlyFileStore store;
        try {
            store = new ReadOnlyFileStore(this);
        } catch (InvalidFileStoreVersionException | IOException e) {
            try {
                revisions.close();
            } catch (IOException re) {
                LOG.warn("Unable to close ReadOnlyRevisions", re);
            }
            throw e;
        }
        store.bind(revisions);
        return store;
    }

    @NotNull
    File getDirectory() {
        return directory;
    }

    @Nullable
    BlobStore getBlobStore() {
        return blobStore;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    int getSegmentCacheSize() {
        return segmentCacheSize;
    }

    int getStringCacheSize() {
        return stringCacheSize;
    }

    int getTemplateCacheSize() {
        return templateCacheSize;
    }

    boolean getMemoryMapping() {
        return memoryMapping;
    }

    boolean getOffHeapAccess() {
        return offHeapAccess;
    }

    @NotNull
    GCListener getGcListener() {
        return gcListener;
    }

    @NotNull
    StatisticsProvider getStatsProvider() {
        return statsProvider;
    }

    @NotNull
    SegmentGCOptions getGcOptions() {
        return gcOptions;
    }

    @NotNull
    SegmentNotFoundExceptionListener getSnfeListener() {
        return snfeListener;
    }

    SegmentNodeStorePersistence getPersistence() {
        return persistence;
    }

    /**
     * @return creates or returns the {@code WriterCacheManager} this builder passes or
     * passed to the store on {@link #build()}.
     * @see #withNodeDeduplicationCacheSize(int)
     * @see #withStringDeduplicationCacheSize(int)
     * @see #withTemplateDeduplicationCacheSize(int)
     */
    @NotNull
    public WriterCacheManager getCacheManager() {
        if (cacheManager == null) {
            cacheManager = new EvictingWriteCacheManager(stringDeduplicationCacheSize,
                    templateDeduplicationCacheSize, nodeDeduplicationCacheSize);
        }
        return cacheManager;
    }

    IOMonitor getIOMonitor() {
        return ioMonitors.isEmpty()
                ? new IOMonitorAdapter()
                : new CompositeIOMonitor(ioMonitors);
    }

    RemoteStoreMonitor getRemoteStoreMonitor() {
        if (remoteStoreMonitor == null) {
            return new RemoteStoreMonitorAdapter();
        }
        return remoteStoreMonitor;
    }

    boolean getStrictVersionCheck() {
        return strictVersionCheck;
    }

    boolean getEagerSegmentCaching() {
        return eagerSegmentCaching;
    }
    
    int getBinariesInlineThreshold() {
        return binariesInlineThreshold;
    }

    @Override
    public String toString() {
        return "FileStoreBuilder{" +
                "version=" + getClass().getPackage().getImplementationVersion() +
                ", directory=" + directory +
                ", blobStore=" + blobStore +
                ", binariesInlineThreshold=" + binariesInlineThreshold +
                ", maxFileSize=" + maxFileSize +
                ", segmentCacheSize=" + segmentCacheSize +
                ", stringCacheSize=" + stringCacheSize +
                ", templateCacheSize=" + templateCacheSize +
                ", stringDeduplicationCacheSize=" + stringDeduplicationCacheSize +
                ", templateDeduplicationCacheSize=" + templateDeduplicationCacheSize +
                ", nodeDeduplicationCacheSize=" + nodeDeduplicationCacheSize +
                ", memoryMapping=" + memoryMapping +
                ", offHeapAccess=" + offHeapAccess +
                ", gcOptions=" + gcOptions +
                '}';
    }

    private static class EvictingWriteCacheManager extends WriterCacheManager.Default {
        public EvictingWriteCacheManager(
                int stringCacheSize,
                int templateCacheSize,
                int nodeCacheSize) {
            super(RecordCache.factory(stringCacheSize, new StringCacheWeigher()),
                    RecordCache.factory(templateCacheSize, new TemplateCacheWeigher()),
                    PriorityCache.factory(nodeCacheSize, new NodeCacheWeigher()));
        }

        void evictOldGeneration(final int newGeneration) {
            evictCaches(generation -> generation < newGeneration);
        }

        void evictGeneration(final int newGeneration) {
            evictCaches(generation -> generation == newGeneration);
        }
    }
}
