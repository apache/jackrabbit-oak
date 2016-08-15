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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_STRING_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_TEMPLATE_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_NODE_CACHE_CAPACITY;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_NODE_CACHE_DEPTH;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_STRING_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_TEMPLATE_CACHE_SIZE;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;

import java.io.File;
import java.io.IOException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.apache.jackrabbit.oak.segment.NodeCache;
import org.apache.jackrabbit.oak.segment.RecordCache;
import org.apache.jackrabbit.oak.segment.Template;
import org.apache.jackrabbit.oak.segment.WriterCacheManager;
import org.apache.jackrabbit.oak.segment.compaction.LoggingGCMonitor;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore.ReadOnlyStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.DelegatingGCMonitor;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for creating {@link FileStore} instances.
 */
public class FileStoreBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

    @Nonnull
    private final File directory;

    @CheckForNull
    private BlobStore blobStore;   // null ->  store blobs inline

    private int maxFileSize = 256;

    private int segmentCacheSize = DEFAULT_SEGMENT_CACHE_MB;

    private int stringCacheSize = DEFAULT_STRING_CACHE_MB;

    private int templateCacheSize = DEFAULT_TEMPLATE_CACHE_MB;

    private int stringDeduplicationCacheSize = DEFAULT_STRING_CACHE_SIZE;

    private int templateDeduplicationCacheSize = DEFAULT_TEMPLATE_CACHE_SIZE;

    private int nodeDeduplicationCacheSize = DEFAULT_NODE_CACHE_CAPACITY;

    private int nodeDeduplicationCacheDepth = DEFAULT_NODE_CACHE_DEPTH;

    private boolean memoryMapping;

    @Nonnull
    private StatisticsProvider statsProvider = StatisticsProvider.NOOP;

    @Nonnull
    private SegmentGCOptions gcOptions = defaultGCOptions();

    @Nonnull
    private final DelegatingGCMonitor gcMonitor = new DelegatingGCMonitor();

    @CheckForNull
    private EvictingWriteCacheManager cacheManager;

    @Nonnull
    private final GCListener gcListener = new GCListener() {
        @Override
        public void info(String message, Object... arguments) {
            gcMonitor.info(message, arguments);
        }

        @Override
        public void warn(String message, Object... arguments) {
            gcMonitor.warn(message, arguments);
        }

        @Override
        public void error(String message, Exception exception) {
            gcMonitor.error(message, exception);
        }

        @Override
        public void skipped(String reason, Object... arguments) {
            gcMonitor.skipped(reason, arguments);
        }

        @Override
        public void compacted(long[] segmentCounts, long[] recordCounts, long[] compactionMapWeights) {
            gcMonitor.compacted(segmentCounts, recordCounts, compactionMapWeights);
        }

        @Override
        public void cleaned(long reclaimedSize, long currentSize) {
            gcMonitor.cleaned(reclaimedSize, currentSize);
        }

        @Override
        public void compacted(@Nonnull Status status, final int newGeneration) {
            checkNotNull(cacheManager);
            switch (status) {
                case SUCCESS:
                    // FIXME OAK-4283: Align GCMonitor API with implementation
                    // This call is still needed to ensure upstream consumers
                    // of GCMonitor callback get properly notified. See
                    // RepositoryImpl.RefreshOnGC and
                    // LuceneIndexProviderService.registerGCMonitor().
                    gcMonitor.compacted(new long[0], new long[0], new long[0]);
                    cacheManager.evictOldGeneration(newGeneration);
                    break;
                case FAILURE:
                    cacheManager.evictGeneration(newGeneration);
            }
        }
    };

    @CheckForNull
    private TarRevisions revisions;

    /**
     * Create a new instance of a {@code FileStoreBuilder} for a file store.
     * @param directory  directory where the tar files are stored
     * @return a new {@code FileStoreBuilder} instance.
     */
    @Nonnull
    public static FileStoreBuilder fileStoreBuilder(@Nonnull File directory) {
        return new FileStoreBuilder(directory);
    }

    private FileStoreBuilder(@Nonnull File directory) {
        this.directory = checkNotNull(directory);
        this.gcMonitor.registerGCMonitor(new LoggingGCMonitor(LOG));
    }

    /**
     * Specify the {@link BlobStore}.
     * @param blobStore
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withBlobStore(@Nonnull BlobStore blobStore) {
        this.blobStore = checkNotNull(blobStore);
        return this;
    }

    /**
     * Maximal size of the generated tar files in MB.
     * @param maxFileSize
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
        return this;
    }

    /**
     * Size of the segment cache in MB.
     * @param segmentCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withSegmentCacheSize(int segmentCacheSize) {
        this.segmentCacheSize = segmentCacheSize;
        return this;
    }

    /**
     * Size of the string cache in MB.
     * @param stringCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withStringCacheSize(int stringCacheSize) {
        this.stringCacheSize = stringCacheSize;
        return this;
    }

    /**
     * Size of the template cache in MB.
     * @param templateCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withTemplateCacheSize(int templateCacheSize) {
        this.templateCacheSize = templateCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the string deduplication cache
     * @param stringDeduplicationCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withStringDeduplicationCacheSize(int stringDeduplicationCacheSize) {
        this.stringDeduplicationCacheSize = stringDeduplicationCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the template deduplication cache
     * @param templateDeduplicationCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withTemplateDeduplicationCacheSize(int templateDeduplicationCacheSize) {
        this.templateDeduplicationCacheSize = templateDeduplicationCacheSize;
        return this;
    }

    /**
     * Number of items to keep in the node deduplication cache
     * @param nodeDeduplicationCacheSize  None negative cache size
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withNodeDeduplicationCacheSize(int nodeDeduplicationCacheSize) {
        this.nodeDeduplicationCacheSize = nodeDeduplicationCacheSize;
        return this;
    }

    /**
     * Maximal depth of the node deduplication cache
     * @param nodeDeduplicationCacheDepth
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withNodeDeduplicationDepth(int nodeDeduplicationCacheDepth) {
        this.nodeDeduplicationCacheDepth = nodeDeduplicationCacheDepth;
        return this;
    }

    /**
     * Turn memory mapping on or off
     * @param memoryMapping
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withMemoryMapping(boolean memoryMapping) {
        this.memoryMapping = memoryMapping;
        return this;
    }

    /**
     * Set memory mapping to the default value based on OS properties
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withDefaultMemoryMapping() {
        this.memoryMapping = FileStore.MEMORY_MAPPING_DEFAULT;
        return this;
    }

    /**
     * {@link GCMonitor} for monitoring this files store's gc process.
     * @param gcMonitor
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withGCMonitor(@Nonnull GCMonitor gcMonitor) {
        this.gcMonitor.registerGCMonitor(checkNotNull(gcMonitor));
        return this;
    }

    /**
     * {@link StatisticsProvider} for collecting statistics related to FileStore
     * @param statisticsProvider
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withStatisticsProvider(@Nonnull StatisticsProvider statisticsProvider) {
        this.statsProvider = checkNotNull(statisticsProvider);
        return this;
    }

    /**
     * {@link SegmentGCOptions} the garbage collection options of the store
     * @param gcOptions
     * @return this instance
     */
    @Nonnull
    public FileStoreBuilder withGCOptions(SegmentGCOptions gcOptions) {
        this.gcOptions = checkNotNull(gcOptions);
        return this;
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
    @Nonnull
    public FileStore build() throws InvalidFileStoreVersionException, IOException {
        checkState(revisions == null, "Cannot re-use builder");
        directory.mkdir();
        revisions = new TarRevisions(false, directory);
        LOG.info("Creating file store {}", this);
        return new FileStore(this, false).bind(revisions);
    }

    /**
     * Create a new {@link ReadOnlyStore} instance with the settings specified in this
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
    @Nonnull
    public ReadOnlyStore buildReadOnly() throws InvalidFileStoreVersionException, IOException {
        checkState(revisions == null, "Cannot re-use builder");
        checkState(directory.exists() && directory.isDirectory());
        revisions = new TarRevisions(true, directory);
        LOG.info("Creating file store {}", this);
        return new ReadOnlyStore(this).bind(revisions);
    }

    @Nonnull
    File getDirectory() {
        return directory;
    }

    @CheckForNull
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

    @Nonnull
    GCListener getGcListener() {
        return gcListener;
    }

    @Nonnull
    StatisticsProvider getStatsProvider() {
        return statsProvider;
    }

    @Nonnull
    SegmentGCOptions getGcOptions() {
        return gcOptions;
    }

    @Nonnull
    TarRevisions getRevisions() {
        checkState(revisions != null, "File store not yet built");
        return revisions;
    }

    @Nonnull
    WriterCacheManager getCacheManager() {
        if (cacheManager == null) {
            cacheManager = new EvictingWriteCacheManager(
                    stringDeduplicationCacheSize, templateDeduplicationCacheSize,
                    nodeDeduplicationCacheSize, nodeDeduplicationCacheDepth);
        }
        return cacheManager;
    }

    @Override
    public String toString() {
        return "FileStoreBuilder{" +
                "directory=" + directory +
                ", blobStore=" + blobStore +
                ", maxFileSize=" + maxFileSize +
                ", segmentCacheSize=" + segmentCacheSize +
                ", stringCacheSize=" + stringCacheSize +
                ", templateCacheSize=" + templateCacheSize +
                ", stringDeduplicationCacheSize=" + stringDeduplicationCacheSize +
                ", templateDeduplicationCacheSize=" + templateDeduplicationCacheSize +
                ", nodeDeduplicationCacheSize=" + nodeDeduplicationCacheSize +
                ", nodeDeduplicationCacheDepth=" + nodeDeduplicationCacheDepth +
                ", memoryMapping=" + memoryMapping +
                ", gcOptions=" + gcOptions +
                '}';
    }

    private static class EvictingWriteCacheManager extends WriterCacheManager.Default {
        public EvictingWriteCacheManager(int stringCacheSize, int templateCacheSize,
                                         int nodeCacheCapacity, int nodeCacheDepth) {
            super(RecordCache.<String>factory(stringCacheSize),
                RecordCache.<Template>factory(templateCacheSize),
                NodeCache.factory(nodeCacheCapacity, nodeCacheDepth));
        }

        void evictOldGeneration(final int newGeneration) {
            evictCaches(new Predicate<Integer>() {
                @Override
                public boolean apply(Integer generation) {
                    return generation < newGeneration;
                }
            });
        }

        void evictGeneration(final int newGeneration) {
            evictCaches(new Predicate<Integer>() {
                @Override
                public boolean apply(Integer generation) {
                    return generation == newGeneration;
                }
            });
        }
    }
}
