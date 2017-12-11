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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.EmpiricalWeigher;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.EvictionListener;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCacheStats;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.gc.LoggingGCMonitor;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Suppliers.ofInstance;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS;

/**
 * A generic builder for a {@link DocumentNodeStore}. By default the builder
 * will create an in-memory {@link DocumentNodeStore}. In most cases this is
 * only useful for tests.
 */
public class DocumentNodeStoreBuilder<T extends DocumentNodeStoreBuilder<T>> {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStoreBuilder.class);

    public static final long DEFAULT_MEMORY_CACHE_SIZE = 256 * 1024 * 1024;
    public static final int DEFAULT_NODE_CACHE_PERCENTAGE = 35;
    public static final int DEFAULT_PREV_DOC_CACHE_PERCENTAGE = 4;
    public static final int DEFAULT_CHILDREN_CACHE_PERCENTAGE = 15;
    public static final int DEFAULT_DIFF_CACHE_PERCENTAGE = 30;
    public static final int DEFAULT_CACHE_SEGMENT_COUNT = 16;
    public static final int DEFAULT_CACHE_STACK_MOVE_DISTANCE = 16;
    public static final int DEFAULT_UPDATE_LIMIT = 100000;

    /**
     * The path where the persistent cache is stored.
     */
    private static final String DEFAULT_PERSISTENT_CACHE_URI =
            System.getProperty("oak.documentMK.persCache");

    /**
     * The threshold where special handling for many child node starts.
     */
    static final int MANY_CHILDREN_THRESHOLD = Integer.getInteger(
            "oak.documentMK.manyChildren", 50);

    /**
     * Enable or disable the LIRS cache (null to use the default setting for this configuration).
     */
    private static final Boolean LIRS_CACHE;

    static {
        String s = System.getProperty("oak.documentMK.lirsCache");
        LIRS_CACHE = s == null ? null : Boolean.parseBoolean(s);
    }

    /**
     * Enable fast diff operations.
     */
    static final boolean FAST_DIFF = Boolean.parseBoolean(
            System.getProperty("oak.documentMK.fastDiff", "true"));

    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the private branch.
     */
    static final int UPDATE_LIMIT = Integer.getInteger("update.limit", DEFAULT_UPDATE_LIMIT);

    protected Supplier<DocumentStore> documentStoreSupplier = ofInstance(new MemoryDocumentStore());
    protected BlobStore blobStore;
    private DiffCache diffCache;
    private int clusterId  = Integer.getInteger("oak.documentMK.clusterId", 0);
    private int asyncDelay = 1000;
    private boolean timing;
    private boolean logging;
    private boolean leaseCheck = true; // OAK-2739 is enabled by default also for non-osgi
    private boolean isReadOnlyMode = false;
    private Weigher<CacheValue, CacheValue> weigher = new EmpiricalWeigher();
    private long memoryCacheSize = DEFAULT_MEMORY_CACHE_SIZE;
    private int nodeCachePercentage = DEFAULT_NODE_CACHE_PERCENTAGE;
    private int prevDocCachePercentage = DEFAULT_PREV_DOC_CACHE_PERCENTAGE;
    private int childrenCachePercentage = DEFAULT_CHILDREN_CACHE_PERCENTAGE;
    private int diffCachePercentage = DEFAULT_DIFF_CACHE_PERCENTAGE;
    private int cacheSegmentCount = DEFAULT_CACHE_SEGMENT_COUNT;
    private int cacheStackMoveDistance = DEFAULT_CACHE_STACK_MOVE_DISTANCE;
    private boolean useSimpleRevision;
    private boolean disableBranches;
    private boolean prefetchExternalChanges;
    private Clock clock = Clock.SIMPLE;
    private Executor executor;
    private String persistentCacheURI = DEFAULT_PERSISTENT_CACHE_URI;
    private PersistentCache persistentCache;
    private String journalCacheURI;
    private PersistentCache journalCache;
    private LeaseFailureHandler leaseFailureHandler;
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
    private BlobStoreStats blobStoreStats;
    private CacheStats blobStoreCacheStats;
    private DocumentStoreStatsCollector documentStoreStatsCollector;
    private DocumentNodeStoreStatsCollector nodeStoreStatsCollector;
    private Map<CacheType, PersistentCacheStats> persistentCacheStats =
            new EnumMap<CacheType, PersistentCacheStats>(CacheType.class);
    private boolean bundlingDisabled;
    private JournalPropertyHandlerFactory journalPropertyHandlerFactory =
            new JournalPropertyHandlerFactory();
    private int updateLimit = UPDATE_LIMIT;
    private int commitValueCacheSize = 10000;
    private long maxRevisionAgeMillis = DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS;
    private GCMonitor gcMonitor = new LoggingGCMonitor(
            LoggerFactory.getLogger(VersionGarbageCollector.class));
    private Predicate<String> nodeCachePredicate = Predicates.alwaysTrue();

    /**
     * @return a new {@link DocumentNodeStoreBuilder}.
     */
    public static DocumentNodeStoreBuilder<?> newDocumentNodeStoreBuilder() {
        return new DocumentNodeStoreBuilder();
    }

    public DocumentNodeStore build() {
        return new DocumentNodeStore(this);
    }

    @SuppressWarnings("unchecked")
    protected final T thisBuilder() {
        return (T) this;
    }

    /**
     * Sets the persistent cache option.
     *
     * @return this
     */
    public T setPersistentCache(String persistentCache) {
        this.persistentCacheURI = persistentCache;
        return thisBuilder();
    }

    /**
     * Sets the journal cache option.
     *
     * @return this
     */
    public T setJournalCache(String journalCache) {
        this.journalCacheURI = journalCache;
        return thisBuilder();
    }

    /**
     * Use the timing document store wrapper.
     *
     * @param timing whether to use the timing wrapper.
     * @return this
     */
    public T setTiming(boolean timing) {
        this.timing = timing;
        return thisBuilder();
    }

    public boolean getTiming() {
        return timing;
    }

    public T setLogging(boolean logging) {
        this.logging = logging;
        return thisBuilder();
    }

    public boolean getLogging() {
        return logging;
    }

    public T setLeaseCheck(boolean leaseCheck) {
        this.leaseCheck = leaseCheck;
        return thisBuilder();
    }

    public boolean getLeaseCheck() {
        return leaseCheck;
    }

    public T setReadOnlyMode() {
        this.isReadOnlyMode = true;
        return thisBuilder();
    }

    public boolean getReadOnlyMode() {
        return isReadOnlyMode;
    }

    public T setLeaseFailureHandler(LeaseFailureHandler leaseFailureHandler) {
        this.leaseFailureHandler = leaseFailureHandler;
        return thisBuilder();
    }

    public LeaseFailureHandler getLeaseFailureHandler() {
        return leaseFailureHandler;
    }

    /**
     * Set the document store to use. By default an in-memory store is used.
     *
     * @param documentStore the document store
     * @return this
     */
    public T setDocumentStore(DocumentStore documentStore) {
        this.documentStoreSupplier = ofInstance(documentStore);
        return thisBuilder();
    }

    public DocumentStore getDocumentStore() {
        return documentStoreSupplier.get();
    }

    public DiffCache getDiffCache() {
        if (diffCache == null) {
            diffCache = new TieredDiffCache(this);
        }
        return diffCache;
    }

    public T setDiffCache(DiffCache diffCache) {
        this.diffCache = diffCache;
        return thisBuilder();
    }

    /**
     * Set the blob store to use. By default an in-memory store is used.
     *
     * @param blobStore the blob store
     * @return this
     */
    public T setBlobStore(BlobStore blobStore) {
        this.blobStore = blobStore;
        return thisBuilder();
    }

    public BlobStore getBlobStore() {
        if (blobStore == null) {
            blobStore = new MemoryBlobStore();
            configureBlobStore(blobStore);
        }
        return blobStore;
    }

    /**
     * Set the cluster id to use. By default, 0 is used, meaning the cluster
     * id is automatically generated.
     *
     * @param clusterId the cluster id
     * @return this
     */
    public T setClusterId(int clusterId) {
        this.clusterId = clusterId;
        return thisBuilder();
    }

    public T setCacheSegmentCount(int cacheSegmentCount) {
        this.cacheSegmentCount = cacheSegmentCount;
        return thisBuilder();
    }

    public T setCacheStackMoveDistance(int cacheSegmentCount) {
        this.cacheStackMoveDistance = cacheSegmentCount;
        return thisBuilder();
    }

    public int getClusterId() {
        return clusterId;
    }

    /**
     * Set the maximum delay to write the last revision to the root node. By
     * default 1000 (meaning 1 second) is used.
     *
     * @param asyncDelay in milliseconds
     * @return this
     */
    public T setAsyncDelay(int asyncDelay) {
        this.asyncDelay = asyncDelay;
        return thisBuilder();
    }

    public int getAsyncDelay() {
        return asyncDelay;
    }

    public Weigher<CacheValue, CacheValue> getWeigher() {
        return weigher;
    }

    public T withWeigher(Weigher<CacheValue, CacheValue> weigher) {
        this.weigher = weigher;
        return thisBuilder();
    }

    public T memoryCacheSize(long memoryCacheSize) {
        this.memoryCacheSize = memoryCacheSize;
        return thisBuilder();
    }

    public T memoryCacheDistribution(int nodeCachePercentage,
                                     int prevDocCachePercentage,
                                     int childrenCachePercentage,
                                     int diffCachePercentage) {
        checkArgument(nodeCachePercentage >= 0);
        checkArgument(prevDocCachePercentage >= 0);
        checkArgument(childrenCachePercentage>= 0);
        checkArgument(diffCachePercentage >= 0);
        checkArgument(nodeCachePercentage + prevDocCachePercentage + childrenCachePercentage +
                diffCachePercentage < 100);
        this.nodeCachePercentage = nodeCachePercentage;
        this.prevDocCachePercentage = prevDocCachePercentage;
        this.childrenCachePercentage = childrenCachePercentage;
        this.diffCachePercentage = diffCachePercentage;
        return thisBuilder();
    }

    public long getNodeCacheSize() {
        return memoryCacheSize * nodeCachePercentage / 100;
    }

    public long getPrevDocumentCacheSize() {
        return memoryCacheSize * prevDocCachePercentage / 100;
    }

    public long getChildrenCacheSize() {
        return memoryCacheSize * childrenCachePercentage / 100;
    }

    public long getDocumentCacheSize() {
        return memoryCacheSize - getNodeCacheSize() - getPrevDocumentCacheSize() - getChildrenCacheSize()
                - getDiffCacheSize();
    }

    public long getDiffCacheSize() {
        return memoryCacheSize * diffCachePercentage / 100;
    }

    public long getMemoryDiffCacheSize() {
        return getDiffCacheSize() / 2;
    }

    public long getLocalDiffCacheSize() {
        return getDiffCacheSize() / 2;
    }

    public T setUseSimpleRevision(boolean useSimpleRevision) {
        this.useSimpleRevision = useSimpleRevision;
        return thisBuilder();
    }

    public boolean isUseSimpleRevision() {
        return useSimpleRevision;
    }

    public Executor getExecutor() {
        if(executor == null){
            return MoreExecutors.sameThreadExecutor();
        }
        return executor;
    }

    public T setExecutor(Executor executor){
        this.executor = executor;
        return thisBuilder();
    }

    public T clock(Clock clock) {
        this.clock = clock;
        return thisBuilder();
    }

    public T setStatisticsProvider(StatisticsProvider statisticsProvider){
        this.statisticsProvider = statisticsProvider;
        return thisBuilder();
    }

    public StatisticsProvider getStatisticsProvider() {
        return this.statisticsProvider;
    }
    public DocumentStoreStatsCollector getDocumentStoreStatsCollector() {
        if (documentStoreStatsCollector == null) {
            documentStoreStatsCollector = new DocumentStoreStats(statisticsProvider);
        }
        return documentStoreStatsCollector;
    }

    public T setDocumentStoreStatsCollector(DocumentStoreStatsCollector documentStoreStatsCollector) {
        this.documentStoreStatsCollector = documentStoreStatsCollector;
        return thisBuilder();
    }

    public DocumentNodeStoreStatsCollector getNodeStoreStatsCollector() {
        if (nodeStoreStatsCollector == null) {
            nodeStoreStatsCollector = new DocumentNodeStoreStats(statisticsProvider);
        }
        return nodeStoreStatsCollector;
    }

    public T setNodeStoreStatsCollector(DocumentNodeStoreStatsCollector statsCollector) {
        this.nodeStoreStatsCollector = statsCollector;
        return thisBuilder();
    }

    @Nonnull
    public Map<CacheType, PersistentCacheStats> getPersistenceCacheStats() {
        return persistentCacheStats;
    }

    @CheckForNull
    public BlobStoreStats getBlobStoreStats() {
        return blobStoreStats;
    }

    @CheckForNull
    public CacheStats getBlobStoreCacheStats() {
        return blobStoreCacheStats;
    }

    public Clock getClock() {
        return clock;
    }

    public T disableBranches() {
        disableBranches = true;
        return thisBuilder();
    }

    public boolean isDisableBranches() {
        return disableBranches;
    }

    public T setBundlingDisabled(boolean enabled) {
        bundlingDisabled = enabled;
        return thisBuilder();
    }

    public boolean isBundlingDisabled() {
        return bundlingDisabled;
    }

    public T setPrefetchExternalChanges(boolean b) {
        prefetchExternalChanges = b;
        return thisBuilder();
    }

    public boolean isPrefetchExternalChanges() {
        return prefetchExternalChanges;
    }

    public T setJournalPropertyHandlerFactory(JournalPropertyHandlerFactory factory) {
        journalPropertyHandlerFactory = factory;
        return thisBuilder();
    }

    public JournalPropertyHandlerFactory getJournalPropertyHandlerFactory() {
        return journalPropertyHandlerFactory;
    }

    public T setUpdateLimit(int limit) {
        updateLimit = limit;
        return thisBuilder();
    }

    public int getUpdateLimit() {
        return updateLimit;
    }

    public T setCommitValueCacheSize(int cacheSize) {
        this.commitValueCacheSize = cacheSize;
        return thisBuilder();
    }

    public int getCommitValueCacheSize() {
        return commitValueCacheSize;
    }

    public T setJournalGCMaxAge(long maxRevisionAgeMillis) {
        this.maxRevisionAgeMillis = maxRevisionAgeMillis;
        return thisBuilder();
    }

    /**
     * The maximum age for journal entries in milliseconds. Older entries
     * are candidates for GC.
     *
     * @return maximum age for journal entries in milliseconds.
     */
    public long getJournalGCMaxAge() {
        return maxRevisionAgeMillis;
    }

    public T setGCMonitor(@Nonnull GCMonitor gcMonitor) {
        this.gcMonitor = checkNotNull(gcMonitor);
        return thisBuilder();
    }

    public GCMonitor getGCMonitor() {
        return gcMonitor;
    }

    public VersionGCSupport createVersionGCSupport() {
        return new VersionGCSupport(getDocumentStore());
    }

    public Iterable<ReferencedBlob> createReferencedBlobs(final DocumentNodeStore ns) {
        return () -> new BlobReferenceIterator(ns);
    }

    public MissingLastRevSeeker createMissingLastRevSeeker() {
        return new MissingLastRevSeeker(getDocumentStore(), getClock());
    }

    public Cache<PathRev, DocumentNodeState> buildNodeCache(DocumentNodeStore store) {
        return buildCache(CacheType.NODE, getNodeCacheSize(), store, null);
    }

    public Cache<PathRev, DocumentNodeState.Children> buildChildrenCache(DocumentNodeStore store) {
        return buildCache(CacheType.CHILDREN, getChildrenCacheSize(), store, null);
    }

    public Cache<PathRev, StringValue> buildMemoryDiffCache() {
        return buildCache(CacheType.DIFF, getMemoryDiffCacheSize(), null, null);
    }

    public Cache<RevisionsKey, LocalDiffCache.Diff> buildLocalDiffCache() {
        return buildCache(CacheType.LOCAL_DIFF, getLocalDiffCacheSize(), null, null);
    }

    public Cache<CacheValue, NodeDocument> buildDocumentCache(DocumentStore docStore) {
        return buildCache(CacheType.DOCUMENT, getDocumentCacheSize(), null, docStore);
    }

    public Cache<StringValue, NodeDocument> buildPrevDocumentsCache(DocumentStore docStore) {
        return buildCache(CacheType.PREV_DOCUMENT, getPrevDocumentCacheSize(), null, docStore);
    }

    public NodeDocumentCache buildNodeDocumentCache(DocumentStore docStore, NodeDocumentLocks locks) {
        Cache<CacheValue, NodeDocument> nodeDocumentsCache = buildDocumentCache(docStore);
        CacheStats nodeDocumentsCacheStats = new CacheStats(nodeDocumentsCache, "Document-Documents", getWeigher(), getDocumentCacheSize());

        Cache<StringValue, NodeDocument> prevDocumentsCache = buildPrevDocumentsCache(docStore);
        CacheStats prevDocumentsCacheStats = new CacheStats(prevDocumentsCache, "Document-PrevDocuments", getWeigher(), getPrevDocumentCacheSize());

        return new NodeDocumentCache(nodeDocumentsCache, nodeDocumentsCacheStats, prevDocumentsCache, prevDocumentsCacheStats, locks);
    }

    public T setNodeCachePredicate(Predicate<String> p){
        this.nodeCachePredicate = p;
        return thisBuilder();
    }

    public Predicate<String> getNodeCachePredicate() {
        return nodeCachePredicate;
    }

    @SuppressWarnings("unchecked")
    private <K extends CacheValue, V extends CacheValue> Cache<K, V> buildCache(
            CacheType cacheType,
            long maxWeight,
            DocumentNodeStore docNodeStore,
            DocumentStore docStore) {
        Set<EvictionListener<K, V>> listeners = new CopyOnWriteArraySet<EvictionListener<K,V>>();
        Cache<K, V> cache = buildCache(cacheType.name(), maxWeight, listeners);
        PersistentCache p = null;
        if (cacheType == CacheType.DIFF || cacheType == CacheType.LOCAL_DIFF) {
            // use separate journal cache if configured
            p = getJournalCache();
        }
        if (p == null) {
            // otherwise fall back to single persistent cache
            p = getPersistentCache();
        }
        if (p != null) {
            cache = p.wrap(docNodeStore, docStore, cache, cacheType, statisticsProvider);
            if (cache instanceof EvictionListener) {
                listeners.add((EvictionListener<K, V>) cache);
            }
            PersistentCacheStats stats = PersistentCache.getPersistentCacheStats(cache);
            if (stats != null) {
                persistentCacheStats.put(cacheType, stats);
            }
        }
        return cache;
    }

    public PersistentCache getPersistentCache() {
        if (persistentCacheURI == null) {
            return null;
        }
        if (persistentCache == null) {
            try {
                persistentCache = new PersistentCache(persistentCacheURI);
            } catch (Throwable e) {
                LOG.warn("Persistent cache not available; please disable the configuration", e);
                throw new IllegalArgumentException(e);
            }
        }
        return persistentCache;
    }

    PersistentCache getJournalCache() {
        if (journalCacheURI == null) {
            return null;
        }
        if (journalCache == null) {
            try {
                journalCache = new PersistentCache(journalCacheURI);
            } catch (Throwable e) {
                LOG.warn("Journal cache not available; please disable the configuration", e);
                throw new IllegalArgumentException(e);
            }
        }
        return journalCache;
    }

    private <K extends CacheValue, V extends CacheValue> Cache<K, V> buildCache(
            String module,
            long maxWeight,
            final Set<EvictionListener<K, V>> listeners) {
        // by default, use the LIRS cache when using the persistent cache,
        // but don't use it otherwise
        boolean useLirs = persistentCacheURI != null;
        // allow to override this by using the system property
        if (LIRS_CACHE != null) {
            useLirs = LIRS_CACHE;
        }
        // do not use LIRS cache when maxWeight is zero (OAK-6953)
        if (useLirs && maxWeight > 0) {
            return CacheLIRS.<K, V>newBuilder().
                    module(module).
                    weigher(new Weigher<K, V>() {
                        @Override
                        public int weigh(K key, V value) {
                            return weigher.weigh(key, value);
                        }
                    }).
                    averageWeight(2000).
                    maximumWeight(maxWeight).
                    segmentCount(cacheSegmentCount).
                    stackMoveDistance(cacheStackMoveDistance).
                    recordStats().
                    evictionCallback(new CacheLIRS.EvictionCallback<K, V>() {
                        @Override
                        public void evicted(K key, V value, RemovalCause cause) {
                            for (EvictionListener<K, V> l : listeners) {
                                l.evicted(key, value, cause);
                            }
                        }
                    }).
                    build();
        }
        return CacheBuilder.newBuilder().
                concurrencyLevel(cacheSegmentCount).
                weigher(weigher).
                maximumWeight(maxWeight).
                recordStats().
                removalListener(new RemovalListener<K, V>() {
                    @Override
                    public void onRemoval(RemovalNotification<K, V> notification) {
                        for (EvictionListener<K, V> l : listeners) {
                            l.evicted(notification.getKey(), notification.getValue(), notification.getCause());
                        }
                    }
                }).
                build();
    }

    protected void setGCBlobStore(GarbageCollectableBlobStore s) {
        configureBlobStore(s);
        PersistentCache p = getPersistentCache();
        if (p != null) {
            s = p.wrapBlobStore(s);
        }
        this.blobStore = s;
    }

    /**
     * BlobStore which are created by builder might get wrapped.
     * So here we perform any configuration and also access any
     * service exposed by the store
     *
     * @param blobStore store to config
     */
    private void configureBlobStore(BlobStore blobStore) {
        if (blobStore instanceof AbstractBlobStore){
            this.blobStoreStats = new BlobStoreStats(statisticsProvider);
            ((AbstractBlobStore) blobStore).setStatsCollector(blobStoreStats);
        }

        if (blobStore instanceof CachingBlobStore){
            blobStoreCacheStats = ((CachingBlobStore) blobStore).getCacheStats();
        }
    }
}
