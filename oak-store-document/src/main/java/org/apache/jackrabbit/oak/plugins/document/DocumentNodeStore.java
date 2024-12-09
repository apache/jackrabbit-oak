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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Iterables.partition;
import static org.apache.jackrabbit.guava.common.collect.Iterables.transform;
import static org.apache.jackrabbit.guava.common.collect.Lists.reverse;
import static java.util.Collections.singletonList;
import static java.util.Objects.nonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.MANY_CHILDREN_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PATH;
import static org.apache.jackrabbit.oak.plugins.document.Path.ROOT;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.alignWithExternalRevisions;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getModuleVersion;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isFullGCEnabled;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isEmbeddedVerificationEnabled;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isThrottlingEnabled;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.pathToId;
import static org.apache.jackrabbit.oak.spi.observation.ChangeSet.COMMIT_CONTEXT_OBSERVATION_CHANGESET;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.util.concurrent.UncheckedExecutionException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PerfLogger;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.Branch.BranchCommit;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundledDocumentDiffer;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler;
import org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast.DynamicBroadcastConfig;
import org.apache.jackrabbit.oak.plugins.document.prefetch.CacheWarming;
import org.apache.jackrabbit.oak.plugins.document.util.LeaseCheckDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.LoggingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.ReadOnlyDocumentStoreWrapperFactory;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.ThrottlingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.observation.ChangeSet;
import org.apache.jackrabbit.oak.spi.observation.ChangeSetBuilder;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.PrefetchNodeStore;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.base.Strings;

import org.apache.jackrabbit.guava.common.base.Suppliers;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;

import org.apache.jackrabbit.guava.common.collect.Iterables;

/**
 * Implementation of a NodeStore on {@link DocumentStore}.
 */
public final class DocumentNodeStore
        implements NodeStore, RevisionContext, Observable, Clusterable, PrefetchNodeStore, NodeStateDiffer {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStore.class);

    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(DocumentNodeStore.class.getName() + ".perf"));

    /**
     * Number of milliseconds in one minute.
     */
    private static final long ONE_MINUTE_MS = TimeUnit.MINUTES.toMillis(1);

    public static final FormatVersion VERSION = FormatVersion.V1_8;

    /**
     * List of meta properties which are created by DocumentNodeStore and which needs to be
     * retained in any cloned copy of DocumentNodeState.
     */
    public static final List<String> META_PROP_NAMES = ImmutableList.of(
            DocumentBundlor.META_PROP_PATTERN,
            DocumentBundlor.META_PROP_BUNDLING_PATH,
            DocumentBundlor.META_PROP_NON_BUNDLED_CHILD,
            DocumentBundlor.META_PROP_BUNDLED_CHILD
    );

    /**
     * Enable fast diff operations.
     */
    private static final boolean FAST_DIFF = SystemPropertySupplier.create("oak.documentMK.fastDiff", Boolean.TRUE).loggingTo(LOG)
            .get();

    /**
     * Feature flag to enable concurrent add/remove operations of hidden empty
     * nodes. See OAK-2673.
     */
    private boolean enableConcurrentAddRemove = SystemPropertySupplier.create("oak.enableConcurrentAddRemove", Boolean.FALSE)
            .loggingTo(LOG).get();

    /**
     * Use fair mode for background operation lock.
     */
    private boolean fairBackgroundOperationLock = SystemPropertySupplier.create("oak.fairBackgroundOperationLock", Boolean.TRUE)
            .loggingTo(LOG).get();

    public static final String SYS_PROP_DISABLE_JOURNAL = "oak.disableJournalDiff";
    /**
     * Feature flag to disable the journal diff mechanism. See OAK-4528.
     */
    private boolean disableJournalDiff = SystemPropertySupplier.create(SYS_PROP_DISABLE_JOURNAL, Boolean.FALSE).loggingTo(LOG)
            .get();

    /**
     * Threshold for number of paths in journal entry to require a force push during commit
     * (instead of at background write)
     */
    private int journalPushThreshold = SystemPropertySupplier.create("oak.journalPushThreshold", 100000).loggingTo(LOG).get();

    /**
     * How many collision entries to collect in a single call.
     */
    private int collisionGarbageBatchSize = SystemPropertySupplier.create("oak.documentMK.collisionGarbageBatchSize", 1000)
            .loggingTo(LOG).get();

    /**
     * The number of updates to batch with a single call to
     * {@link DocumentStore#createOrUpdate(Collection, List)}.
     */
    private final int createOrUpdateBatchSize = SystemPropertySupplier.create("oak.documentMK.createOrUpdateBatchSize", 1000)
            .loggingTo(LOG).get();

    public static final String SYS_PROP_DISABLE_SWEEP2 = "oak.documentMK.disableSweep2";
    private boolean disableSweep2 = SystemPropertySupplier.create(SYS_PROP_DISABLE_SWEEP2, Boolean.FALSE).loggingTo(LOG)
            .get();

    // OAK-2682: time difference detection applied at startup with a default
    // max time diff of 2000 millis (2sec)
    static final long DEFAULT_MAX_SERVER_TIME_DIFFERENCE = 2000L;
    private final long maxTimeDiffMillis = SystemPropertySupplier.create("oak.documentMK.maxServerTimeDiffMillis", DEFAULT_MAX_SERVER_TIME_DIFFERENCE).loggingTo(LOG).get();

    public static final String SYS_PROP_PREFETCH = "oak.documentstore.prefetch";
    private final boolean prefetchEnabled = SystemPropertySupplier.create(SYS_PROP_PREFETCH, false).loggingTo(LOG).get();

    /**
     * The document store without potentially lease checking wrapper.
     */
    private final DocumentStore nonLeaseCheckingStore;

    /**
     * The document store (might be used by multiple node stores).
     */
    private final DocumentStore store;

    /**
     * Marker node, indicating a node does not exist at a given revision.
     */
    private final DocumentNodeState missing;

    /**
     * The commit queue to coordinate the commits.
     */
    protected final CommitQueue commitQueue;

    /**
     * Commit queue for batch updates.
     */
    private final BatchCommitQueue batchCommitQueue;

    /**
     * The change dispatcher for this node store.
     */
    private final ChangeDispatcher dispatcher;

    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    private int asyncDelay = 1000;

    /**
     * The maximum back off time in milliseconds when merges are retried. The
     * default value is twice the {@link #asyncDelay}.
     */
    private int maxBackOffMillis = SystemPropertySupplier.create("oak.maxBackOffMS", asyncDelay * 2).loggingTo(LOG).get();

    private int changeSetMaxItems = SystemPropertySupplier.create("oak.document.changeSet.maxItems", 50).loggingTo(LOG).get();

    /**
     * Batch size to purge uncommitted revisions & collisions on boot-up. The default value is 50.
     */
    private int purgeUncommittedRevisions = SystemPropertySupplier.create("oak.document.purgeUncommittedRevisions.batchSize", 50).loggingTo(LOG).get();

    private int changeSetMaxDepth = SystemPropertySupplier.create("oak.document.changeSet.maxDepth", 9).loggingTo(LOG).get();

    /**
     * Whether this instance is disposed.
     */
    private final AtomicBoolean isDisposed = new AtomicBoolean();

    /**
     * Whether the lease update thread shall be stopped.
     */
    private final AtomicBoolean stopLeaseUpdateThread = new AtomicBoolean();

    /**
     * The cluster instance info.
     */
    @NotNull
    private final ClusterNodeInfo clusterNodeInfo;

    /**
     * The unique cluster id, similar to the unique machine id in MongoDB.
     */
    private final int clusterId;

    /**
     * Map of known cluster nodes and the last known state updated
     * by {@link #updateClusterState()}.
     * Key: clusterId, value: ClusterNodeInfoDocument
     */
    private final ConcurrentMap<Integer, ClusterNodeInfoDocument> clusterNodes = new ConcurrentHashMap<>();

    /**
     * Unmerged branches of this DocumentNodeStore instance.
     */
    private final UnmergedBranches branches;

    /**
     * The unsaved last revisions. This contains the parents of all changed
     * nodes, once those nodes are committed but the parent node itself wasn't
     * committed yet. The parents are not immediately persisted as this would
     * cause each commit to change all parents (including the root node), which
     * would limit write scalability.
     *
     * Key: path, value: revision.
     */
    private final UnsavedModifications unsavedLastRevisions = new UnsavedModifications();

    /**
     * Set of IDs for documents that may need to be split.
     */
    private final Map<String, String> splitCandidates = new ConcurrentHashMap<>();

    /**
     * Summary of changes done by this cluster node to persist by the background
     * update thread.
     */
    private JournalEntry changes;

    /**
     * The current root node state.
     */
    private volatile DocumentNodeState root;

    private Thread backgroundReadThread;

    /**
     * Monitor object to synchronize background reads.
     */
    private final Object backgroundReadMonitor = new Object();

    /**
     * Background thread performing updates of _lastRev entries.
     */
    private Thread backgroundUpdateThread;

    /**
     * Background thread performing purging of unmerged branch commits & collision markers
     */
    private Thread backgroundPurgeThread;

    /**
     * Monitor object to synchronize background writes.
     */
    private final Object backgroundWriteMonitor = new Object();

    /**
     * Background thread performing the clusterId lease renew.
     */
    @NotNull
    private Thread leaseUpdateThread;

    /**
     * Background thread performing the cluster update
     */
    @NotNull
    private Thread clusterUpdateThread;

    /**
     * Monitor object to synchronize background sweeps.
     */
    private final Object backgroundSweepMonitor = new Object();

    /**
     * Sweep thread cleaning up uncommitted changes.
     */
    private Thread backgroundSweepThread;

    /**
     * Extra, one-off sweep2 background task for fixing OAK-9176 ie missing _bc
     * on parents and root.
     */
    private Thread backgroundSweep2Thread;

    /**
     * The sweep revision vector. Revisions for trunk commits older than this
     * can safely be considered committed without looking up the commit value
     * on the commit root document.
     */
    private RevisionVector sweepRevisions = new RevisionVector();

    /**
     * Read/Write lock for background operations. Regular commits will acquire
     * a shared lock, while a background write acquires an exclusive lock.
     */
    private final ReadWriteLock backgroundOperationLock =
            new ReentrantReadWriteLock(fairBackgroundOperationLock);

    /**
     * Read/Write lock to coordinate merges. In most cases merges acquire a
     * shared read lock and can proceed concurrently. An exclusive write lock
     * is acquired when the merge fails even after some retries and a final
     * retry cycle is done.
     * See {@link DocumentNodeStoreBranch#merge(CommitHook, CommitInfo)}.
     */
    private final ReadWriteLock mergeLock = new ReentrantReadWriteLock();

    /**
     * Enable using simple revisions (just a counter). This feature is useful
     * for testing.
     */
    private AtomicInteger simpleRevisionCounter;

    /**
     * The node cache.
     *
     * Key: PathRev, value: DocumentNodeState
     */
    private final Cache<PathRev, DocumentNodeState> nodeCache;
    private final CacheStats nodeCacheStats;

    /**
     * Child node cache.
     *
     * Key: PathRev, value: Children
     */
    private final Cache<NamePathRev, DocumentNodeState.Children> nodeChildrenCache;
    private final CacheStats nodeChildrenCacheStats;

    /**
     * The change log to keep track of commits for diff operations.
     */
    private final DiffCache diffCache;

    /**
     * Tiny cache for non existence of any revisions in previous documents
     * for particular properties.
     */
    private final Cache<StringValue, StringValue> prevNoPropCache;

    /**
     * The commit value resolver for this node store.
     */
    private final CommitValueResolver commitValueResolver;

    /**
     * The blob store.
     */
    private final BlobStore blobStore;

    /**
     * The clusterStateChangeListener is invoked on any noticed change in the
     * clusterNodes collection.
     * <p>
     * Note that there is no synchronization between setting this one and using
     * it, but arguably that is not necessary since it will be set at startup
     * time and then never be changed.
     */
    private ClusterStateChangeListener clusterStateChangeListener;

    /**
     * The BlobSerializer.
     */
    private final BlobSerializer blobSerializer = new BlobSerializer() {
        @Override
        public String serialize(Blob blob) {

            if (blob instanceof BlobStoreBlob) {
                BlobStoreBlob bsb = (BlobStoreBlob) blob;
                BlobStore bsbBlobStore = bsb.getBlobStore();
                //see if the blob has been created from another store
                if (bsbBlobStore != null && bsbBlobStore.equals(blobStore)) {
                    return bsb.getBlobId();
                }
            }

            String id;

            String reference = blob.getReference();
            if(reference != null){
                id = blobStore.getBlobId(reference);
                if(id != null){
                    return id;
                }
            }

            try {
                id = createBlob(blob.getNewStream()).getBlobId();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return id;
        }
    };

    private final Clock clock;

    private final Checkpoints checkpoints;

    private final VersionGarbageCollector versionGarbageCollector;

    private final JournalGarbageCollector journalGarbageCollector;

    private final Iterable<ReferencedBlob> referencedBlobs;
    
    private final Executor executor;

    private final MissingLastRevSeeker lastRevSeeker;

    private final LastRevRecoveryAgent lastRevRecoveryAgent;

    private final boolean disableBranches;

    private PersistentCache persistentCache;

    private PersistentCache journalCache;

    private final DocumentNodeStoreMBean mbean;

    private final boolean readOnlyMode;

    private DocumentNodeStateCache nodeStateCache = DocumentNodeStateCache.NOOP;

    private final DocumentNodeStoreStatsCollector nodeStoreStatsCollector;

    private final BundlingConfigHandler bundlingConfigHandler = new BundlingConfigHandler();

    private final BundledDocumentDiffer bundledDocDiffer = new BundledDocumentDiffer(this);

    private final JournalPropertyHandlerFactory journalPropertyHandlerFactory;

    private final int updateLimit;

    /**
     * A set of non-branch commit revisions that are currently in progress. A
     * revision is added to this set when {@link #newTrunkCommit(Changes, RevisionVector)}
     * is called and removed when the commit either:
     * <ul>
     *     <li>Succeeds with {@link #done(Commit, boolean, CommitInfo)}</li>
     *     <li>Fails with {@link #canceled(Commit)} and the commit was
     *      successfully rolled back.</li>
     * </ul>
     * The {@link NodeDocumentSweeper} periodically goes through this set and
     * reverts changes done by commits in the set that are older than the
     * current head revision.
     */
    private final Set<Revision> inDoubtTrunkCommits = CollectionUtils.newConcurrentHashSet();

    /**
     * Contains journal entry revisions (branch commit style) that were created
     * as a result of a rollback and are meant to trigger an invalidation in
     * peer cluster nodes. This list is typically empty or small. It is emptied
     * upon each backgroundWrite. It is used to avoid duplicate journal entries
     * that would otherwise be created as a result of merge (normal plus exclusive) retries
     */
    private final Set<String> pendingRollbackInvalidations = CollectionUtils.newConcurrentHashSet();

    private final Predicate<Path> nodeCachePredicate;

    private final Feature prefetchFeature;

    private final Feature cancelInvalidationFeature;

    private final Feature noChildOrderCleanupFeature;

    private Boolean cancelInvalidationLogged;

    private CacheWarming cacheWarming;

    public DocumentNodeStore(DocumentNodeStoreBuilder<?> builder) {
        this.nodeCachePredicate = builder.getNodeCachePathPredicate();
        this.updateLimit = builder.getUpdateLimit();
        this.commitValueResolver = new CachingCommitValueResolver(
                builder.getCommitValueCacheSize(), this::getSweepRevisions)
                .withEmptyCommitValueCache(
                        builder.getCacheEmptyCommitValue() && builder.getReadOnlyMode(),
                        builder.getClock(), builder.getJournalGCMaxAge());
        this.blobStore = builder.getBlobStore();
        this.nodeStoreStatsCollector = builder.getNodeStoreStatsCollector();
        if (builder.isUseSimpleRevision()) {
            this.simpleRevisionCounter = new AtomicInteger(0);
        }
        DocumentStore s = builder.getDocumentStore();
        checkServerTimeDifference(s);
        if (builder.getTiming()) {
            s = new TimingDocumentStoreWrapper(s);
        }
        if (builder.getLogging()) {
            if (builder.getLoggingPrefix() != null) {
                s = new LoggingDocumentStoreWrapper(s, builder.getLoggingPrefix());
            } else {
                s = new LoggingDocumentStoreWrapper(s);
            }
        }
        if (builder.getReadOnlyMode()) {
            s = ReadOnlyDocumentStoreWrapperFactory.getInstance(s);
            readOnlyMode = true;
        } else {
            readOnlyMode = false;
        }
        checkVersion(s, readOnlyMode);
        this.nonLeaseCheckingStore = s;
        this.executor = builder.getExecutor();
        this.lastRevSeeker = builder.createMissingLastRevSeeker();
        this.clock = builder.getClock();

        int cid = builder.getClusterId();
        cid = SystemPropertySupplier.create("oak.documentMK.clusterId", cid).loggingTo(LOG).get();
        if (readOnlyMode) {
            clusterNodeInfo = ClusterNodeInfo.getReadOnlyInstance(nonLeaseCheckingStore);
        } else {
            clusterNodeInfo = ClusterNodeInfo.getInstance(nonLeaseCheckingStore,
                    new RecoveryHandlerImpl(nonLeaseCheckingStore, clock, lastRevSeeker),
                    null, null, cid, builder.isClusterInvisible(),
                    builder.getClusterIdReuseDelayAfterRecovery());
            checkRevisionAge(nonLeaseCheckingStore, clusterNodeInfo, clock);
        }
        this.clusterId = clusterNodeInfo.getId();

        if (isThrottlingEnabled(builder)) {
            s = new ThrottlingDocumentStoreWrapper(s, builder.getThrottlingStatsCollector());
        }

        clusterNodeInfo.setLeaseCheckMode(builder.getLeaseCheckMode());
        if (builder.getLeaseCheckMode() != LeaseCheckMode.DISABLED) {
            s = new LeaseCheckDocumentStoreWrapper(s, clusterNodeInfo);
            clusterNodeInfo.setLeaseFailureHandler(builder.getLeaseFailureHandler());
        }
        String threadNamePostfix = "(" + clusterId + ")";
        leaseUpdateThread = new Thread(new BackgroundLeaseUpdate(this, stopLeaseUpdateThread),
                "DocumentNodeStore lease update thread " + threadNamePostfix);
        leaseUpdateThread.setDaemon(true);
        if (!readOnlyMode) {
            // OAK-3398 : make lease updating more robust by ensuring it
            // has higher likelihood of succeeding than other threads
            // on a very busy machine - so as to prevent lease timeout.
            leaseUpdateThread.setPriority(Thread.MAX_PRIORITY);
            leaseUpdateThread.start();
        }

        this.prefetchFeature = builder.getPrefetchFeature();
        this.cancelInvalidationFeature = builder.getCancelInvalidationFeature();
        this.noChildOrderCleanupFeature = builder.getNoChildOrderCleanupFeature();
        this.cacheWarming = new CacheWarming(s);

        this.journalPropertyHandlerFactory = builder.getJournalPropertyHandlerFactory();
        this.store = s;
        this.changes = newJournalEntry();
        this.branches = new UnmergedBranches();
        this.asyncDelay = builder.getAsyncDelay();
        this.versionGarbageCollector = new VersionGarbageCollector(
                this, builder.createVersionGCSupport(), isFullGCEnabled(builder), false,
                isEmbeddedVerificationEnabled(builder), builder.getFullGCMode(), builder.getFullGCDelayFactor(),
                builder.getFullGCBatchSize(), builder.getFullGCProgressSize());
        this.versionGarbageCollector.setStatisticsProvider(builder.getStatisticsProvider());
        this.versionGarbageCollector.setGCMonitor(builder.getGCMonitor());
        this.versionGarbageCollector.setFullGCPaths(builder.getFullGCIncludePaths(), builder.getFullGCExcludePaths());
        this.journalGarbageCollector = new JournalGarbageCollector(
                this, builder.getJournalGCMaxAge());
        this.referencedBlobs =
                builder.createReferencedBlobs(this);
        this.lastRevRecoveryAgent = new LastRevRecoveryAgent(store, this,
                lastRevSeeker, clusterId -> this.signalClusterStateChange());
        this.disableBranches = builder.isDisableBranches();
        this.missing = new DocumentNodeState(this, new Path("missing"),
                new RevisionVector(new Revision(0, 0, 0))) {
            @Override
            public int getMemory() {
                return 8;
            }
        };

        //TODO Make stats collection configurable as it add slight overhead

        nodeCache = builder.buildNodeCache(this);
        nodeCacheStats = new CacheStats(nodeCache, "Document-NodeState",
                builder.getWeigher(), builder.getNodeCacheSize());

        nodeChildrenCache = builder.buildChildrenCache(this);
        nodeChildrenCacheStats = new CacheStats(nodeChildrenCache, "Document-NodeChildren",
                builder.getWeigher(), builder.getChildrenCacheSize());

        diffCache = builder.getDiffCache(this.clusterId);

        // builder checks for feature toggle directly and returns null if disabled
        prevNoPropCache = builder.buildPrevNoPropCache();

        // check if root node exists
        NodeDocument rootDoc = store.find(NODES, Utils.getIdFromPath(ROOT));
        if (rootDoc == null) {
            if (readOnlyMode) {
                throw new DocumentStoreException("Unable to initialize a " +
                        "read-only DocumentNodeStore. The DocumentStore nodes " +
                        "collection does not have a root document.");
            }
            // root node is missing: repository is not initialized
            Revision commitRev = newRevision();
            RevisionVector head = new RevisionVector(commitRev);
            Commit commit = new CommitBuilder(this, commitRev, null)
                    .addNode(ROOT)
                    .build();
            try {
                commit.applyToDocumentStore();
            } catch (ConflictException e) {
                commit.rollback();
                throw new IllegalStateException("Conflict while creating root document", e);
            }
            unsavedLastRevisions.put(ROOT, commitRev);
            sweepRevisions = sweepRevisions.pmax(head);
            setRoot(head);
            // make sure _lastRev is written back to store
            backgroundWrite();
            rootDoc = store.find(NODES, Utils.getIdFromPath(ROOT));
            // at this point the root document must exist
            if (rootDoc == null) {
                throw new IllegalStateException("Root document does not exist");
            }
        } else {
            sweepRevisions = sweepRevisions.pmax(rootDoc.getSweepRevisions());
            initializeRootState(rootDoc);
            // check if _lastRev for our clusterId exists
            if (!rootDoc.getLastRev().containsKey(clusterId)) {
                RevisionVector rootRev = getRoot().getRootRevision();
                Revision initialRev = rootRev.getRevision(clusterId);
                if (initialRev == null) {
                    throw new IllegalStateException(
                            "missing revision for clusterId " + clusterId +
                                    ": " + rootRev);
                }
                unsavedLastRevisions.put(ROOT, initialRev);
                // set initial sweep revision
                sweepRevisions = sweepRevisions.pmax(new RevisionVector(initialRev));
                if (!readOnlyMode) {
                    backgroundWrite();
                }
            }
        }

        checkpoints = new Checkpoints(this);
        // initialize branchCommits
        branches.init(store, this, purgeUncommittedRevisions);

        dispatcher = builder.isPrefetchExternalChanges() ?
                new PrefetchDispatcher(getRoot(), executor) :
                new ChangeDispatcher(getRoot());
        commitQueue = new CommitQueue(this);
        commitQueue.setStatisticsCollector(nodeStoreStatsCollector);
        commitQueue.setSuspendTimeoutMillis(builder.getSuspendTimeoutMillis());
        batchCommitQueue = new BatchCommitQueue(store);
        // prepare background threads
        backgroundReadThread = new Thread(
                new BackgroundReadOperation(this, isDisposed),
                "DocumentNodeStore background read thread " + threadNamePostfix);
        backgroundReadThread.setDaemon(true);
        backgroundPurgeThread = new Thread(
                new BackgroundPurgeOperation(this, isDisposed),
                "DocumentNodeStore background purge thread " + threadNamePostfix);
        backgroundPurgeThread.setDaemon(true);
        backgroundUpdateThread = new Thread(
                new BackgroundUpdateOperation(this, isDisposed),
                "DocumentNodeStore background update thread " + threadNamePostfix);
        backgroundUpdateThread.setDaemon(true);
        backgroundSweepThread = new Thread(
                new BackgroundSweepOperation(this, isDisposed),
                "DocumentNodeStore background sweep thread " + threadNamePostfix);
        backgroundSweepThread.setDaemon(true);
        clusterUpdateThread = new Thread(new BackgroundClusterUpdate(this, isDisposed),
                "DocumentNodeStore cluster update thread " + threadNamePostfix);
        clusterUpdateThread.setDaemon(true);
        // now start the background threads
        clusterUpdateThread.start();
        backgroundReadThread.start();
        if (!readOnlyMode) {
            // OAK-8466 - background sweep may take a long time if there is no
            // sweep revision for this clusterId. When this process is suddenly
            // stopped while performing the sweep, a recovery will be needed
            // starting at the timestamp of _lastRev for this clusterId, which
            // is potentially old and the recovery will be expensive. Hence
            // triggering below function to update _lastRev, just before
            // triggering sweep
            runBackgroundUpdateOperations();

            // check if we need a sweep2 *before* doing a backgroundSweep.
            // this enables us to detect a direct Oak <= 1.6 upgrade situation,
            // where a sweep2 is *not* needed.

            // there are 3 different cases with sweep[1]/sweep2:
            // 1) Oak <= 1.6 direct upgrade:
            //    -> no sweep2 is needed as a sweep1 is needed anyway and sweep2
            //       from now on happens as part of it (with the OAK-9176 fix)
            // 2) Oak >= 1.8 which never did an Oak <= 1.6 upgrade:
            //    -> no sweep2 is needed as OAK-9176 doesn't apply (the repository
            //       never ran <= 1.6)
            // 3) Oak >= 1.8 which was previously doing an Oak <= 1.6 upgrade:
            //    -> A (full) sweep2 is needed. This is the main case of OAK-9176.

            // In case 3 there is a valid, recent "_sweepRev" - and
            // we can go ahead and do a "quick" backgroundSweep() here
            // before continuing, to unblock the startup.
            // After that, an async/background task is started for sweep2.

            // which of cases 1-3 we have is determined via 'sweep2LockIfNecessary'
            // and recorded in the settings collection.

            // except for this case detection (which acquires a "sweep2 lock" if needed)
            // we can otherwise continue normally. That means, a sweep1 can
            // be considered as usual.
            // Note that for case 3, doing this normal sweep1 can now also
            // fix some "_bc" - which before OAK-9176 were missing
            // and which sweep2 would separately fix as well - but this is not a problem.

            // Note that by setting the SYS_PROP_DISABLE_SWEEP2 system property
            // the sweep2 is bypassed and the sweep2 status is explicitly stored as "swept".
            final long sweep2Lock;
            if (disableSweep2) {
                try {
                    final Sweep2StatusDocument sweep2Status = Sweep2StatusDocument.readFrom(store);
                    if (sweep2Status == null || !sweep2Status.isSwept()) {
                        // setting the disableSweep2 flag stores this in the repository
                        Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, clusterId);
                    }
                } catch(Exception e) {
                    LOG.warn("<init> sweep2 is diabled as instructed by system property ("
                            + SYS_PROP_DISABLE_SWEEP2 + "=true) - however, got an Exception"
                                    + " while storing sweep2 status in the settings collection: " + e, e);
                }
                sweep2Lock = -1;
            } else {
                // So: unless sweep2 is disabled: acquire sweep2 if one is (maybe) necessary
                sweep2Lock = Sweep2Helper.acquireSweep2LockIfNecessary(store, clusterId);
            }

            // perform an initial document sweep if needed
            // this may be long running if there is no sweep revision
            // for this clusterId (upgrade from Oak <= 1.6).
            // it is therefore important the lease thread is running already.
            backgroundSweep();

            backgroundUpdateThread.start();
            backgroundSweepThread.start();
            backgroundPurgeThread.start();

            if (sweep2Lock >= 0) {
                // sweep2 is necessary - so start a sweep2 background task
                backgroundSweep2Thread = new Thread(
                        new BackgroundSweep2Operation(this, isDisposed, sweep2Lock),
                        "DocumentNodeStore background sweep2 thread " + threadNamePostfix);
                backgroundSweep2Thread.setDaemon(true);
                backgroundSweep2Thread.start();
            }
        }

        persistentCache = builder.getPersistentCache();
        if (!readOnlyMode && persistentCache != null) {
            DynamicBroadcastConfig broadcastConfig = new DocumentBroadcastConfig(this);
            persistentCache.setBroadcastConfig(broadcastConfig);
        }
        journalCache = builder.getJournalCache();

        this.mbean = createMBean(builder);
        LOG.info("ChangeSetBuilder enabled and size set to maxItems: {}, maxDepth: {}", changeSetMaxItems, changeSetMaxDepth);
        LOG.info("Initialized DocumentNodeStore with clusterNodeId: {}, updateLimit: {} ({})",
                clusterId, updateLimit,
                getClusterNodeInfoDisplayString());
        if (prevNoPropCache == null) {
            LOG.info("prevNoProp cache is disabled");
        } else {
            // unfortunate that the guava cache doesn't unveil its max size
            // hence falling back to using the builder's original value for now.
            LOG.info("prevNoProp cache is enabled with size: " + builder.getPrevNoPropCacheSize());
        }

        if (!builder.isBundlingDisabled()) {
            bundlingConfigHandler.initialize(this, executor);
        }
    }


    public boolean isChildOrderCleanupEnabled() {
        return noChildOrderCleanupFeature == null || !noChildOrderCleanupFeature.isEnabled();
    }

    public void dispose() {
        LOG.info("Starting disposal of DocumentNodeStore with clusterNodeId: {} ({})", clusterId,
                getClusterNodeInfoDisplayString());

        if (isDisposed.getAndSet(true)) {
            // only dispose once
            return;
        }

        // notify background threads waiting on isDisposed
        synchronized (isDisposed) {
            isDisposed.notifyAll();
        }

        Utils.joinQuietly(backgroundReadThread,
                backgroundUpdateThread,
                backgroundSweepThread,
                backgroundSweep2Thread,
                backgroundPurgeThread);

        DocumentStoreException ex = null;

        // create a tombstone commit revision after isDisposed is set to true.
        // commits created earlier than this revision will be able to finish
        // and we'll get a headOfQueue() callback when they are done
        // after this call there are no more pending trunk commits and
        // the commit queue is empty and stays empty
        if (!readOnlyMode) {
            try {
                Revision tombstone = commitQueue.createRevision();
                commitQueue.done(tombstone, new CommitQueue.Callback() {
                    @Override
                    public void headOfQueue(@NotNull Revision revision) {
                        setRoot(getHeadRevision().update(revision));
                        unsavedLastRevisions.put(ROOT, revision);
                    }
                });
            } catch (DocumentStoreException e) {
                LOG.error("dispose: a DocumentStoreException happened during dispose's attempt to commit a tombstone: " + e, e);
                ex = e;
            }
        }

        try {
            bundlingConfigHandler.close();
        } catch (IOException e) {
            LOG.warn("Error closing bundlingConfigHandler", bundlingConfigHandler, e);
        }

        // do a final round of background operations after
        // the background thread stopped
        if (!readOnlyMode) {
            try {
                // force a final background sweep to ensure there are
                // no uncommitted changes up to the current head revision
                internalRunBackgroundSweepOperation();
                // push final changes to store, after this there must not
                // be any more changes to documents in the nodes collection
                internalRunBackgroundUpdateOperations();
            } catch (DocumentStoreException e) {
                // OAK-3250 : when a lease check fails, subsequent modifying requests
                // to the DocumentStore will throw an DocumentStoreException. Since as a result
                // of a failing lease check a bundle.stop is done and thus a dispose of the
                // DocumentNodeStore happens, it is very likely that in that case
                // you run into an Exception. We should still continue with disposing
                // though - thus catching and logging..
                LOG.error("dispose: a DocumentStoreException happened during dispose's last background ops: " + e, e);
                ex = e;
            }
        }

        Utils.joinQuietly(clusterUpdateThread);

        // attempt diagnostics on lease update thread
        boolean isLeaseExpired = clusterNodeInfo.isLeaseExpired(clock.getTime());
        Thread.State leaseUpdateState = leaseUpdateThread.getState();
        if (leaseUpdateState == Thread.State.TERMINATED) {
            LOG.error("leaseUpdateThread (" + leaseUpdateThread.getName() + ") is terminated");
        }

        if (isLeaseExpired || LOG.isDebugEnabled()) {
            StringBuilder message = new StringBuilder(
                    "Status of lease update thread (" + leaseUpdateThread.getName() + "): " + leaseUpdateState + "; Stack Trace:");
            for (StackTraceElement se : leaseUpdateThread.getStackTrace()) {
                message.append("\n\tat ");
                message.append(se.toString());
            }
            if (isLeaseExpired) {
                LOG.info(message.toString());
            } else {
                LOG.debug(message.toString());
            }
        }

        // Stop lease update thread once no further document store operations
        // are required
        LOG.debug("Stopping LeaseUpdate thread...");
        stopLeaseUpdateThread.set(true);
        synchronized (stopLeaseUpdateThread) {
            stopLeaseUpdateThread.notifyAll();
        }
        Utils.joinQuietly(leaseUpdateThread);
        LOG.debug("Stopped LeaseUpdate thread");

        // now mark this cluster node as inactive by disposing the
        // clusterNodeInfo, but only if final background operations
        // were successful
        if (ex == null) {
            clusterNodeInfo.dispose();
        }

        store.dispose();

        try {
            blobStore.close();
        } catch (Exception e) {
            LOG.debug("Error closing blob store " + blobStore, e);
        }

        if (persistentCache != null) {
            persistentCache.close();
        }
        if (journalCache != null) {
            journalCache.close();
        }
        String result = "(successful)";
        if (ex != null) {
            result = "(with exception: " + ex.toString() + ")";
        }
        LOG.info("Disposed DocumentNodeStore with clusterNodeId: {}, {}",
                clusterId, result);
        if (ex != null) {
            throw ex;
        }
    }

    private String getClusterNodeInfoDisplayString() {
        return (readOnlyMode?"readOnly:true, ":"") + clusterNodeInfo.toString().replaceAll("[\r\n\t]", " ").trim();
    }

    void setRoot(@NotNull RevisionVector newHead) {
        checkArgument(!newHead.isBranch());
        root = getRoot(newHead);
    }

    @NotNull
    public DocumentStore getDocumentStore() {
        return store;
    }

    /**
     * Creates a new commit. The caller must acknowledge the commit either with
     * {@link #done(Commit, boolean, CommitInfo)} or {@link #canceled(Commit)},
     * depending on the result of the commit.
     *
     * @param changes the changes to commit.
     * @param base the base revision for the commit or <code>null</code> if the
     *             commit should use the current head revision as base.
     * @param branch the branch instance if this is a branch commit. The life
     *               time of this branch commit is controlled by the
     *               reachability of this parameter. Once {@code branch} is
     *               weakly reachable, the document store implementation is
     *               free to remove the commits associated with the branch.
     * @return a new commit.
     */
    @NotNull
    Commit newCommit(@NotNull Changes changes,
                     @Nullable RevisionVector base,
                     @Nullable DocumentNodeStoreBranch branch) {
        if (base == null) {
            base = getHeadRevision();
        }
        if (base.isBranch()) {
            return newBranchCommit(changes, base, branch);
        } else {
            return newTrunkCommit(changes, base);
        }
    }

    /**
     * Creates a new merge commit. The caller must acknowledge the commit either with
     * {@link #done(Commit, boolean, CommitInfo)} or {@link #canceled(Commit)},
     * depending on the result of the commit.
     *
     * @param base the base revision for the commit.
     * @param numBranchCommits the number of branch commits to merge.
     * @return a new merge commit.
     */
    @NotNull
    private MergeCommit newMergeCommit(@NotNull RevisionVector base, int numBranchCommits) {
        requireNonNull(base);
        backgroundOperationLock.readLock().lock();
        boolean success = false;
        MergeCommit c;
        try {
            checkOpen();
            c = new MergeCommit(this, base, commitQueue.createRevisions(numBranchCommits));
            success = true;
        } finally {
            if (!success) {
                backgroundOperationLock.readLock().unlock();
            }
        }
        return c;
    }

    RevisionVector done(final @NotNull Commit c, boolean isBranch, final @NotNull CommitInfo info) {
        if (commitQueue.contains(c.getRevision())) {
            try {
                inDoubtTrunkCommits.remove(c.getRevision());
                final RevisionVector[] newHead = new RevisionVector[1];
                commitQueue.done(c.getRevision(), new CommitQueue.Callback() {
                    @Override
                    public void headOfQueue(@NotNull Revision revision) {
                        // remember before revision
                        RevisionVector before = getHeadRevision();

                        // update head revision
                        Revision r = c.getRevision();
                        newHead[0] = before.update(r);
                        boolean success = false;
                        boolean cacheUpdated = false;
                        try {
                            // apply lastRev updates
                            c.applyLastRevUpdates(false);

                            // track modified paths
                            changes.modified(c.getModifiedPaths());
                            changes.readFrom(info);
                            changes.addChangeSet(getChangeSet(info));

                            // if we get here all required in-memory changes
                            // have been applied. The following operations in
                            // the try block may fail and the commit can still
                            // be considered successful
                            success = true;

                            // apply changes to cache, based on before revision
                            c.applyToCache(before, false);
                            cacheUpdated = true;
                            if (changes.getNumChangedNodes() >= journalPushThreshold) {
                                LOG.info("Pushing journal entry at {} as number of changes ({}) have reached threshold of {}",
                                        r, changes.getNumChangedNodes(), journalPushThreshold);
                                pushJournalEntry(r);
                            }
                        } catch (Throwable e) {
                            if (success) {
                                if (cacheUpdated) {
                                    LOG.warn("Pushing journal entry at {} failed", revision, e);
                                } else {
                                    LOG.warn("Updating caches at {} failed", revision, e);
                                }
                            } else {
                                LOG.error("Applying in-memory changes at {} failed", revision, e);
                            }
                        } finally {
                            setRoot(newHead[0]);
                            commitQueue.headRevisionChanged();
                            dispatcher.contentChanged(getRoot(), info);
                        }
                    }
                });
                return newHead[0];
            } finally {
                backgroundOperationLock.readLock().unlock();
            }
        } else {
            // branch commit
            try {
                c.applyLastRevUpdates(isBranch);
                c.applyToCache(c.getBaseRevision(), isBranch);
                return c.getBaseRevision().update(c.getRevision().asBranchRevision());
            } finally {
                if (isDisableBranches()) {
                    backgroundOperationLock.readLock().unlock();
                }
            }
        }
    }

    void canceled(Commit c) {
        invalidatePathsOnCancel(c);
        if (commitQueue.contains(c.getRevision())) {
            try {
                commitQueue.canceled(c.getRevision());
                if (c.rollback()) {
                    // rollback was successful
                    inDoubtTrunkCommits.remove(c.getRevision());
                }
            } finally {
                backgroundOperationLock.readLock().unlock();
            }
        } else {
            try {
                c.rollback();
                Branch b = branches.getBranch(c.getBaseRevision());
                if (b != null) {
                    b.removeCommit(c.getRevision().asBranchRevision());
                }
            } finally {
                if (isDisableBranches()) {
                    backgroundOperationLock.readLock().unlock();
                }
            }
        }
    }

    void invalidatePathsOnCancel(Commit c) {
        final boolean cancelInvalidationEnabled = (cancelInvalidationFeature != null
                && cancelInvalidationFeature.isEnabled());
        if (cancelInvalidationLogged == null
                || (cancelInvalidationLogged.booleanValue() != cancelInvalidationEnabled)) {
            // log at info at first use and when toggle changes, for observability
            LOG.info("invalidatePathsOnCancel : cancelInvalidationEnabled = {}",
                    cancelInvalidationEnabled);
            // thread holds the merge lock at this point, no synchronization needed
            cancelInvalidationLogged = cancelInvalidationEnabled;
        }
        if (!cancelInvalidationEnabled) {
            return;
        }
        Iterable<Path> pathsToInvalidate = c.getModifiedPaths();
        if (!pathsToInvalidate.iterator().hasNext()) {
            // nothing to do
            return;
        }
        if (changes != null) {
            // first check if a new journal entry is needed at all
            for (String pri : pendingRollbackInvalidations) {
                JournalEntry je = store.find(JOURNAL, pri);
                if (je == null) {
                    // quite unexpected
                    continue;
                }
                if (je.containsModified(pathsToInvalidate)) {
                    return;
                }
            }
        }
        try {
            // create the invalidation journal entry (branch commit style)
            createInvalidationJournalEntries(pathsToInvalidate, "rollback invalidation", "");
            // but also, already push it now, not waiting for backgroundWrite
            // reason being that otherwise it might get lost on a crash,
            // in which case the peers still would have uncommitted revisions
            // in their nodesCache which might become "committed" on a
            // subsequent fresh parent lastRev change of a future
            // incarnation of this clusterId.
            // thanks to pendingRollbackInvalidations however, this expensive
            // journal entry push is only done once per collision (within
            // a backgroundWrite cycle)
            pushJournalEntry(Revision.newRevision(clusterId));
        } catch(DocumentStoreException e) {
            LOG.error("invalidatePathsOnCancel: " + e.getMessage());
        }
    }

    public void setAsyncDelay(int delay) {
        this.asyncDelay = delay;
    }

    public int getAsyncDelay() {
        return asyncDelay;
    }

    public void setMaxBackOffMillis(int time) {
        maxBackOffMillis = time;
    }

    public int getMaxBackOffMillis() {
        return maxBackOffMillis;
    }

    public int getChangeSetMaxItems() {
        return changeSetMaxItems;
    }

    public void setChangeSetMaxItems(int changeSetMaxItems) {
        this.changeSetMaxItems = changeSetMaxItems;
    }

    public int getChangeSetMaxDepth() {
        return changeSetMaxDepth;
    }

    public void setChangeSetMaxDepth(int changeSetMaxDepth) {
        this.changeSetMaxDepth = changeSetMaxDepth;
    }

    void setEnableConcurrentAddRemove(boolean b) {
        enableConcurrentAddRemove = b;
    }

    boolean getEnableConcurrentAddRemove() {
        return enableConcurrentAddRemove;
    }

    int getJournalPushThreshold() {
        return journalPushThreshold;
    }

    void setJournalPushThreshold(int journalPushThreshold) {
        this.journalPushThreshold = journalPushThreshold;
    }

    @NotNull
    public ClusterNodeInfo getClusterInfo() {
        return clusterNodeInfo;
    }

    public CacheStats getNodeCacheStats() {
        return nodeCacheStats;
    }

    public CacheStats getNodeChildrenCacheStats() {
        return nodeChildrenCacheStats;
    }

    @NotNull
    public Iterable<CacheStats> getDiffCacheStats() {
        return diffCache.getStats();
    }

    public Cache<PathRev, DocumentNodeState> getNodeCache() {
        return nodeCache;
    }

    public Cache<NamePathRev, DocumentNodeState.Children> getNodeChildrenCache() {
        return nodeChildrenCache;
    }

    public Predicate<Path> getNodeCachePredicate() {
        return nodeCachePredicate;
    }

    public Cache<StringValue, StringValue> getPrevNoPropCache() {
        return prevNoPropCache;
    }

    /**
     * Returns the journal entry that will be stored in the journal with the
     * next background updated.
     *
     * @return the current journal entry.
     */
    JournalEntry getCurrentJournalEntry() {
        return changes;
    }

    void invalidateNodeChildrenCache() {
        nodeChildrenCache.invalidateAll();
    }

    void invalidateNodeCache(String path, RevisionVector revision){
        nodeCache.invalidate(new PathRev(Path.fromString(path), revision));
    }

    public int getPendingWriteCount() {
        return unsavedLastRevisions.getPaths().size();
    }

    public boolean isDisableBranches() {
        return disableBranches;
    }

    public long getMaxTimeDiffMillis() {
        return maxTimeDiffMillis;
    }

    /**
     * Enqueue the document with the given id as a split candidate.
     *
     * @param id the id of the document to check if it needs to be split.
     */
    void addSplitCandidate(String id) {
        splitCandidates.put(id, id);
    }

    @Nullable
    AbstractDocumentNodeState getSecondaryNodeState(@NotNull final Path path,
                              @NotNull final RevisionVector rootRevision,
                              @NotNull final RevisionVector rev) {
        //Check secondary cache first
        return nodeStateCache.getDocumentNodeState(path, rootRevision, rev);
    }

    @NotNull
    public PropertyState createPropertyState(String name, String value){
        return DocumentPropertyStateFactory.createPropertyState(this, name, requireNonNull(value));
    }

    /**
     * Get the node for the given path and revision. The returned object might
     * not be modified directly.
     *
     * @param path the path of the node.
     * @param rev the read revision.
     * @return the node or <code>null</code> if the node does not exist at the
     *          given revision.
     */
    @Nullable
    public DocumentNodeState getNode(@NotNull final Path path,
                                     @NotNull final RevisionVector rev) {
        requireNonNull(rev);
        requireNonNull(path);
        final long start = PERFLOG.start();
        try {
            PathRev key = new PathRev(path, rev);
            DocumentNodeState node = nodeCache.get(key, new Callable<DocumentNodeState>() {
                @Override
                public DocumentNodeState call() throws Exception {
                    boolean nodeDoesNotExist = checkNodeNotExistsFromChildrenCache(path, rev);
                    if (nodeDoesNotExist){
                        return missing;
                    }
                    DocumentNodeState n = readNode(path, rev);
                    if (n == null) {
                        n = missing;
                    }
                    return n;
                }
            });
            final DocumentNodeState result = node == missing
                    || node.equals(missing) ? null : node;
            PERFLOG.end(start, 1, "getNode: path={}, rev={}", path, rev);
            return result;
        } catch (UncheckedExecutionException e) {
            throw DocumentStoreException.convert(e.getCause());
        } catch (ExecutionException e) {
            throw DocumentStoreException.convert(e.getCause());
        }
    }

    /**
     * Returns the node for the given path and revision from the cache. This
     * method returns {@code null} if the cache does not have an entry for the
     * node with the given revision.
     * <p>
     * This implementation behaves slightly different from
     * {@link #getNode(Path, RevisionVector)} when the cache knows a node does
     * not exist at a location. This method then returns a
     * {@code DocumentNodeState} which will return false when asked whether it
     * {@link DocumentNodeState#exists()}.
     * 
     * @param path the path of a node state.
     * @param rev the revision of a node state.
     * @return the node state or {@code null} if the cache does not have an
     *          entry for the location and revision.
     */
    @Nullable
    DocumentNodeState getNodeIfCached(@NotNull final Path path,
                                      @NotNull final RevisionVector rev) {
        PathRev key = new PathRev(path, rev);
        DocumentNodeState state = nodeCache.getIfPresent(key);
        if (state == missing) {
            state = DocumentNodeState.newMissingNode(this, path, rev);
        }
        return state;
    }

    @NotNull
    DocumentNodeState.Children getChildren(@NotNull final AbstractDocumentNodeState parent,
                                           @NotNull final String name,
                                           final int limit)
            throws DocumentStoreException {
        if (requireNonNull(parent).hasNoChildren()) {
            return DocumentNodeState.NO_CHILDREN;
        }
        final Path path = requireNonNull(parent).getPath();
        final RevisionVector readRevision = parent.getLastRevision();
        try {
            NamePathRev key = childNodeCacheKey(path, readRevision, name);
            DocumentNodeState.Children children = nodeChildrenCache.get(key, new Callable<DocumentNodeState.Children>() {
                @Override
                public DocumentNodeState.Children call() throws Exception {
                    return readChildren(parent, name, limit);
                }
            });
            if (children.children.size() < limit && children.hasMore) {
                // not enough children loaded - load more,
                // and put that in the cache
                // (not using nodeChildrenCache.invalidate, because
                // the generational persistent cache doesn't support that)
                children = readChildren(parent, name, limit);
                nodeChildrenCache.put(key, children);
            }
            return children;
        } catch (UncheckedExecutionException e) {
            throw DocumentStoreException.convert(e.getCause(),
                    "Error occurred while fetching children for path "
                            + path);
        } catch (ExecutionException e) {
            throw DocumentStoreException.convert(e.getCause(),
                    "Error occurred while fetching children for path "
                            + path);
        }
    }

    /**
     * Read the children of the given parent node state starting at the child
     * node with {@code name}. The given {@code name} is exclusive and will not
     * appear in the list of children. The returned children are sorted in
     * ascending order.
     *
     * @param parent the parent node.
     * @param name the name of the lower bound child node (exclusive) or the
     *              empty {@code String} if no lower bound is given.
     * @param limit the maximum number of child nodes to return.
     * @return the children of {@code parent}.
     */
    DocumentNodeState.Children readChildren(@NotNull AbstractDocumentNodeState parent,
                                            @NotNull String name, int limit) {
        String queriedName = name;
        Path path = parent.getPath();
        RevisionVector rev = parent.getLastRevision();
        LOG.trace("Reading children for [{}] at rev [{}]", path, rev);
        Iterable<NodeDocument> docs;
        DocumentNodeState.Children c = new DocumentNodeState.Children();
        // add one to the requested limit for the raw limit
        // this gives us a chance to detect whether there are more
        // child nodes than requested.
        int rawLimit = (int) Math.min(Integer.MAX_VALUE, ((long) limit) + 1);
        for (;;) {
            docs = readChildDocs(path, name, rawLimit);
            int numReturned = 0;
            for (NodeDocument doc : docs) {
                numReturned++;
                Path p = doc.getPath();
                // remember name of last returned document for
                // potential next round of readChildDocs()
                name = p.getName();
                // filter out deleted children
                DocumentNodeState child = getNode(p, rev);
                if (child == null) {
                    continue;
                }
                if (c.children.size() < limit) {
                    // add to children until limit is reached
                    c.children.add(p.getName());
                } else {
                    // enough collected and we know there are more
                    c.hasMore = true;
                    return c;
                }
            }
            // if we get here we have less than or equal the requested children
            if (numReturned < rawLimit) {
                // fewer documents returned than requested
                // -> no more documents
                c.hasMore = false;
                if (queriedName.isEmpty()) {
                    //we've got to the end of list and we started from the top
                    //This list is complete and can be sorted
                    Collections.sort(c.children);
                }
                return c;
            }
        }
    }

    /**
     * Returns the child documents at the given {@code path} and returns up to
     * {@code limit} documents. The returned child documents are sorted in
     * ascending child node name order. If a {@code name} is passed, the first
     * child document returned is after the given name. That is, the name is the
     * lower exclusive bound.
     *
     * @param path the path of the parent document.
     * @param name the name of the lower bound child node (exclusive) or the
     *              empty {@code String} if no lower bound is given.
     * @param limit the maximum number of child documents to return.
     * @return the child documents.
     */
    @NotNull
    private Iterable<NodeDocument> readChildDocs(@NotNull final Path path,
                                                 @NotNull String name,
                                                 final int limit) {
        final String to = Utils.getKeyUpperLimit(requireNonNull(path));
        final String from;
        if (name.isEmpty()) {
            from = Utils.getKeyLowerLimit(path);
        } else {
            from = Utils.getIdFromPath(new Path(path, name));
        }
        return store.query(Collection.NODES, from, to, limit);
    }

    /**
     * Returns up to {@code limit} child nodes, starting at the given
     * {@code name} (exclusive).
     *
     * @param parent the parent node.
     * @param name the name of the lower bound child node (exclusive) or the
     *             empty {@code String}, if the method should start with the
     *             first known child node.
     * @param limit the maximum number of child nodes to return.
     * @return the child nodes.
     */
    @NotNull
    Iterable<DocumentNodeState> getChildNodes(@NotNull final DocumentNodeState parent,
                                              @NotNull final String name,
                    final int limit) {
        // Preemptive check. If we know there are no children then
        // return straight away
        if (requireNonNull(parent).hasNoChildren()) {
            return Collections.emptyList();
        }

        final RevisionVector readRevision = parent.getLastRevision();
        return transform(getChildren(parent, name, limit).children, new Function<String, DocumentNodeState>() {
            @Override
            public DocumentNodeState apply(String input) {
                Path p = new Path(parent.getPath(), input);
                DocumentNodeState result = getNode(p, readRevision);
                if (result == null) {
                    // This is very unexpected situation - parent's child list
                    // declares the child to exist, while its node state is
                    // null. Let's put some extra effort to do some logging
                    // and invalidate the affected cache entries.
                    String id = Utils.getIdFromPath(p);
                    String cachedDocStr = docAsString(id, true);
                    String uncachedDocStr = docAsString(id, false);
                    nodeCache.invalidate(new PathRev(p, readRevision));
                    nodeChildrenCache.invalidate(childNodeCacheKey(
                            parent.getPath(), readRevision, name));
                    String exceptionMsg = String.format(
                            "Aborting getChildNodes() - DocumentNodeState is null for %s at %s " +
                                    "{\"cachedDoc\":{%s}, \"uncachedDoc\":{%s}}",
                            readRevision, p, cachedDocStr, uncachedDocStr);
                    throw new DocumentStoreException(exceptionMsg);
                }
                return result.withRootRevision(parent.getRootRevision(),
                        parent.isFromExternalChange());
            }

            private String docAsString(String id, boolean cached) {
                try {
                    NodeDocument doc;
                    if (cached) {
                        doc = store.find(Collection.NODES, id);
                    } else {
                        doc = store.find(Collection.NODES, id, 0);
                    }
                    if (doc == null) {
                        return "<null>";
                    } else {
                        return doc.asString();
                    }
                } catch (DocumentStoreException e) {
                    return e.toString();
                }
            }
        }::apply);
    }

    @Nullable
    DocumentNodeState readNode(Path path, RevisionVector readRevision) {
        final long start = PERFLOG.start();
        String id = Utils.getIdFromPath(path);
        Revision lastRevision = getPendingModifications().get(path);
        NodeDocument doc = store.find(Collection.NODES, id);
        if (doc == null) {
            PERFLOG.end(start, 1,
                    "readNode: (document not found) path={}, readRevision={}",
                    path, readRevision);
            return null;
        }
        final DocumentNodeState result = doc.getNodeAtRevision(this, readRevision, lastRevision);
        PERFLOG.end(start, 1, "readNode: path={}, readRevision={}", path, readRevision);
        return result;
    }

    public BundlingConfigHandler getBundlingConfigHandler() {
        return bundlingConfigHandler;
    }

    /**
     * Apply the changes of a node to the cache.
     *
     * @param before the before revision (old head)
     * @param after the after revision (new head)
     * @param rev the commit revision
     * @param path the path
     * @param isNew whether this is a new node
     * @param added the list of added child nodes
     * @param removed the list of removed child nodes
     * @param changed the list of changed child nodes
     *
     */
    void applyChanges(RevisionVector before, RevisionVector after,
                      Revision rev, Path path,
                      boolean isNew, List<Path> added,
                      List<Path> removed, List<Path> changed) {
        if (isNew) {
            // determine the revision for the nodeChildrenCache entry when
            // the node is new. Fallback to after revision in case document
            // is not in the cache. (OAK-4715)
            NodeDocument doc = store.getIfCached(NODES, getIdFromPath(path));
            RevisionVector afterLastRev = after;
            if (doc != null) {
                afterLastRev = new RevisionVector(doc.getLastRev().values());
                afterLastRev = afterLastRev.update(rev);
            }
            if (added.isEmpty()) {
                // this is a leaf node.
                // check if it has the children flag set
                if (doc != null && doc.hasChildren()) {
                    NamePathRev key = childNodeCacheKey(path, afterLastRev, "");
                    LOG.debug("nodeChildrenCache.put({},{})", key, "NO_CHILDREN");
                    nodeChildrenCache.put(key, DocumentNodeState.NO_CHILDREN);
                }
            } else {
                DocumentNodeState.Children c = new DocumentNodeState.Children();
                Set<String> set = new TreeSet<>();
                for (Path p : added) {
                    set.add(p.getName());
                }
                c.children.addAll(set);
                NamePathRev key = childNodeCacheKey(path, afterLastRev, "");
                LOG.debug("nodeChildrenCache.put({},{})", key, c);
                nodeChildrenCache.put(key, c);
            }
        } else {
            // existed before
            DocumentNodeState beforeState = getRoot(before);
            // do we have a cached before state that can be used
            // to calculate the new children?
            int depth = path.getDepth();
            for (int i = 1; i <= depth && beforeState != null; i++) {
                Path p = path.getAncestor(depth - i);
                RevisionVector lastRev = beforeState.getLastRevision();
                PathRev key = new PathRev(p, lastRev);
                beforeState = nodeCache.getIfPresent(key);
                if (missing.equals(beforeState)) {
                    // This is unexpected. The before state should exist.
                    // Invalidate the relevant cache entries. (OAK-6294)
                    LOG.warn("Before state is missing {}. Invalidating " +
                            "affected cache entries.", key);
                    store.invalidateCache(NODES, Utils.getIdFromPath(p));
                    nodeCache.invalidate(key);
                    nodeChildrenCache.invalidate(childNodeCacheKey(path, lastRev, ""));
                    beforeState = null;
                }
            }
            DocumentNodeState.Children children = null;
            if (beforeState != null) {
                if (beforeState.hasNoChildren()) {
                    children = DocumentNodeState.NO_CHILDREN;
                } else {
                    NamePathRev key = childNodeCacheKey(path, beforeState.getLastRevision(), "");
                    children = nodeChildrenCache.getIfPresent(key);
                }
            }
            if (children != null) {
                NamePathRev afterKey = childNodeCacheKey(path, beforeState.getLastRevision().update(rev), "");
                // are there any added or removed children?
                if (added.isEmpty() && removed.isEmpty()) {
                    // simply use the same list
                    LOG.debug("nodeChildrenCache.put({},{})", afterKey, children);
                    nodeChildrenCache.put(afterKey, children);
                } else if (!children.hasMore){
                    // list is complete. use before children as basis
                    Set<String> afterChildren = new TreeSet<>(children.children);
                    for (Path p : added) {
                        afterChildren.add(p.getName());
                    }
                    for (Path p : removed) {
                        afterChildren.remove(p.getName());
                    }
                    DocumentNodeState.Children c = new DocumentNodeState.Children();
                    c.children.addAll(afterChildren);
                    if (c.children.size() <= DocumentNodeState.MAX_FETCH_SIZE) {
                        LOG.debug("nodeChildrenCache.put({},{})", afterKey, c);
                        nodeChildrenCache.put(afterKey, c);
                    } else {
                        LOG.info("not caching more than {} child names for {}",
                                DocumentNodeState.MAX_FETCH_SIZE, path);
                    }
                } else if (added.isEmpty()) {
                    // incomplete list, but we only removed nodes
                    // use linked hash set to retain order
                    Set<String> afterChildren = new LinkedHashSet<>(children.children);
                    for (Path p : removed) {
                        afterChildren.remove(p.getName());
                    }
                    DocumentNodeState.Children c = new DocumentNodeState.Children();
                    c.children.addAll(afterChildren);
                    c.hasMore = true;
                    LOG.debug("nodeChildrenCache.put({},{})", afterKey, c);
                    nodeChildrenCache.put(afterKey, c);
                }
            }
        }
    }

    /**
     * Called when a branch is merged.
     *
     * @param revisions the revisions of the merged branch commits.
     */
    void revisionsMerged(@NotNull Iterable<Revision> revisions) {
        changes.branchCommit(revisions);
    }

    /**
     * Updates a commit root document.
     *
     * @param commit the updates to apply on the commit root document.
     * @param commitRev the commit revision.
     * @return the document before the update was applied or <code>null</code>
     *          if the update failed because of a collision.
     * @throws DocumentStoreException if the update fails with an error.
     */
    @Nullable
    NodeDocument updateCommitRoot(UpdateOp commit, Revision commitRev)
            throws DocumentStoreException {
        // use batch commit when there are only revision and modified updates
        boolean batch = true;
        for (Map.Entry<Key, Operation> op : commit.getChanges().entrySet()) {
            String name = op.getKey().getName();
            if (NodeDocument.isRevisionsEntry(name)
                    || NodeDocument.MODIFIED_IN_SECS.equals(name)) {
                continue;
            }
            batch = false;
            break;
        }
        try {
            if (batch) {
                return batchUpdateCommitRoot(commit);
            } else {
                return store.findAndUpdate(NODES, commit);
            }
        } catch (DocumentStoreException e) {
            return verifyCommitRootUpdateApplied(commit, commitRev, e);
        }
    }

    /**
     * Verifies if the {@code commit} update on the commit root was applied by
     * reading the affected document and checks if the {@code commitRev} is
     * set in the revisions map.
     *
     * @param commit the update operation on the commit root document.
     * @param commitRev the commit revision.
     * @param e the exception that will be thrown when this method determines
     *          that the update was not applied.
     * @return the before document.
     * @throws DocumentStoreException the exception passed to this document
     *      in case the commit update was not applied.
     */
    private NodeDocument verifyCommitRootUpdateApplied(UpdateOp commit,
                                                       Revision commitRev,
                                                       DocumentStoreException e)
            throws DocumentStoreException {
        LOG.info("Update of commit root failed with exception", e);
        int numRetries = 10;
        for (int i = 0; i < numRetries; i++) {
            LOG.info("Checking if change made it to the DocumentStore anyway {}/{} ...",
                    i + 1, numRetries);
            NodeDocument commitRootDoc;
            try {
                commitRootDoc = store.find(NODES, commit.getId(), 0);
            } catch (Exception ex) {
                LOG.info("Failed to read commit root document", ex);
                continue;
            }
            if (commitRootDoc == null) {
                LOG.info("Commit root document missing for {}", commit.getId());
                break;
            }
            if (commitRootDoc.getLocalRevisions().containsKey(commitRev)) {
                LOG.info("Update made it to the store even though the call " +
                        "failed with an exception. Previous exception will " +
                        "be suppressed. {}", commit);
                NodeDocument before = NODES.newDocument(store);
                commitRootDoc.deepCopy(before);
                UpdateUtils.applyChanges(before, commit.getReverseOperation());
                return before;
            }
            break;
        }
        LOG.info("Update didn't make it to the store. Re-throwing the exception");
        throw e;
    }

    private NodeDocument batchUpdateCommitRoot(UpdateOp commit)
            throws DocumentStoreException {
        try {
            return batchCommitQueue.updateDocument(commit).call();
        } catch (InterruptedException e) {
            throw DocumentStoreException.convert(e,
                    "Interrupted while updating commit root document");
        } catch (Exception e) {
            throw DocumentStoreException.convert(e,
                    "Update of commit root document failed");
        }
    }

    /**
     * Returns the root node state at the given revision.
     *
     * @param revision a revision.
     * @return the root node state at the given revision.
     */
    @NotNull
    DocumentNodeState getRoot(@NotNull RevisionVector revision) {
        DocumentNodeState root = getNode(ROOT, revision);
        if (root == null) {
            throw new IllegalStateException(
                    "root node does not exist at revision " + revision);
        }
        return root;
    }

    @NotNull
    DocumentNodeStoreBranch createBranch(DocumentNodeState base) {
        return new DocumentNodeStoreBranch(this, base, mergeLock);
    }

    @NotNull
    RevisionVector rebase(@NotNull RevisionVector branchHead,
                          @NotNull RevisionVector base) {
        requireNonNull(branchHead);
        requireNonNull(base);
        if (disableBranches) {
            return branchHead;
        }
        // TODO conflict handling
        Branch b = getBranches().getBranch(branchHead);
        if (b == null) {
            // empty branch
            return base.asBranchRevision(getClusterId());
        }
        if (b.getBase(branchHead.getBranchRevision()).equals(base)) {
            return branchHead;
        }
        // add a pseudo commit to make sure current head of branch
        // has a higher revision than base of branch
        Revision head = newRevision().asBranchRevision();
        b.rebase(head, base);
        return base.update(head);
    }

    @NotNull
    RevisionVector reset(@NotNull RevisionVector branchHead,
                         @NotNull RevisionVector ancestor) {
        requireNonNull(branchHead);
        requireNonNull(ancestor);
        Branch b = getBranches().getBranch(branchHead);
        if (b == null) {
            throw new DocumentStoreException("Empty branch cannot be reset");
        }
        if (!b.getCommits().last().equals(branchHead.getRevision(getClusterId()))) {
            throw new DocumentStoreException(branchHead + " is not the head " +
                    "of a branch");
        }
        Revision ancestorRev = ancestor.getBranchRevision();
        if (!b.containsCommit(ancestorRev)
                && !b.getBase().asBranchRevision(getClusterId()).equals(ancestor)) {
            throw new DocumentStoreException(ancestor + " is not " +
                    "an ancestor revision of " + branchHead);
        }
        // tailSet is inclusive and will contain the ancestorRev, unless
        // the ancestorRev is the base revision of the branch
        List<Revision> revs = new ArrayList<>();
        if (!b.containsCommit(ancestorRev)) {
            // add before all other branch revisions
            revs.add(ancestorRev);
        }
        revs.addAll(b.getCommits().tailSet(ancestorRev));
        UpdateOp rootOp = new UpdateOp(Utils.getIdFromPath(ROOT), false);
        // reset each branch commit in reverse order
        Map<Path, UpdateOp> operations = new HashMap<>();
        AtomicReference<Revision> currentRev = new AtomicReference<>();
        for (Revision r : reverse(revs)) {
            operations.clear();
            Revision previous = currentRev.getAndSet(r);
            if (previous == null) {
                continue;
            }
            NodeDocument.removeCollision(rootOp, previous.asTrunkRevision());
            NodeDocument.removeRevision(rootOp, previous.asTrunkRevision());
            NodeDocument.removeBranchCommit(rootOp, previous.asTrunkRevision());
            BranchCommit bc = b.getCommit(previous);
            if (bc.isRebase()) {
                continue;
            }
            DocumentNodeState branchState = getRoot(bc.getBase().update(previous));
            DocumentNodeState baseState = getRoot(bc.getBase().update(r));
            LOG.debug("reset: comparing branch {} with base {}",
                    branchState.getRootRevision(), baseState.getRootRevision());
            branchState.compareAgainstBaseState(baseState,
                    new ResetDiff(previous.asTrunkRevision(), operations));
            LOG.debug("reset: applying {} operations", operations.size());
            // apply reset operations
            for (List<UpdateOp> ops : partition(operations.values(), getCreateOrUpdateBatchSize())) {
                store.createOrUpdate(NODES, ops);
            }
        }
        store.findAndUpdate(NODES, rootOp);
        // clean up in-memory branch data
        for (Revision r : revs) {
            if (!r.equals(ancestorRev)) {
                b.removeCommit(r);
            }
        }
        return ancestor;
    }

    @NotNull
    RevisionVector merge(@NotNull RevisionVector branchHead,
                         @NotNull CommitInfo info)
            throws ConflictException, CommitFailedException {
        Branch b = getBranches().getBranch(branchHead);
        RevisionVector base = branchHead;
        if (b != null) {
            base = b.getBase(branchHead.getBranchRevision());
        }
        int numBranchCommits = b != null ? b.getCommits().size() : 1;
        RevisionVector newHead;
        boolean success = false;
        MergeCommit commit = newMergeCommit(base, numBranchCommits);
        try {
            // make branch commits visible
            UpdateOp op = new UpdateOp(Utils.getIdFromPath(ROOT), false);
            NodeDocument.setModified(op, commit.getRevision());
            if (b != null) {
                // check the branch age and fail the commit
                // if the first branch commit is too old
                checkBranchAge(b);

                commit.addBranchCommits(b);
                Iterator<Revision> mergeCommits = commit.getMergeRevisions().iterator();
                for (Revision rev : b.getCommits()) {
                    rev = rev.asTrunkRevision();
                    String commitTag = "c-" + mergeCommits.next();
                    NodeDocument.setRevision(op, rev, commitTag);
                    op.containsMapEntry(NodeDocument.COLLISIONS, rev, false);
                }
                if (store.findAndUpdate(Collection.NODES, op) != null) {
                    // remove from branchCommits map after successful update
                    b.applyTo(getPendingModifications(), commit.getRevision());
                    getBranches().remove(b);
                } else {
                    NodeDocument root = Utils.getRootDocument(store);
                    Set<Revision> conflictRevs = root.getConflictsFor(b.getCommits());
                    String msg = "Conflicting concurrent change. Update operation failed: " + op;
                    throw new ConflictException(msg, conflictRevs);
                }
            } else {
                // no commits in this branch -> do nothing
            }
            newHead = done(commit, false, info);
            success = true;
        } finally {
            if (!success) {
                canceled(commit);
            }
        }
        return newHead;
    }

    /**
     * Compares the given {@code node} against the {@code base} state and
     * reports the differences to the {@link NodeStateDiff}.
     *
     * @param node the node to compare.
     * @param base the base node to compare against.
     * @param diff handler of node state differences
     * @return {@code true} if the full diff was performed, or
     *         {@code false} if it was aborted as requested by the handler
     *         (see the {@link NodeStateDiff} contract for more details)
     */
    @Override
    public boolean compare(@NotNull final AbstractDocumentNodeState node,
                           @NotNull final AbstractDocumentNodeState base,
                           @NotNull NodeStateDiff diff) {
        if (!AbstractNodeState.comparePropertiesAgainstBaseState(node, base, diff)) {
            return false;
        }
        if (node.hasNoChildren() && base.hasNoChildren()) {
            return true;
        }
        return new JsopNodeStateDiffer(diffCache.getChanges(base.getRootRevision(),
                node.getRootRevision(), node.getPath(),
                new DiffCache.Loader() {
                    @Override
                    public String call() {
                        return diffImpl(base, node);
                    }
                })).withoutPropertyChanges().compare(node, base, diff);
    }

    /**
     * Creates a tracker for the given commit revision.
     *
     * @param r a commit revision.
     * @param isBranchCommit whether this is a branch commit.
     * @return a _lastRev tracker for the given commit revision.
     */
    LastRevTracker createTracker(final @NotNull Revision r,
                                 final boolean isBranchCommit) {
        if (isBranchCommit && !disableBranches) {
            Revision branchRev = r.asBranchRevision();
            return branches.getBranchCommit(branchRev);
        } else {
            return new LastRevTracker() {
                @Override
                public void track(Path path) {
                    unsavedLastRevisions.put(path, r);
                }
            };
        }
    }

    /**
     * Suspends until all given revisions are either visible from the current
     * headRevision or canceled from the commit queue.
     *
     * Only revisions from the local cluster node will be considered if the async
     * delay is set to 0.
     *
     * @param conflictRevisions the revision to become visible.
     * @return the suspend time in milliseconds.
     */
    long suspendUntilAll(@NotNull Set<Revision> conflictRevisions) {
        long time = clock.getTime();
        // do not suspend if revision is from another cluster node
        // and background read is disabled
        if (getAsyncDelay() == 0) {
            Set<Revision> onlyLocal = new HashSet<Revision>(conflictRevisions.size());
            for (Revision r : conflictRevisions) {
                if (r.getClusterId() == getClusterId()) {
                    onlyLocal.add(r);
                }
            }
            commitQueue.suspendUntilAll(onlyLocal);
        } else {
            commitQueue.suspendUntilAll(conflictRevisions);
        }
        return clock.getTime() - time;
    }

    //------------------------< Observable >------------------------------------

    @Override
    public Closeable addObserver(Observer observer) {
        return dispatcher.addObserver(observer);
    }

    //-------------------------< NodeStore >------------------------------------

    @NotNull
    @Override
    public DocumentNodeState getRoot() {
        return root;
    }

    @NotNull
    @Override
    public NodeState merge(@NotNull NodeBuilder builder,
                           @NotNull CommitHook commitHook,
                           @NotNull CommitInfo info)
            throws CommitFailedException {
        return asDocumentRootBuilder(builder).merge(commitHook, info);
    }

    @NotNull
    @Override
    public NodeState rebase(@NotNull NodeBuilder builder) {
        return asDocumentRootBuilder(builder).rebase();
    }

    @Override
    public NodeState reset(@NotNull NodeBuilder builder) {
        return asDocumentRootBuilder(builder).reset();
    }

    @Override
    @NotNull
    public BlobStoreBlob createBlob(InputStream inputStream) throws IOException {
        return new BlobStoreBlob(blobStore, blobStore.writeBlob(inputStream));
    }

    /**
     * Returns the {@link Blob} with the given reference. Note that this method is meant to
     * be used with secure reference obtained from Blob#reference which is different from blobId
     *
     * @param reference the reference of the blob.
     * @return the blob.
     */
    @Override
    public Blob getBlob(@NotNull String reference) {
        String blobId = blobStore.getBlobId(reference);
        if(blobId != null){
            return new BlobStoreBlob(blobStore, blobId);
        }
        LOG.debug("No blobId found matching reference [{}]", reference);
        return null;
    }

    /**
     * Returns the {@link Blob} with the given blobId.
     *
     * @param blobId the blobId of the blob.
     * @return the blob.
     */
    public Blob getBlobFromBlobId(String blobId){
        return new BlobStoreBlob(blobStore, blobId);
    }

    @NotNull
    @Override
    public String checkpoint(long lifetime, @NotNull Map<String, String> properties) {
        checkOpen();
        return checkpoints.create(lifetime, properties).toString();
    }

    @NotNull
    @Override
    public String checkpoint(long lifetime) {
        checkOpen();
        Map<String, String> empty = Collections.emptyMap();
        return checkpoint(lifetime, empty);
    }

    @NotNull
    @Override
    public Map<String, String> checkpointInfo(@NotNull String checkpoint) {
        checkOpen();
        Revision r = Revision.fromString(checkpoint);
        Checkpoints.Info info = checkpoints.getCheckpoints().get(r);
        if (info == null) {
            // checkpoint does not exist
            return Collections.emptyMap();
        } else {
            return info.get();
        }
    }

    @NotNull
    @Override
    public Iterable<String> checkpoints() {
        checkOpen();
        final long now = clock.getTime();
        return Iterables.transform(Iterables.filter(checkpoints.getCheckpoints().entrySet(),
                cp -> cp.getValue().getExpiryTime() > now),
                cp -> cp.getKey().toString());
    }

    @Nullable
    @Override
    public NodeState retrieve(@NotNull String checkpoint) {
        checkOpen();
        RevisionVector rv = getCheckpoints().retrieve(checkpoint);
        if (rv == null) {
            return null;
        }
        // make sure all changes up to checkpoint are visible
        suspendUntilAll(CollectionUtils.toSet(rv));
        return getRoot(rv);
    }

    @Override
    public boolean release(@NotNull String checkpoint) {
        checkOpen();
        checkpoints.release(checkpoint);
        return true;
    }

    //------------------------< RevisionContext >-------------------------------

    @Override
    public UnmergedBranches getBranches() {
        return branches;
    }

    @Override
    public UnsavedModifications getPendingModifications() {
        return unsavedLastRevisions;
    }

    @Override
    public int getClusterId() {
        return clusterId;
    }

    @Override
    @NotNull
    public RevisionVector getHeadRevision() {
        return root.getRootRevision();
    }

    @Override
    @NotNull
    public Revision newRevision() {
        if (simpleRevisionCounter != null) {
            return new Revision(simpleRevisionCounter.getAndIncrement(), 0, clusterId);
        }
        Revision r = Revision.newRevision(clusterId);
        if (clusterNodeInfo.getLeaseCheckMode() == LeaseCheckMode.STRICT) {
            // verify revision timestamp is within lease period
            long leaseEnd = clusterNodeInfo.getLeaseEndTime();
            if (r.getTimestamp() >= leaseEnd) {
                String msg = String.format("Cannot use new revision %s " +
                        "with timestamp %s >= lease end %s",
                        r, r.getTimestamp(), leaseEnd);
                throw new DocumentStoreException(msg);
            }
        }
        return r;
    }

    @Override
    @NotNull
    public Clock getClock() {
        return clock;
    }

    @Override
    public String getCommitValue(@NotNull Revision changeRevision,
                                 @NotNull NodeDocument doc) {
        return commitValueResolver.resolve(changeRevision, doc);
    }

    //----------------------< background operations >---------------------------

    /** Used for testing only */
    public void runBackgroundOperations() {
        runBackgroundSweepOperation();
        runBackgroundUpdateOperations();
        runBackgroundReadOperations();
    }

    /** Note: made package-protected for testing purpose, would otherwise be private **/
    void runBackgroundUpdateOperations() {
        if (readOnlyMode || isDisposed.get()) {
            return;
        }
        try {
            internalRunBackgroundUpdateOperations();
        } catch (RuntimeException e) {
            if (isDisposed.get()) {
                return;
            }
            LOG.warn("Background update operation failed (will be retried with next run): " + e.toString(), e);
            throw e;
        }
    }

    private void internalRunBackgroundUpdateOperations() {
        BackgroundWriteStats stats;
        synchronized (backgroundWriteMonitor) {
            long start = clock.getTime();
            long time = start;
            // clean orphaned branches and collisions
            cleanOrphanedBranches();
            cleanRootCollisions();
            long cleanTime = clock.getTime() - time;
            time = clock.getTime();
            // split documents (does not create new revisions)
            backgroundSplit();
            long splitTime = clock.getTime() - time;
            time = clock.getTime();
            maybeRefreshHeadRevision();
            long refreshTime = clock.getTime() - time;
            // write back pending updates to _lastRev
            stats = backgroundWrite();
            stats.refresh = refreshTime;
            stats.split = splitTime;
            stats.clean = cleanTime;
            stats.totalWriteTime = clock.getTime() - start;
            String msg = "Background operations stats ({})";
            logBackgroundOperation(start, msg, stats);
        }
        //Push stats outside of sync block
        nodeStoreStatsCollector.doneBackgroundUpdate(stats);
    }

    //----------------------< background read operations >----------------------

    /** Note: made package-protected for testing purpose, would otherwise be private **/
    void runBackgroundReadOperations() {
        if (isDisposed.get()) {
            return;
        }
        try {
            internalRunBackgroundReadOperations();
        } catch (RuntimeException e) {
            if (isDisposed.get()) {
                return;
            }
            LOG.warn("Background read operation failed: " + e.toString(), e);
            throw e;
        }
    }

    /** OAK-2624 : background read operations are split from background update ops */
    private void internalRunBackgroundReadOperations() {
        BackgroundReadStats readStats = null;
        synchronized (backgroundReadMonitor) {
            long start = clock.getTime();
            // pull in changes from other cluster nodes
            readStats = backgroundRead();
            readStats.totalReadTime = clock.getTime() - start;
            String msg = "Background read operations stats (read:{} {})";
            logBackgroundOperation(start, msg, readStats.totalReadTime, readStats);
        }
        nodeStoreStatsCollector.doneBackgroundRead(readStats);
    }

    //----------------------< background sweep operation >----------------------

    void runBackgroundSweepOperation() {
        if (readOnlyMode || isDisposed.get()) {
            return;
        }
        try {
            internalRunBackgroundSweepOperation();
        } catch (RuntimeException e) {
            if (isDisposed.get()) {
                return;
            }
            LOG.warn("Background sweep operation failed (will be retried with next run): " + e.toString(), e);
            throw e;
        }
    }

    private void internalRunBackgroundSweepOperation()
            throws DocumentStoreException {
        BackgroundWriteStats stats = new BackgroundWriteStats();
        synchronized (backgroundSweepMonitor) {
            long start = clock.getTime();
            stats.num = backgroundSweep();
            stats.sweep = clock.getTime() - start;
            String msg = "Background sweep operation stats ({})";
            logBackgroundOperation(start, msg, stats);
        }
        nodeStoreStatsCollector.doneBackgroundUpdate(stats);
    }

    /**
     * Log a background operation with the given {@code startTime}. The
     * {@code message} is logged at INFO if more than 10 seconds passed since
     * the {@code startTime}, otherwise at DEBUG.
     *
     * @param startTime time when the background operation started.
     * @param message   the log message.
     * @param arguments the arguments for the log message.
     */
    private void logBackgroundOperation(long startTime,
                                        String message,
                                        Object... arguments) {
        if (clock.getTime() - startTime > SECONDS.toMillis(10)) {
            // log as info if it took more than 10 seconds
            LOG.info(message, arguments);
        } else {
            LOG.debug(message, arguments);
        }
    }

    /**
     * Renews the cluster lease if necessary.
     *
     * @return {@code true} if the lease was renewed; {@code false} otherwise.
     */
    boolean renewClusterIdLease() {
        Stopwatch sw = Stopwatch.createStarted();
        boolean renewedOrException = true;
        try {
            renewedOrException = clusterNodeInfo.renewLease();
        } finally {
            // we need to collect the stats if,
            // 1. Either lease had been renewed
            // 2. or there is exception while renewing the lease i.e lease renewal failed/timed out
            // In case lease is not renewed (can happen if it had not expired), we don't collect the stats
            if (renewedOrException) {
                nodeStoreStatsCollector.doneLeaseUpdate(sw.elapsed(MICROSECONDS));
            }
        }
        return renewedOrException;
    }

    /**
     * Updates the state about cluster nodes in {@link #clusterNodes}.
     *
     * @return true if the cluster state has changed, false if the cluster state
     * remained unchanged
     */
    boolean updateClusterState() {
        boolean hasChanged = false;
        Set<Integer> clusterIds = new HashSet<>();
        for (ClusterNodeInfoDocument doc : ClusterNodeInfoDocument.all(nonLeaseCheckingStore)) {
            int cId = doc.getClusterId();
            clusterIds.add(cId);
            ClusterNodeInfoDocument old = clusterNodes.get(cId);
            // do not replace document for inactive cluster node
            // in order to keep the created timestamp of the document
            // for the time when the cluster node was first seen inactive
            if (old != null && !old.isActive() && !doc.isActive()) {
                continue;
            }
            clusterNodes.put(cId, doc);
            if (old == null || old.isActive() != doc.isActive()) {
                hasChanged = true;
            }
        }
        hasChanged |= clusterNodes.keySet().retainAll(clusterIds);
        return hasChanged;
    }

    /**
     * @return the minimum revisions of foreign cluster nodes since they were
     *          started. The revision is derived from the start time of the
     *          cluster node.
     */
    @NotNull
    private RevisionVector getMinExternalRevisions() {
        return Utils.getStartRevisions(clusterNodes.values()).remove(getClusterId());
    }

    /**
     * Perform a background read and make external changes visible.
     */
    private BackgroundReadStats backgroundRead() {
        return new ExternalChange(this) {
            @Override
            void invalidateCache(@NotNull Iterable<String> paths) {
                stats.cacheStats = store.invalidateCache(pathToId(paths));
            }

            @Override
            void invalidateCache() {
                stats.cacheStats = store.invalidateCache();
            }

            @Override
            void updateHead(@NotNull Set<Revision> externalChanges,
                            @NotNull RevisionVector sweepRevs,
                            @Nullable Iterable<String> changedPaths) {
                long time = clock.getTime();
                // make sure no local commit is in progress
                backgroundOperationLock.writeLock().lock();
                try {
                    stats.lock = clock.getTime() - time;

                    RevisionVector oldHead = getHeadRevision();
                    RevisionVector newHead = oldHead;
                    for (Revision r : externalChanges) {
                        newHead = newHead.update(r);
                    }
                    setRoot(newHead);
                    // update sweep revisions
                    sweepRevisions = sweepRevisions.pmax(sweepRevs);

                    commitQueue.headRevisionChanged();
                    time = clock.getTime();
                    if (changedPaths != null) {
                        // then there were external changes and reading them
                        // was successful -> apply them to the diff cache
                        try {
                            JournalEntry.applyTo(changedPaths, diffCache,
                                    ROOT, oldHead, newHead);
                        } catch (Exception e1) {
                            LOG.error("backgroundRead: Exception while processing external changes from journal: " + e1, e1);
                        }
                    }
                    stats.populateDiffCache = clock.getTime() - time;
                    time = clock.getTime();

                    ChangeSet changeSet = getChangeSetBuilder().build();
                    LOG.debug("Dispatching external change with ChangeSet {}", changeSet);
                    dispatcher.contentChanged(getRoot().fromExternalChange(),
                            newCommitInfo(changeSet, getJournalPropertyHandler()));
                } finally {
                    backgroundOperationLock.writeLock().unlock();
                }
                stats.dispatchChanges = clock.getTime() - time;
            }
        }.process();
    }

    private static CommitInfo newCommitInfo(@NotNull ChangeSet changeSet, JournalPropertyHandler journalPropertyHandler) {
        CommitContext commitContext = new SimpleCommitContext();
        commitContext.set(COMMIT_CONTEXT_OBSERVATION_CHANGESET, changeSet);
        journalPropertyHandler.addTo(commitContext);
        Map<String, Object> info = Map.of(CommitContext.NAME, commitContext);
        return new CommitInfo(CommitInfo.OAK_UNKNOWN, CommitInfo.OAK_UNKNOWN, info, true);
    }

    private static ChangeSet getChangeSet(CommitInfo info) {
        CommitContext commitContext = (CommitContext) info.getInfo().get(CommitContext.NAME);
        if (commitContext == null){
            return null;
        }
        return (ChangeSet) commitContext.get(COMMIT_CONTEXT_OBSERVATION_CHANGESET);
    }

    private ChangeSetBuilder newChangeSetBuilder() {
        return new ChangeSetBuilder(changeSetMaxItems, changeSetMaxDepth);
    }

    private void cleanOrphanedBranches() {
        Branch b;
        while ((b = branches.pollOrphanedBranch()) != null) {
            LOG.debug("Cleaning up orphaned branch with base revision: {}, " + 
                    "commits: {}", b.getBase(), b.getCommits());
            UpdateOp op = new UpdateOp(Utils.getIdFromPath(ROOT), false);
            for (Revision r : b.getCommits()) {
                r = r.asTrunkRevision();
                NodeDocument.removeRevision(op, r);
                NodeDocument.removeBranchCommit(op, r);
            }
            store.findAndUpdate(NODES, op);
        }
    }

    private void cleanRootCollisions() {
        String id = Utils.getIdFromPath(ROOT);
        NodeDocument root = store.find(NODES, id);
        if (root != null) {
            cleanCollisions(root, Integer.MAX_VALUE);
        }
    }

    private void cleanCollisions(NodeDocument doc, int limit) {
        RevisionVector head = getHeadRevision();
        Map<Revision, String> map = doc.getLocalMap(NodeDocument.COLLISIONS);
        UpdateOp op = new UpdateOp(doc.getId(), false);
        for (Revision r : map.keySet()) {
            if (r.getClusterId() == clusterId) {
                // remove collision if there is no active branch with
                // this revision and the revision is before the current
                // head. That is, the collision cannot be related to commit
                // which is progress.
                if (branches.getBranchCommit(r) == null 
                        && !head.isRevisionNewer(r)) {
                    NodeDocument.removeCollision(op, r);
                    if (--limit <= 0) {
                        break;
                    }
                }
            }
        }
        if (op.hasChanges()) {
            LOG.debug("Removing collisions {} on {}",
                    op.getChanges().keySet(), doc.getId());
            store.findAndUpdate(NODES, op);
        }
    }

    private void backgroundSplit() {
        final int initialCapacity = getCreateOrUpdateBatchSize() + 4;
        Set<Path> invalidatedPaths = new HashSet<>(initialCapacity);
        Set<Path> pathsToInvalidate = new HashSet<>(initialCapacity);
        RevisionVector head = getHeadRevision();
        // OAK-9149 : With backgroundSplit being done in batches, the
        // updateOps must be executed in "phases".
        // Reason being that the (DocumentStore) batch calls
        // are not atomic. That means they could potentially
        // be partially executed only - without any guarantees on
        // which part is executed and which not.
        // The split algorithm, however, requires that
        // a part of the operations, namely intermediate/garbage/split ops,
        // are executed *before* the main document is updated.
        // In order to reflect this necessity in the batch variant,
        // all those intermediate/garbage/split updateOps are grouped
        // into a first phase - and the main document updateOps in a second phase.
        // That way, if the first phase fails, partially, the main documents
        // are not yet touched.
        // TODO but if the split fails, we create actual garbage that cannot
        // be cleaned up later, since there is no "pointer" to it. That's
        // something to look at/consider at some point.

        // phase1 therefore only contains intermediate/garbage/split updateOps
        List<UpdateOp> splitOpsPhase1 = new ArrayList<>(initialCapacity);
        // phase2 contains main document updateOps.
        List<UpdateOp> splitOpsPhase2 = new ArrayList<>(initialCapacity);
        List<String> removeCandidates = new ArrayList<>(initialCapacity);
        for (String id : splitCandidates.keySet()) {
            removeCandidates.add(id);
            NodeDocument doc = store.find(Collection.NODES, id);
            if (doc == null) {
                continue;
            }
            cleanCollisions(doc, collisionGarbageBatchSize);
            Iterator<UpdateOp> it = doc.split(this, head, input -> getBinarySize(input)).iterator();
            while(it.hasNext()) {
                UpdateOp op = it.next();
                Path path = doc.getPath();
                // add an invalidation journal entry, unless the path
                // already has a pending _lastRev update or an invalidation
                // entry was already added in this backgroundSplit() call
                if (unsavedLastRevisions.get(path) == null && !invalidatedPaths.contains(path)) {
                    pathsToInvalidate.add(path);
                }
                // the last entry is the main document update
                // (as per updated NodeDocument.split documentation).
                if (it.hasNext()) {
                    splitOpsPhase1.add(op);
                } else {
                    splitOpsPhase2.add(op);
                }
            }
            if (splitOpsPhase1.size() >= getCreateOrUpdateBatchSize()
                    || splitOpsPhase2.size() >= getCreateOrUpdateBatchSize()) {
                invalidatePaths(pathsToInvalidate);
                batchSplit(splitOpsPhase1);
                batchSplit(splitOpsPhase2);
                invalidatedPaths.addAll(pathsToInvalidate);
                pathsToInvalidate.clear();
                splitOpsPhase1.clear();
                splitOpsPhase2.clear();
                splitCandidates.keySet().removeAll(removeCandidates);
                removeCandidates.clear();
            }
        }

        if (splitOpsPhase1.size() + splitOpsPhase2.size() > 0) {
            invalidatePaths(pathsToInvalidate);
            batchSplit(splitOpsPhase1);
            batchSplit(splitOpsPhase2);
        }
        splitCandidates.keySet().removeAll(removeCandidates);
    }

    private void invalidatePaths(@NotNull Set<Path> pathsToInvalidate) {
        if (pathsToInvalidate.isEmpty()) {
            // nothing to do
            return;
        }
        createInvalidationJournalEntries(pathsToInvalidate, "split",
                " Will be retried with next background split operation.");
    }

    private void createInvalidationJournalEntries(@NotNull Iterable<Path> pathsToInvalidate, String type,
            String errorSuffixMsg) {
        // create journal entry for cache invalidation
        JournalEntry entry = JOURNAL.newDocument(getDocumentStore());
        entry.modified(pathsToInvalidate);
        Revision r = newRevision().asBranchRevision();
        UpdateOp journalOp = entry.asUpdateOp(r);
        if (store.create(JOURNAL, singletonList(journalOp))) {
            changes.invalidate(singletonList(r));
            LOG.debug("Journal entry {} created for {} of document(s) {}",
                    journalOp.getId(), type, pathsToInvalidate);
        } else {
            String msg = "Unable to create journal entry " +
                    journalOp.getId() + " for document invalidation. " +
                    errorSuffixMsg;
            throw new DocumentStoreException(msg);
        }
    }

    private void batchSplit(@NotNull List<UpdateOp> splitOps) {
        if (splitOps.isEmpty()) {
            // nothing to do
            return;
        }
        // apply the split operations
        List<NodeDocument> beforeList = store.createOrUpdate(Collection.NODES, splitOps);
        if (LOG.isDebugEnabled()) {
            // this is rather expensive - but given we were doing log.debug before
            // the batchSplit mechanism, so this somewhat negates the batch improvement indeed
            for (int i = 0; i < splitOps.size(); i++) {
                UpdateOp op = splitOps.get(i);
                NodeDocument before = beforeList.size() > i ? beforeList.get(i) : null;
                if (before != null) {
                    NodeDocument after = store.find(Collection.NODES, op.getId());
                    if (after != null) {
                        LOG.debug("Split operation on {}. Size before: {}, after: {}",
                                op.getId(), before.getMemory(), after.getMemory());
                    }
                } else {
                    LOG.debug("Split operation created {}", op.getId());
                }
            }
        }
    }

    @NotNull
    Set<String> getSplitCandidates() {
        return Collections.unmodifiableSet(splitCandidates.keySet());
    }

    @NotNull
    RevisionVector getSweepRevisions() {
        return sweepRevisions;
    }

    int getCreateOrUpdateBatchSize() {
        return createOrUpdateBatchSize;
    }

    //-----------------------------< internal >---------------------------------

    private BackgroundWriteStats backgroundWrite() {
        return unsavedLastRevisions.persist(getDocumentStore(),
                new Supplier<Revision>() {
            @Override
            public Revision get() {
                return getSweepRevisions().getRevision(getClusterId());
            }
        }, new UnsavedModifications.Snapshot() {
            @Override
            public void acquiring(Revision mostRecent) {
                pendingRollbackInvalidations.clear();
                pushJournalEntry(mostRecent);
            }
        }, backgroundOperationLock.writeLock());
    }

    private void maybeRefreshHeadRevision() {
        if (isDisposed.get() || isDisableBranches()) {
            return;
        }
        DocumentNodeState rootState = getRoot();
        // check if local head revision is outdated and needs an update
        // this ensures the head and sweep revisions are recent and the
        // revision garbage collector can remove old documents
        Revision head = rootState.getRootRevision().getRevision(clusterId);
        Revision lastRev = rootState.getLastRevision().getRevision(clusterId);
        long oneMinuteAgo = clock.getTime() - ONE_MINUTE_MS;
        if ((head != null && head.getTimestamp() < oneMinuteAgo) ||
                (lastRev != null && lastRev.getTimestamp() < oneMinuteAgo)) {
            // head was not updated for more than a minute
            // create an empty commit that updates the head
            boolean success = false;
            Commit c = newTrunkCommit(nop -> {}, getHeadRevision());
            try {
                c.markChanged(ROOT);
                done(c, false, CommitInfo.EMPTY);
                success = true;
            } finally {
                if (!success) {
                    canceled(c);
                }
            }
        }

    }

    /**
     * Executes the sweep2 (from within a background thread)
     * @param sweep2Lock the lock value originally acquired
     * @return true if sweep2 is done or no longer needed, false otherwise (in which case it should be retried)
     * @throws DocumentStoreException
     */
    boolean backgroundSweep2(long sweep2Lock) throws DocumentStoreException {
        if (sweep2Lock == 0) {
            sweep2Lock = Sweep2Helper.acquireSweep2LockIfNecessary(store, clusterId);
            if (sweep2Lock == 0) {
                // still not well defined, retry in a minute (done in BackgroundSweep2Operation)
                return false;
            }
            if (sweep2Lock == -1) {
                // then we're done
                return true;
            }
        }
        // sweep2Lock > 0, the local instance holds the lock
        Sweep2StatusDocument statusDoc = Sweep2StatusDocument.readFrom(store);
        if (statusDoc != null /*should never be null as we hold the lock, but let's play safe anyway .. */
                && statusDoc.isChecking()) {
            // very likely no 'isSweep2Necessary' might not have been done yet, let's do it now
            LOG.info("backgroundSweep2: checking whether sweep2 is necessary...");
            if (!Sweep2Helper.isSweep2Necessary(store)) {
                LOG.info("backgroundSweep2: sweep2 check determined a sweep2 is NOT necessary. Marking sweep2 status as 'swept'.");
                if (!Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, clusterId)) {
                    LOG.error("backgroundSweep2 : failed to update the sweep2 status to 'swept'. Aborting sweep2 for now.");
                }
                return true;
            }
            LOG.info("backgroundSweep2: sweep2 check determined a sweep2 IS necessary. Marking sweep2 status as 'sweeping'.");
        }
        // update the lock status one more time to ensure no check can conclude sweep2 is not necessary anymore
        sweep2Lock = Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, clusterId,
                true /* this true here is what's relevant: locks-in the 'sweeping' status */);
        if (sweep2Lock == 0) {
            // something came in between, retry later
            LOG.info("backgroundSweep2: could not update the sweep2 lock to sweeping, someone got in between. Retry later.");
            return false;
        } else if (sweep2Lock == -1) {
            // odd, someone else concluded we're done
            LOG.info("backgroundSweep2: meanwhile, someone else concluded sweep2 is done.");
            return true;
        }

        // compile the list of clusterIds for which sweep2 should be done.
        // in an ideal situation sweep(1) would have been done for all clusterIds,
        // and in that case "_sweepRev" will contain all clusterIds ever used.
        // However, it is a supported situation that not sweep(1) was not run for all clusterIds,
        // and in this case sweep2 must also not be done for all, but only for those clusterIds.
        List<Integer> includedClusterIds = new LinkedList<>();
        for (Revision aSweepRev : sweepRevisions) {
            includedClusterIds.add(aSweepRev.getClusterId());
        }

        // at this point we did properly acquire a lock and can go ahead doing sweep2
        LOG.info("backgroundSweep2: starting sweep2 (includedClusterIds={})", includedClusterIds);
        int num = forceBackgroundSweep2(includedClusterIds);
        LOG.info("backgroundSweep2: finished sweep2, num swept=" + num);

        // release the lock.
        // Note that in theory someone else could have released our lock, or that
        // the sweep2 status was deleted - that actually doesn't matter:
        // we just went through a full, successful sweep2 and we want to record it
        // that way - irrespective of any interference with the status
        // -> hence the 'force' aspect of releasing here
        if (!Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, clusterId)) {
            LOG.error("backgroundSweep2 : sweep2 finished but we failed to update the sweep2 status accordingly");
        }
        return true;
    }

    /**
     * Executes a sweep2 either only for the provided or for all clusterIds otherwise
     * (which case applies depends on the status of sweep(1) in the repository).
     * @param includedClusterIds restrict sweep2 to only these clusterIds - or do it for
     * all clusterIds if this list is empty or null.
     * @return number of documents swept
     * @throws DocumentStoreException
     */
    int forceBackgroundSweep2(List<Integer> includedClusterIds) throws DocumentStoreException {
        final RevisionVector emptySweepRevision = new RevisionVector();
        CommitValueResolver cvr = new CachingCommitValueResolver(
                0 /* disable caching for sweep2 as caching has a risk of propagating wrong values */,
                () -> emptySweepRevision);
        MissingBcSweeper2 sweeper = new MissingBcSweeper2(this, cvr, includedClusterIds, isDisposed);
        LOG.info("Starting document sweep2. Head: {}, starting at 0", getHeadRevision());
        Iterable<NodeDocument> docs = lastRevSeeker.getCandidates(0);
        try {
            final AtomicInteger numUpdates = new AtomicInteger();

            sweeper.sweep2(docs, new NodeDocumentSweepListener() {
                @Override
                public void sweepUpdate(final Map<Path, UpdateOp> updates)
                        throws DocumentStoreException {
                    // create a synthetic commit. this commit does not have any
                    // changes, we just use it to create a journal entry for
                    // cache invalidation and apply the sweep updates
                    backgroundOperationLock.readLock().lock();
                    try {
                        boolean success = false;
                        Revision r = commitQueue.createRevision();
                        try {
                            commitQueue.done(r, new CommitQueue.Callback() {
                                @Override
                                public void headOfQueue(@NotNull Revision revision) {
                                    writeUpdates(updates, revision);
                                }
                            });
                            success = true;
                        } finally {
                            if (!success && commitQueue.contains(r)) {
                                commitQueue.canceled(r);
                            }
                        }
                    } finally {
                        backgroundOperationLock.readLock().unlock();
                    }
                }

                private void writeUpdates(Map<Path, UpdateOp> updates,
                                          Revision revision)
                        throws DocumentStoreException {
                    // create journal entry
                    JournalEntry entry = JOURNAL.newDocument(getDocumentStore());
                    entry.modified(updates.keySet());
                    Revision r = newRevision().asBranchRevision();
                    if (!store.create(JOURNAL, singletonList(entry.asUpdateOp(r)))) {
                        String msg = "Unable to create journal entry for " +
                                "document invalidation. Will be retried with " +
                                "next background sweep2 operation.";
                        throw new DocumentStoreException(msg);
                    }
                    changes.invalidate(Collections.singleton(r));
                    unsavedLastRevisions.put(ROOT, revision);
                    RevisionVector newHead = getHeadRevision().update(revision);
                    setRoot(newHead);
                    commitQueue.headRevisionChanged();

                    store.createOrUpdate(NODES, new ArrayList<>(updates.values()));
                    numUpdates.addAndGet(updates.size());
                    LOG.debug("Background sweep2 updated {}", updates.keySet());
                }
            });

            return numUpdates.get();
        } finally {
            Utils.closeIfCloseable(docs);
        }
    }

    private int backgroundSweep() throws DocumentStoreException {
        // decide if it needs to run
        Revision head = getHeadRevision().getRevision(clusterId);
        if (head == null) {
            // should never happen because the head revision vector
            // always contains an element for the current clusterId,
            // unless we are in read-only mode
            return 0;
        }

        // are there in-doubt commit revisions that are older than
        // the current head revision?
        SortedSet<Revision> garbage = new TreeSet<>(StableRevisionComparator.INSTANCE);
        for (Revision r : inDoubtTrunkCommits) {
            if (r.compareRevisionTime(head) < 0) {
                garbage.add(r);
            }
        }
        // skip if there is no garbage and we have at least some sweep
        // revision for the local clusterId. A sweep is needed even
        // without garbage when an upgrade happened and no sweep revision
        // exists for the local clusterId
        Revision sweepRev = sweepRevisions.getRevision(clusterId);
        if (garbage.isEmpty() && sweepRev != null) {
            updateSweepRevision(head);
            return 0;
        }

        Revision startRev = new Revision(0, 0, clusterId);
        if (!garbage.isEmpty()) {
            startRev = garbage.first();
        }

        String reason = "";
        if (!garbage.isEmpty()) {
            reason = garbage.size() + " garbage revision(s)";
        }
        if (sweepRev == null) {
            if (! reason.isEmpty()) {
                reason += ", ";
            }
            reason += "no sweepRevision for " + clusterId;
        }

        int num = forceBackgroundSweep(startRev, reason);
        inDoubtTrunkCommits.removeAll(garbage);
        return num;
    }

    private void purgeUnmergedBranchCommitAndCollisionMarkers(final ClusterNodeInfoDocument cluster) {
        branches.purgeUnmergedBranchCommitAndCollisionMarkers(store, cluster.getClusterId(), purgeUncommittedRevisions,
                cluster.isOlderThanLastWrittenRootRevPredicate());
    }

    private int forceBackgroundSweep(Revision startRev, String reason) throws DocumentStoreException {
        NodeDocumentSweeper sweeper = new NodeDocumentSweeper(this, false);
        LOG.info("Starting document sweep. Head: {}, starting at {} (reason: {})",
                sweeper.getHeadRevision(), startRev, reason);
        Iterable<NodeDocument> docs = lastRevSeeker.getCandidates(startRev.getTimestamp());
        try {
            final AtomicInteger numUpdates = new AtomicInteger();

            Revision newSweepRev = sweeper.sweep(docs, new NodeDocumentSweepListener() {
                @Override
                public void sweepUpdate(final Map<Path, UpdateOp> updates)
                        throws DocumentStoreException {
                    // create a synthetic commit. this commit does not have any
                    // changes, we just use it to create a journal entry for
                    // cache invalidation and apply the sweep updates
                    backgroundOperationLock.readLock().lock();
                    try {
                        boolean success = false;
                        Revision r = commitQueue.createRevision();
                        try {
                            commitQueue.done(r, new CommitQueue.Callback() {
                                @Override
                                public void headOfQueue(@NotNull Revision revision) {
                                    writeUpdates(updates, revision);
                                }
                            });
                            success = true;
                        } finally {
                            if (!success && commitQueue.contains(r)) {
                                commitQueue.canceled(r);
                            }
                        }
                    } finally {
                        backgroundOperationLock.readLock().unlock();
                    }
                }

                private void writeUpdates(Map<Path, UpdateOp> updates,
                                          Revision revision)
                        throws DocumentStoreException {
                    // create journal entry
                    JournalEntry entry = JOURNAL.newDocument(getDocumentStore());
                    entry.modified(updates.keySet());
                    Revision r = newRevision().asBranchRevision();
                    if (!store.create(JOURNAL, singletonList(entry.asUpdateOp(r)))) {
                        String msg = "Unable to create journal entry for " +
                                "document invalidation. Will be retried with " +
                                "next background sweep operation.";
                        throw new DocumentStoreException(msg);
                    }
                    changes.invalidate(Collections.singleton(r));
                    unsavedLastRevisions.put(ROOT, revision);
                    RevisionVector newHead = getHeadRevision().update(revision);
                    setRoot(newHead);
                    commitQueue.headRevisionChanged();

                    store.createOrUpdate(NODES, new ArrayList<>(updates.values()));
                    numUpdates.addAndGet(updates.size());
                    LOG.debug("Background sweep updated {}", updates.keySet());
                }
            });

            if (newSweepRev != null) {
                updateSweepRevision(newSweepRev);
            }
            return numUpdates.get();
        } finally {
            Utils.closeIfCloseable(docs);
        }
    }

    /**
     * Updates the local sweep revision.
     *
     * @param newSweepRevision the new sweep revision for this document node
     *                         store instance.
     */
    private void updateSweepRevision(Revision newSweepRevision) {
        backgroundOperationLock.readLock().lock();
        try {
            sweepRevisions = sweepRevisions.pmax(new RevisionVector(newSweepRevision));
        } finally {
            backgroundOperationLock.readLock().unlock();
        }
    }

    /**
     * Checks if the server time difference is more than the allowed time
     * (default: 2000 ms).
     *
     * @param s the document store to get the time difference from.
     * @throws AssertionError when the time difference is too high.
     */
    private void checkServerTimeDifference(DocumentStore s)
            throws AssertionError {
        try {
            if (maxTimeDiffMillis >= 0) {
                final long timeDiff = s.determineServerTimeDifferenceMillis();
                LOG.info("Server time difference: {}ms (max allowed: {}ms)", timeDiff, maxTimeDiffMillis);
                if (Math.abs(timeDiff) > maxTimeDiffMillis) {
                    throw new AssertionError("Server clock seems off (" + timeDiff + "ms) by more than configured amount ("
                            + maxTimeDiffMillis + "ms)");
                }
            }
        } catch (RuntimeException e) { // no checked exception
            // in case of a RuntimeException, just log but continue
            LOG.warn("Got RuntimeException while trying to determine time difference to server: " + e, e);
        }

    }

    /**
     * Checks if this node store can operate on the data in the given document
     * store.
     *
     * @param store the document store.
     * @param readOnlyMode whether this node store is in read-only mode.
     * @throws DocumentStoreException if the versions are incompatible given the
     *      access mode (read-write vs. read-only).
     */
    private static void checkVersion(DocumentStore store, boolean readOnlyMode)
            throws DocumentStoreException {
        FormatVersion storeVersion = FormatVersion.versionOf(store);
        if (!VERSION.canRead(storeVersion)) {
            throw new DocumentStoreException("Cannot open DocumentNodeStore. " +
                    "Existing data in DocumentStore was written with more " +
                    "recent version. Store version: " + storeVersion +
                    ", this version: " + VERSION);
        }
        if (!readOnlyMode) {
            if (storeVersion == FormatVersion.V0) {
                // no version present. set to current version
                VERSION.writeTo(store);
                LOG.info("FormatVersion set to {}", VERSION);
            } else if (!VERSION.equals(storeVersion)) {
                // version does not match. fail the check and
                // require a manual upgrade first
                throw new DocumentStoreException("Cannot open DocumentNodeStore " +
                        "in read-write mode. Existing data in DocumentStore " +
                        "was written with older version. Store version: " +
                        storeVersion + ", this version: " + VERSION + ". Use " +
                        "the oak-run-" + getModuleVersion() + ".jar tool " +
                        "with the unlockUpgrade command first.");
            }
        }
    }

    /**
     * Checks the revision age as defined in
     * {@link Utils#checkRevisionAge(DocumentStore, ClusterNodeInfo, Clock)}
     * and disposes the passed cluster node {@code info} if the check fails.
     *
     * @param store the document store from where to read the root document.
     * @param info the cluster node info with the clusterId.
     * @param clock the clock to get the current time.
     * @throws DocumentStoreException if the check fails.
     */
    private static void checkRevisionAge(DocumentStore store,
                                         ClusterNodeInfo info,
                                         Clock clock)
            throws DocumentStoreException {
        boolean success = false;
        try {
            Utils.checkRevisionAge(store, info, clock);
            success = true;
        } finally {
            if (!success) {
                info.dispose();
            }
        }
    }

    private void pushJournalEntry(Revision r) throws DocumentStoreException {
        if (!changes.hasChanges()) {
            LOG.debug("Not pushing journal as there are no changes");
        } else if (store.create(JOURNAL, singletonList(changes.asUpdateOp(r)))) {
            // success: start with a new document
            changes = newJournalEntry();
        } else {
            // fail: log and keep the changes
            LOG.error("Failed to write to journal({}), accumulating changes for future write (~{} bytes, {} paths)",
                    r, changes.getMemory(), changes.getNumChangedNodes());
        }
    }

    /**
     * Returns the binary size of a property value represented as a JSON or
     * {@code -1} if the property is not of type binary.
     *
     * @param json the property value.
     * @return the size of the referenced binary value(s); otherwise {@code -1}.
     */
    private long getBinarySize(@Nullable String json) {
        if (json == null) {
            return -1;
        }
        PropertyState p = DocumentPropertyStateFactory.createPropertyState(DocumentNodeStore.this, "p", json);
        if (p.getType().tag() != PropertyType.BINARY) {
            return -1;
        }
        long size = 0;
        if (p.isArray()) {
            for (int i = 0; i < p.count(); i++) {
                size += p.size(i);
            }
        } else {
            size = p.size();
        }
        return size;
    }

    private JournalEntry newJournalEntry() {
        return new JournalEntry(store, true, newChangeSetBuilder(), journalPropertyHandlerFactory.newHandler());
    }

    /**
     * Performs an initial read of the _lastRevs on the root document and sets
     * the root state.
     *
     * @param rootDoc the current root document.
     */
    private void initializeRootState(NodeDocument rootDoc) {
        Validate.checkState(root == null);

        try {
            alignWithExternalRevisions(rootDoc, clock, clusterId, maxTimeDiffMillis);
        } catch (InterruptedException e) {
            throw new DocumentStoreException("Interrupted while aligning with " +
                    "external revision: " + rootDoc.getLastRev());
        }
        RevisionVector headRevision = new RevisionVector(
                rootDoc.getLastRev().values()).update(newRevision());
        setRoot(headRevision);
    }

    private Commit newTrunkCommit(@NotNull Changes changes,
                                  @NotNull RevisionVector base) {
        checkArgument(!requireNonNull(base).isBranch(),
                "base must not be a branch revision: " + base);

        // build commit before revision is created by the commit queue (OAK-7869)
        CommitBuilder commitBuilder = newCommitBuilder(base, null);
        changes.with(commitBuilder);

        boolean success = false;
        Commit c;
        backgroundOperationLock.readLock().lock();
        try {
            checkOpen();
            c = commitBuilder.build(commitQueue.createRevision());
            inDoubtTrunkCommits.add(c.getRevision());
            success = true;
        } finally {
            if (!success) {
                backgroundOperationLock.readLock().unlock();
            }
        }
        return c;
    }

    @NotNull
    private Commit newBranchCommit(@NotNull Changes changes,
                                   @NotNull RevisionVector base,
                                   @Nullable DocumentNodeStoreBranch branch) {
        checkArgument(requireNonNull(base).isBranch(),
                "base must be a branch revision: " + base);

        checkOpen();
        Revision commitRevision = newRevision();
        CommitBuilder commitBuilder = newCommitBuilder(base, commitRevision);
        changes.with(commitBuilder);
        if (isDisableBranches()) {
            // Regular branch commits do not need to acquire the background
            // operation lock because the head is not updated and no pending
            // lastRev updates are done on trunk. When branches are disabled,
            // a branch commit becomes a pseudo trunk commit and the lock
            // must be acquired.
            backgroundOperationLock.readLock().lock();
        } else {
            Revision rev = commitRevision.asBranchRevision();
            // remember branch commit
            Branch b = getBranches().getBranch(base);
            if (b == null) {
                // baseRev is marker for new branch
                getBranches().create(base.asTrunkRevision(), rev, branch);
                LOG.debug("Branch created with base revision {}", base);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Branch created", new Exception());
                }
            } else {
                b.addCommit(rev);
            }
        }
        return commitBuilder.build();
    }

    @NotNull
    private CommitBuilder newCommitBuilder(@NotNull RevisionVector base,
                                           @Nullable Revision commitRevision) {
        CommitBuilder cb;
        if (commitRevision != null) {
            cb = new CommitBuilder(this, commitRevision, base);
        } else {
            cb = new CommitBuilder(this, base);
        }
        RevisionVector startRevs = Utils.getStartRevisions(clusterNodes.values());
        return cb.withStartRevisions(startRevs);
    }

    /**
     * Checks if this store is still open and throws an
     * {@link IllegalStateException} if it is already disposed (or a dispose
     * is in progress).
     *
     * @throws IllegalStateException if this store is disposed.
     */
    private void checkOpen() throws IllegalStateException {
        if (isDisposed.get()) {
            throw new IllegalStateException("This DocumentNodeStore is disposed");
        }
    }

    /**
     * Search for presence of child node as denoted by path in the children cache of parent
     *
     * @param path the path of the child node
     * @param rev revision at which check is performed
     * @return <code>true</code> if and only if the children cache entry for parent path is complete
     * and that list does not have the given child node. A <code>false</code> indicates that node <i>might</i>
     * exist
     */
    private boolean checkNodeNotExistsFromChildrenCache(Path path,
                                                        RevisionVector rev) {
        final Path parentPath = path.getParent();
        if (parentPath == null) {
            return false;
        }

        NamePathRev key = childNodeCacheKey(parentPath, rev, "");//read first child cache entry
        DocumentNodeState.Children children = nodeChildrenCache.getIfPresent(key);
        String lookupChildName = path.getName();

        //Does not know about children so cannot say for sure
        if (children == null) {
            return false;
        }

        //List not complete so cannot say for sure
        if (children.hasMore) {
            return false;
        }

        int childPosition = Collections.binarySearch(children.children, lookupChildName);
        if (childPosition < 0) {
            //Node does not exist for sure
            LOG.trace("Child node as per path {} does not exist at revision {}", path, rev);
            return true;
        }

        return false;
    }

    private String diffImpl(AbstractDocumentNodeState from, AbstractDocumentNodeState to)
            throws DocumentStoreException {
        int max = MANY_CHILDREN_THRESHOLD;

        final boolean debug = LOG.isDebugEnabled();
        final long start = debug ? now() : 0;
        long getChildrenDoneIn = start;

        String diff = null;
        String diffAlgo = null;
        RevisionVector fromRev = null;
        RevisionVector toRev = null;
        long minTimestamp = Utils.getMinTimestampForDiff(
                from.getRootRevision(), to.getRootRevision(),
                getMinExternalRevisions());
        long minJournalTimestamp = newRevision().getTimestamp() -
                journalGarbageCollector.getMaxRevisionAgeMillis() / 2;

        // use journal if possible
        Revision tailRev = journalGarbageCollector.getTailRevision();
        if (!disableJournalDiff
                && tailRev.getTimestamp() < minTimestamp
                && minJournalTimestamp < minTimestamp) {
            try {
                diff = new JournalDiffLoader(from, to, this).call();
                diffAlgo = "diffJournalChildren";
                fromRev = from.getRootRevision();
                toRev = to.getRootRevision();
            } catch (RuntimeException e) {
                // avoid filling the log file with stack traces for a known issue
                // see OAK-6016 and OAK-6011
                if (e instanceof IllegalStateException &&
                        "Root document does not have a lastRev entry for local clusterId 0".equals(e.getMessage())) {
                    LOG.warn("diffJournalChildren failed with " +
                            e.getClass().getSimpleName() +
                            ", falling back to classic diff : " + e.getMessage());
                } else {
                    LOG.warn("diffJournalChildren failed with " +
                            e.getClass().getSimpleName() +
                            ", falling back to classic diff", e);
                }
            }
        }
        if (diff == null) {
            // fall back to classic diff
            fromRev = from.getLastRevision();
            toRev = to.getLastRevision();

            JsopWriter w = new JsopStream();
            boolean continueDiff = bundledDocDiffer.diff(from, to, w);

            if (continueDiff) {
                DocumentNodeState.Children fromChildren, toChildren;
                fromChildren = getChildren(from, "", max);
                toChildren = getChildren(to, "", max);
                getChildrenDoneIn = debug ? now() : 0;

                if (!fromChildren.hasMore && !toChildren.hasMore) {
                    diffAlgo = "diffFewChildren";
                    diffFewChildren(w, from.getPath(), fromChildren,
                            fromRev, toChildren, toRev);
                } else {
                    if (FAST_DIFF) {
                        diffAlgo = "diffManyChildren";
                        fromRev = from.getRootRevision();
                        toRev = to.getRootRevision();
                        diffManyChildren(w, from.getPath(), fromRev, toRev);
                    } else {
                        diffAlgo = "diffAllChildren";
                        max = Integer.MAX_VALUE;
                        fromChildren = getChildren(from, "", max);
                        toChildren = getChildren(to, "", max);
                        diffFewChildren(w, from.getPath(), fromChildren,
                                fromRev, toChildren, toRev);
                    }
                }
            } else {
                diffAlgo = "allBundledChildren";
            }
            diff = w.toString();
        }

        if (debug) {
            long end = now();
            LOG.debug("Diff performed via '{}' at [{}] between revisions [{}] => [{}] took {} ms ({} ms), diff '{}', external '{}",
                    diffAlgo, from.getPath(), fromRev, toRev,
                    end - start, getChildrenDoneIn - start, diff,
                    to.isFromExternalChange());
        }
        return diff;
    }

    void diffManyChildren(JsopWriter w, Path path,
                                  RevisionVector fromRev,
                                  RevisionVector toRev) {
        long minTimestamp = Utils.getMinTimestampForDiffManyChildren(
                fromRev, toRev, getMinExternalRevisions());
        for (RevisionVector r : new RevisionVector[]{fromRev, toRev}) {
            if (r.isBranch()) {
                Branch b = branches.getBranch(r);
                if (b != null) {
                    minTimestamp = Math.min(b.getBase().getRevision(clusterId).getTimestamp(), minTimestamp);
                }
            }
        }
        long minValue = NodeDocument.getModifiedInSecs(minTimestamp);
        String fromKey = Utils.getKeyLowerLimit(path);
        String toKey = Utils.getKeyUpperLimit(path);
        Set<Path> paths = new HashSet<>();

        LOG.debug("diffManyChildren: path: {}, fromRev: {}, toRev: {}", path, fromRev, toRev);

        for (NodeDocument doc : store.query(Collection.NODES, fromKey, toKey,
                NodeDocument.MODIFIED_IN_SECS, minValue, Integer.MAX_VALUE, List.of(PATH))) {
            paths.add(doc.getPath());
        }

        LOG.debug("diffManyChildren: Affected paths: {}", paths.size());
        // also consider nodes with not yet stored modifications (OAK-1107)
        Revision minRev = new Revision(minTimestamp, 0, getClusterId());
        addPathsForDiff(path, paths, getPendingModifications().getPaths(minRev));
        for (RevisionVector rv : new RevisionVector[]{fromRev, toRev}) {
            if (rv.isBranch()) {
                Revision r = rv.getBranchRevision();
                Branch b = branches.getBranch(rv);
                if (b != null) {
                    addPathsForDiff(path, paths, b.getModifiedPathsUntil(r));
                }
            }
        }
        for (Path p : paths) {
            DocumentNodeState fromNode = getNode(p, fromRev);
            DocumentNodeState toNode = getNode(p, toRev);
            String name = p.getName();

            LOG.trace("diffManyChildren: Changed Path {}", path);

            if (fromNode != null) {
                // exists in fromRev
                if (toNode != null) {
                    // exists in both revisions
                    // check if different
                    RevisionVector a = fromNode.getLastRevision();
                    RevisionVector b = toNode.getLastRevision();
                    if (a == null && b == null) {
                        // ok
                    } else if (a == null || b == null || !a.equals(b)) {
                        w.tag('^').key(name).object().endObject();
                    }
                } else {
                    // does not exist in toRev -> was removed
                    w.tag('-').value(name);
                }
            } else {
                // does not exist in fromRev
                if (toNode != null) {
                    // exists in toRev
                    w.tag('+').key(name).object().endObject();
                } else {
                    // does not exist in either revisions
                    // -> do nothing
                }
            }
        }
    }

    private static void addPathsForDiff(Path path,
                                        Set<Path> paths,
                                        Iterable<Path> modified) {
        for (Path p : modified) {
            if (p.isRoot()) {
                continue;
            }
            Path parent = p.getParent();
            if (path.equals(parent)) {
                paths.add(p);
            }
        }
    }

    private void diffFewChildren(JsopWriter w, Path parentPath,
                                 DocumentNodeState.Children fromChildren,
                                 RevisionVector fromRev,
                                 DocumentNodeState.Children toChildren,
                                 RevisionVector toRev) {
        Set<String> childrenSet = new HashSet<>(toChildren.children);
        for (String n : fromChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('-').value(n);
            } else {
                Path path = new Path(parentPath, n);
                DocumentNodeState n1 = getNode(path, fromRev);
                DocumentNodeState n2 = getNode(path, toRev);
                // this is not fully correct:
                // a change is detected if the node changed recently,
                // even if the revisions are well in the past
                // if this is a problem it would need to be changed
                requireNonNull(n1, String.format("Node at [%s] not found for fromRev [%s]", path, fromRev));
                requireNonNull(n2, String.format("Node at [%s] not found for toRev [%s]", path, toRev));
                if (!n1.getLastRevision().equals(n2.getLastRevision())) {
                    w.tag('^').key(n).object().endObject();
                }
            }
        }
        childrenSet = new HashSet<>(fromChildren.children);
        for (String n : toChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('+').key(n).object().endObject();
            }
        }
    }

    private static NamePathRev childNodeCacheKey(@NotNull Path path,
                                                 @NotNull RevisionVector readRevision,
                                                 @NotNull String name) {
        return new NamePathRev(name, path, readRevision);
    }

    private static DocumentRootBuilder asDocumentRootBuilder(NodeBuilder builder)
            throws IllegalArgumentException {
        if (!(builder instanceof DocumentRootBuilder)) {
            throw new IllegalArgumentException("builder must be a " +
                    DocumentRootBuilder.class.getName());
        }
        return (DocumentRootBuilder) builder;
    }

    private static long now(){
        return System.currentTimeMillis();
    }

    private void checkBranchAge(Branch b) throws CommitFailedException {
        // check if initial branch commit is too old to merge
        long journalMaxAge = journalGarbageCollector.getMaxRevisionAgeMillis();
        long branchMaxAge = journalMaxAge / 2;
        long created = b.getCommits().first().getTimestamp();
        long branchAge = newRevision().getTimestamp() - created;
        if (branchAge > branchMaxAge) {
            String msg = "Long running commit detected. Branch was created " +
                    Utils.timestampToString(created) + ". Consider breaking " +
                    "the commit down into smaller pieces or increasing the " +
                    "'journalGCMaxAge' currently set to " + journalMaxAge +
                    " ms (" + MILLISECONDS.toMinutes(journalMaxAge) + " min).";
            throw new CommitFailedException(OAK, 200, msg);
        }
    }

    /**
     * Creates and returns a MarkSweepGarbageCollector if the current BlobStore
     * supports garbage collection
     *
     * @param blobGcMaxAgeInSecs
     * @param repositoryId
     * @param whiteboard
     * @param statisticsProvider
     * @return garbage collector of the BlobStore supports GC otherwise null
     */
    @Nullable
    public MarkSweepGarbageCollector createBlobGarbageCollector(long blobGcMaxAgeInSecs, String repositoryId,
        Whiteboard whiteboard, StatisticsProvider statisticsProvider) {
        MarkSweepGarbageCollector blobGC = null;
        if(blobStore instanceof GarbageCollectableBlobStore){
            try {
                blobGC = new MarkSweepGarbageCollector(
                        new DocumentBlobReferenceRetriever(this),
                            (GarbageCollectableBlobStore) blobStore,
                        executor,
                        SECONDS.toMillis(blobGcMaxAgeInSecs),
                        repositoryId,
                        whiteboard,
                        statisticsProvider);
            } catch (IOException e) {
                throw new RuntimeException("Error occurred while initializing " +
                        "the MarkSweepGarbageCollector",e);
            }
        }
        return blobGC;
    }

    void setClusterStateChangeListener(ClusterStateChangeListener clusterStateChangeListener) {
        this.clusterStateChangeListener = clusterStateChangeListener;
    }

    private void signalClusterStateChange() {
        if (clusterStateChangeListener != null) {
            clusterStateChangeListener.handleClusterStateChange();
        }
    }

    public DocumentNodeStoreMBean getMBean() {
        return mbean;
    }

    private DocumentNodeStoreMBean createMBean(DocumentNodeStoreBuilder<?> builder) {
        return new DocumentNodeStoreMBeanImpl(this,
                builder.getStatisticsProvider().getStats(),
                clusterNodes.values(),
                builder.getRevisionGCMaxAge());
    }

    private static abstract class NodeStoreTask implements Runnable {
        final WeakReference<DocumentNodeStore> ref;
        private final AtomicBoolean isDisposed;
        private final Supplier<Integer> delaySupplier;
        private boolean failing;

        NodeStoreTask(final DocumentNodeStore nodeStore,
                      final AtomicBoolean isDisposed,
                      Supplier<Integer> delay) {
            this.ref = new WeakReference<DocumentNodeStore>(nodeStore);
            this.isDisposed = isDisposed;
            if (delay == null) {
                delay = new Supplier<Integer>() {
                    @Override
                    public Integer get() {
                        DocumentNodeStore ns = ref.get();
                        return ns != null ? ns.getAsyncDelay() : 0;
                    }
                };
            }
            this.delaySupplier = delay;
        }

        NodeStoreTask(final DocumentNodeStore nodeStore,
                      final AtomicBoolean isDisposed) {
            this(nodeStore, isDisposed, null);
        }

        protected abstract void execute(@NotNull DocumentNodeStore nodeStore);

        @Override
        public void run() {
            int delay = delaySupplier.get();
            while (delay != 0 && !isDisposed.get()) {
                synchronized (isDisposed) {
                    try {
                        isDisposed.wait(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                DocumentNodeStore nodeStore = ref.get();
                if (nodeStore != null) {
                    try {
                        execute(nodeStore);
                        if (failing) {
                            LOG.info("Background operation {} successful again",
                                    getClass().getSimpleName());
                            failing = false;
                        }
                    } catch (Throwable t) {
                        failing = true;
                        LOG.warn("Background operation failed: " + t.toString(), t);
                    }
                    delay = delaySupplier.get();
                } else {
                    // node store not in use anymore
                    break;
                }
            }
        }
    }

    /**
     * Background update operations.
     */
    static class BackgroundUpdateOperation extends NodeStoreTask {

        BackgroundUpdateOperation(DocumentNodeStore nodeStore,
                                  AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed);
        }

        @Override
        protected void execute(@NotNull DocumentNodeStore nodeStore) {
            nodeStore.runBackgroundUpdateOperations();
        }
    }

    /**
     * Background read operations.
     */
    static class BackgroundReadOperation extends NodeStoreTask {

        BackgroundReadOperation(DocumentNodeStore nodeStore,
                                AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed);
        }

        @Override
        protected void execute(@NotNull DocumentNodeStore nodeStore) {
            nodeStore.runBackgroundReadOperations();
        }
    }

    /**
     * Background sweep operation.
     */
    private static class BackgroundSweepOperation extends NodeStoreTask {

        BackgroundSweepOperation(DocumentNodeStore nodeStore,
                                 AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed, getDelay(nodeStore));
        }

        @Override
        protected void execute(@NotNull DocumentNodeStore nodeStore) {
            nodeStore.backgroundSweep();
        }

        private static Supplier<Integer> getDelay(DocumentNodeStore ns) {
            int delay = 0;
            if (ns.getAsyncDelay() != 0) {
                delay = (int) SECONDS.toMillis(MODIFIED_IN_SECS_RESOLUTION);
            }
            return Suppliers.ofInstance(delay);
        }
    }

    /**
     * Background unmerged branch commits & collision marker operation.
     */
    private static class BackgroundPurgeOperation extends NodeStoreTask {

        BackgroundPurgeOperation(DocumentNodeStore nodeStore, AtomicBoolean isDisposed) {
            // run every 60 secs
            super(nodeStore, isDisposed, Suppliers.ofInstance(60000));
        }

        @Override
        protected void execute(@NotNull DocumentNodeStore nodeStore) {
            final Clock clock = nodeStore.getClock();
            final long now = clock.getTime();
            final boolean debug = LOG.isDebugEnabled();
            if (debug) {
                LOG.debug("BackgroundPurgeOperation.execute: started purging for non active clusterIds");
            }
            ClusterNodeInfoDocument.all(nodeStore.nonLeaseCheckingStore)
                    .stream().filter(cluster -> !cluster.isActive())
                    .filter(cluster -> nonNull(cluster.getLastWrittenRootRev()))
                    .forEach(nodeStore::purgeUnmergedBranchCommitAndCollisionMarkers);
            if (debug) {
                LOG.debug("BackgroundPurgeOperation.execute: done purging for non active clusterIds, time taken {}ms",
                        clock.getTime() - now);
            }
        }
    }

    /**
     * Background sweep2 operation (also see OAK-9176 for details/context).
     */
    private static class BackgroundSweep2Operation extends NodeStoreTask {

        private final long sweep2Lock;
        private int retryCount = 0;

        BackgroundSweep2Operation(DocumentNodeStore nodeStore,
                                 AtomicBoolean isDisposed,
                                 long sweep2Lock) {
            // default asyncDelay is 1000ms == 1sec
            // the sweep2 is fine to run every 60sec by default as it is not time critical
            // to achieve this we're doing a Math.min(60sec, 60 * getAsyncDelay())
            super(nodeStore, isDisposed,
                    Suppliers.ofInstance(Math.min(60000, 60 * nodeStore.getAsyncDelay())));
            if (sweep2Lock < 0) {
                throw new IllegalArgumentException("sweep2Lock must not be negative");
            }
            this.sweep2Lock = sweep2Lock;
        }

        @Override
        protected void execute(@NotNull DocumentNodeStore nodeStore) {
            if (retryCount > 0) {
                LOG.info("BackgroundSweep2Operation.execute: retrying sweep2. retryCount=" + retryCount);
            }
            if (nodeStore.backgroundSweep2(sweep2Lock)) {
                done();
            }
            // log the fact that we noticed an ongoing sweep2 - is only logged once every 5min
            // until either the other instance finishes or crashes, at which point we or another
            // instance will pick up
            if (++retryCount % 5 == 0) {
                LOG.info("BackgroundSweep2Operation.execute: another instance is currently (still) executing sweep2. "
                        + "Waiting for its outcome. retryCount=" + retryCount);
            }
        }

        private void done() {
            ref.clear();
        }
    }

    private static class BackgroundLeaseUpdate extends NodeStoreTask {

        /** OAK-4859 : log if time between two renewClusterIdLease calls is too long **/
        private long lastRenewClusterIdLeaseCall = -1;

        /** elapsed time for previous update operation **/
        private long elapsedForPreviousRenewal  = -1;

        private static int INTERVAL_MS = 1000;
        private static int TOLERANCE_FOR_WARNING_MS = 2000;

        BackgroundLeaseUpdate(DocumentNodeStore nodeStore,
                              AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed, Suppliers.ofInstance(INTERVAL_MS));
        }

        @Override
        protected void execute(@NotNull DocumentNodeStore nodeStore) {
            // OAK-4859 : keep track of invocation time of renewClusterIdLease
            // and warn if time since last call is longer than 5sec
            Clock clock = nodeStore.getClock();
            long now = clock.getTime();

            if (lastRenewClusterIdLeaseCall >= 0) {
                final long diff = now - lastRenewClusterIdLeaseCall;
                if (diff > INTERVAL_MS + TOLERANCE_FOR_WARNING_MS) {
                    String renewTimeMessage = elapsedForPreviousRenewal <= 0 ? ""
                            : String.format(" (of which the last update operation took %dms)", elapsedForPreviousRenewal);
                    LOG.warn(
                            "BackgroundLeaseUpdate.execute: time since last renewClusterIdLease() call longer than expected: {}ms{} (expected ~{}ms)",
                            diff, renewTimeMessage, INTERVAL_MS);
                }
            }
            lastRenewClusterIdLeaseCall = now;

            nodeStore.renewClusterIdLease();
            elapsedForPreviousRenewal = clock.getTime() - now;
        }
    }

    private static class BackgroundClusterUpdate extends NodeStoreTask {

        BackgroundClusterUpdate(DocumentNodeStore nodeStore,
                              AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed, Suppliers.ofInstance(1000));
        }

        @Override
        protected void execute(@NotNull DocumentNodeStore nodeStore) {
            if (nodeStore.updateClusterState()) {
                nodeStore.signalClusterStateChange();
            }
        }
    }

    public BlobStore getBlobStore() {
        return blobStore;
    }

    BlobSerializer getBlobSerializer() {
        return blobSerializer;
    }

    /**
     * Returns an iterator for all the blob present in the store.
     *
     * <p>In some cases the iterator might implement {@link java.io.Closeable}. So
     * callers should check for such iterator and close them.
     *
     * @return an iterator for all the blobs
     */
    public Iterator<ReferencedBlob> getReferencedBlobsIterator() {
        return referencedBlobs.iterator();
    }

    public DiffCache getDiffCache() {
        return diffCache;
    }

    public Checkpoints getCheckpoints() {
        return checkpoints;
    }

    @NotNull
    public VersionGarbageCollector getVersionGarbageCollector() {
        return versionGarbageCollector;
    }

    @NotNull
    public JournalGarbageCollector getJournalGarbageCollector() {
        return journalGarbageCollector;
    }
    
    @NotNull
    public LastRevRecoveryAgent getLastRevRecoveryAgent() {
        return lastRevRecoveryAgent;
    }

    @Override
    public String getInstanceId() {
        return String.valueOf(getClusterId());
    }
    
    @Override
    public String getVisibilityToken() {
        final DocumentNodeState theRoot = root;
        if (theRoot == null) {
            // unlikely but for paranoia reasons...
            return "";
        }
        return theRoot.getRootRevision().asString();
    }
    
    private boolean isVisible(RevisionVector rv) {
        // do not synchronize, take a local copy instead
        final DocumentNodeState localRoot = root;
        if (localRoot == null) {
            // unlikely but for paranoia reasons...
            return false;
        }
        return Utils.isGreaterOrEquals(localRoot.getRootRevision(), rv);
    }
    
    @Override
    public boolean isVisible(@NotNull String visibilityToken, long maxWaitMillis) throws InterruptedException {
        if (Strings.isNullOrEmpty(visibilityToken)) {
            // we've asked for @Nonnull..
            // hence throwing an exception
            throw new IllegalArgumentException("visibilityToken must not be null or empty");
        }
        // 'fromString' would throw a RuntimeException if it can't parse 
        // that would be re thrown automatically
        final RevisionVector visibilityTokenRv = RevisionVector.fromString(visibilityToken);

        if (isVisible(visibilityTokenRv)) {
            // the simple case
            return true;
        }
        
        // otherwise wait until the visibility token's revisions all become visible
        // (or maxWaitMillis has passed)
        commitQueue.suspendUntilAll(CollectionUtils.toSet(visibilityTokenRv), maxWaitMillis);
        
        // if we got interrupted above would throw InterruptedException
        // otherwise, we don't know why suspendUntilAll returned, so
        // check the final isVisible state and return it
        return isVisible(visibilityTokenRv);
    }

    public DocumentNodeStoreStatsCollector getStatsCollector() {
        return nodeStoreStatsCollector;
    }

    public DocumentNodeStateCache getNodeStateCache() {
        return nodeStateCache;
    }

    public void setNodeStateCache(DocumentNodeStateCache nodeStateCache) {
        this.nodeStateCache = nodeStateCache;
    }

    public JournalPropertyHandlerFactory getJournalPropertyHandlerFactory() {
        return journalPropertyHandlerFactory;
    }

    int getUpdateLimit() {
        return updateLimit;
    }
    
    boolean isReadOnlyMode() {
        return readOnlyMode;
    }

    @Override
    public void prefetch(java.util.Collection<String> paths, NodeState rootState) {
        if (paths != null
                && rootState instanceof DocumentNodeState
                && isPrefetchEnabled()) {
            cacheWarming.prefetch(paths, (DocumentNodeState) rootState);
        }
    }

    private boolean isPrefetchEnabled() {
        // feature can be enabled with system property or feature toggle
        return prefetchEnabled
                || (prefetchFeature != null && prefetchFeature.isEnabled());
    }

    /**
     * Configures the performance logger with the specified info log interval.
     *
     * @param infoLogMillis the interval in milliseconds for logging performance information.
     */
    static void configurePerfLogger(long infoLogMillis) {
        PERFLOG.setInfoLogMillis(infoLogMillis);
    }
}
