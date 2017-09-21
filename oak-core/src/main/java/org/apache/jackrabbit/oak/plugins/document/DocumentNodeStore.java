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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.jackrabbit.oak.api.CommitFailedException.OAK;
import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.FAST_DIFF;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.MANY_CHILDREN_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MODIFIED_IN_SECS_RESOLUTION;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.alignWithExternalRevisions;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getModuleVersion;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.pathToId;
import static org.apache.jackrabbit.oak.plugins.observation.ChangeCollectorProvider.COMMIT_CONTEXT_OBSERVATION_CHANGESET;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.management.NotCompliantMBeanException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.Branch.BranchCommit;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundledDocumentDiffer;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigHandler;
import org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast.DynamicBroadcastConfig;
import org.apache.jackrabbit.oak.plugins.document.util.ReadOnlyDocumentStoreWrapperFactory;
import org.apache.jackrabbit.oak.plugins.observation.ChangeSet;
import org.apache.jackrabbit.oak.plugins.observation.ChangeSetBuilder;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.json.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.document.util.LeaseCheckDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.LoggingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.CommitContext;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.commit.SimpleCommitContext;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.commons.benchmark.PerfLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a NodeStore on {@link DocumentStore}.
 */
public final class DocumentNodeStore
        implements NodeStore, RevisionContext, Observable, Clusterable, NodeStateDiffer {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStore.class);

    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(DocumentNodeStore.class.getName() + ".perf"));

    /**
     * Number of milliseconds in one minute.
     */
    private static final long ONE_MINUTE_MS = TimeUnit.MINUTES.toMillis(1);

    public static final FormatVersion VERSION = FormatVersion.V1_8;

    /**
     * Do not cache more than this number of children for a document.
     */
    static final int NUM_CHILDREN_CACHE_LIMIT = Integer.getInteger("oak.documentMK.childrenCacheLimit", 16 * 1024);

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
     * Feature flag to enable concurrent add/remove operations of hidden empty
     * nodes. See OAK-2673.
     */
    private boolean enableConcurrentAddRemove =
            Boolean.getBoolean("oak.enableConcurrentAddRemove");

    /**
     * Use fair mode for background operation lock.
     */
    private boolean fairBackgroundOperationLock =
            Boolean.parseBoolean(System.getProperty("oak.fairBackgroundOperationLock", "true"));

    /**
     * The timeout in milliseconds to wait for the recovery performed by
     * another cluster node.
     */
    private long recoveryWaitTimeoutMS =
            Long.getLong("oak.recoveryWaitTimeoutMS", 60000);


    public static final String SYS_PROP_DISABLE_JOURNAL = "oak.disableJournalDiff";
    /**
     * Feature flag to disable the journal diff mechanism. See OAK-4528.
     */
    private boolean disableJournalDiff =
            Boolean.getBoolean(SYS_PROP_DISABLE_JOURNAL);

    /**
     * Threshold for number of paths in journal entry to require a force push during commit
     * (instead of at background write)
     */
    private int journalPushThreshold = Integer.getInteger("oak.journalPushThreshold", 100000);

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
    private int maxBackOffMillis =
            Integer.getInteger("oak.maxBackOffMS", asyncDelay * 2);

    private int changeSetMaxItems =  Integer.getInteger("oak.document.changeSet.maxItems", 50);

    private int changeSetMaxDepth =  Integer.getInteger("oak.document.changeSet.maxDepth", 9);

    /**
     * Whether this instance is disposed.
     */
    private final AtomicBoolean isDisposed = new AtomicBoolean();

    /**
     * The cluster instance info.
     */
    @Nonnull
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
    private final ConcurrentMap<Integer, ClusterNodeInfoDocument> clusterNodes
            = Maps.newConcurrentMap();

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
    private final Map<String, String> splitCandidates = Maps.newConcurrentMap();

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
     * Monitor object to synchronize background writes.
     */
    private final Object backgroundWriteMonitor = new Object();

    /**
     * Background thread performing the clusterId lease renew.
     */
    @Nonnull
    private Thread leaseUpdateThread;

    /**
     * Background thread performing the cluster update
     */
    @Nonnull
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
    private final Cache<PathRev, DocumentNodeState.Children> nodeChildrenCache;
    private final CacheStats nodeChildrenCacheStats;

    /**
     * The change log to keep track of commits for diff operations.
     */
    private final DiffCache diffCache;

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

    /**
     * A predicate, which takes a String and returns {@code true} if the String
     * is a serialized binary value of a {@link DocumentPropertyState}. The
     * apply method will throw an IllegalArgumentException if the String is
     * malformed.
     */
    private final Function<String, Long> binarySize = new Function<String, Long>() {
        @Override
        public Long apply(@Nullable String input) {
            return getBinarySize(input);
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
     * revision is added to this set when {@link #newTrunkCommit(RevisionVector)}
     * is called and removed when the commit either:
     * <ul>
     *     <li>Succeeds with {@link #done(Commit, boolean, CommitInfo)}</li>
     *     <li>Fails with {@link #canceled(Commit)} and the commit does *not*
     *      have the {@link Commit#rollbackFailed()} flag set.</li>
     * </ul>
     * The {@link NodeDocumentSweeper} periodically goes through this set and
     * reverts changes done by commits in the set that are older than the
     * current head revision.
     */
    private final Set<Revision> inDoubtTrunkCommits = Sets.newConcurrentHashSet();

    public DocumentNodeStore(DocumentMK.Builder builder) {
        this.updateLimit = builder.getUpdateLimit();
        this.commitValueResolver = new CommitValueResolver(builder.getCommitValueCacheSize(),
                new Supplier<RevisionVector>() {
            @Override
            public RevisionVector get() {
                return getSweepRevisions();
            }
        });
        this.blobStore = builder.getBlobStore();
        this.nodeStoreStatsCollector = builder.getNodeStoreStatsCollector();
        if (builder.isUseSimpleRevision()) {
            this.simpleRevisionCounter = new AtomicInteger(0);
        }
        DocumentStore s = builder.getDocumentStore();
        if (builder.getTiming()) {
            s = new TimingDocumentStoreWrapper(s);
        }
        if (builder.getLogging()) {
            s = new LoggingDocumentStoreWrapper(s);
        }
        if (builder.getReadOnlyMode()) {
            s = ReadOnlyDocumentStoreWrapperFactory.getInstance(s);
            readOnlyMode = true;
        } else {
            readOnlyMode = false;
        }
        checkVersion(s, readOnlyMode);
        this.executor = builder.getExecutor();
        this.clock = builder.getClock();

        int cid = builder.getClusterId();
        cid = Integer.getInteger("oak.documentMK.clusterId", cid);
        if (readOnlyMode) {
            clusterNodeInfo = ClusterNodeInfo.getReadOnlyInstance(s);
        } else {
            clusterNodeInfo = ClusterNodeInfo.getInstance(s, cid);
        }
        // TODO we should ensure revisions generated from now on
        // are never "older" than revisions already in the repository for
        // this cluster id
        cid = clusterNodeInfo.getId();

        this.nonLeaseCheckingStore = s;

        if (builder.getLeaseCheck()) {
            s = new LeaseCheckDocumentStoreWrapper(s, clusterNodeInfo);
            clusterNodeInfo.setLeaseFailureHandler(builder.getLeaseFailureHandler());
        } else {
            clusterNodeInfo.setLeaseCheckDisabled(true);
        }

        this.journalPropertyHandlerFactory = builder.getJournalPropertyHandlerFactory();
        this.store = s;
        this.changes = newJournalEntry();
        this.clusterId = cid;
        this.branches = new UnmergedBranches();
        this.asyncDelay = builder.getAsyncDelay();
        this.versionGarbageCollector = new VersionGarbageCollector(
                this, builder.createVersionGCSupport());
        this.versionGarbageCollector.setStatisticsProvider(builder.getStatisticsProvider());
        this.versionGarbageCollector.setGCMonitor(builder.getGCMonitor());
        this.journalGarbageCollector = new JournalGarbageCollector(
                this, builder.getJournalGCMaxAge());
        this.referencedBlobs =
                builder.createReferencedBlobs(this);
        this.lastRevSeeker = builder.createMissingLastRevSeeker();
        this.lastRevRecoveryAgent = new LastRevRecoveryAgent(this, lastRevSeeker);
        this.disableBranches = builder.isDisableBranches();
        this.missing = new DocumentNodeState(this, "MISSING",
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

        diffCache = builder.getDiffCache();
        checkpoints = new Checkpoints(this);

        // check if root node exists
        NodeDocument rootDoc = store.find(NODES, Utils.getIdFromPath("/"));
        if (rootDoc == null) {
            // root node is missing: repository is not initialized
            Revision commitRev = newRevision();
            Commit commit = new Commit(this, commitRev, null);
            RevisionVector head = new RevisionVector(commitRev);
            DocumentNodeState n = new DocumentNodeState(this, "/", head);
            commit.addNode(n);
            commit.applyToDocumentStore();
            unsavedLastRevisions.put("/", commitRev);
            sweepRevisions = sweepRevisions.pmax(new RevisionVector(commitRev));
            setRoot(head);
            // make sure _lastRev is written back to store
            backgroundWrite();
            rootDoc = store.find(NODES, Utils.getIdFromPath("/"));
            // at this point the root document must exist
            if (rootDoc == null) {
                throw new IllegalStateException("Root document does not exist");
            }
        } else {
            sweepRevisions = sweepRevisions.pmax(rootDoc.getSweepRevisions());
            if (!readOnlyMode) {
                checkLastRevRecovery();
            }
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
                unsavedLastRevisions.put("/", initialRev);
                // set initial sweep revision
                sweepRevisions = sweepRevisions.pmax(new RevisionVector(initialRev));
                if (!readOnlyMode) {
                    backgroundWrite();
                }
            }
        }

        // Renew the lease because it may have been stale
        renewClusterIdLease();

        // initialize branchCommits
        branches.init(store, this);

        dispatcher = builder.isPrefetchExternalChanges() ?
                new PrefetchDispatcher(getRoot(), executor) :
                new ChangeDispatcher(getRoot());
        commitQueue = new CommitQueue(this);
        String threadNamePostfix = "(" + clusterId + ")";
        batchCommitQueue = new BatchCommitQueue(store);
        // prepare background threads
        backgroundReadThread = new Thread(
                new BackgroundReadOperation(this, isDisposed),
                "DocumentNodeStore background read thread " + threadNamePostfix);
        backgroundReadThread.setDaemon(true);
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
        leaseUpdateThread = new Thread(new BackgroundLeaseUpdate(this, isDisposed),
                "DocumentNodeStore lease update thread " + threadNamePostfix);
        leaseUpdateThread.setDaemon(true);
        // now start the background threads
        clusterUpdateThread.start();
        backgroundReadThread.start();
        if (!readOnlyMode) {
            // OAK-3398 : make lease updating more robust by ensuring it
            // has higher likelihood of succeeding than other threads
            // on a very busy machine - so as to prevent lease timeout.
            leaseUpdateThread.setPriority(Thread.MAX_PRIORITY);
            leaseUpdateThread.start();

            // perform an initial document sweep if needed
            // this may be long running if there is no sweep revision
            // for this clusterId (upgrade from Oak <= 1.6).
            // it is therefore important the lease thread is running already.
            backgroundSweep();

            backgroundUpdateThread.start();
            backgroundSweepThread.start();
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

        if (!builder.isBundlingDisabled()) {
            bundlingConfigHandler.initialize(this, executor);
        }
    }


    /**
     * Recover _lastRev recovery if needed.
     *
     * @throws DocumentStoreException if recovery did not finish within
     *          {@link #recoveryWaitTimeoutMS}.
     */
    private void checkLastRevRecovery() throws DocumentStoreException {
        long timeout = clock.getTime() + recoveryWaitTimeoutMS;
        int numRecovered = lastRevRecoveryAgent.recover(clusterId, timeout);
        if (numRecovered == -1) {
            ClusterNodeInfoDocument doc = store.find(CLUSTER_NODES, String.valueOf(clusterId));
            String otherId = "n/a";
            if (doc != null) {
                otherId = String.valueOf(doc.get(ClusterNodeInfo.REV_RECOVERY_BY));
            }
            String msg = "This cluster node (" + clusterId + ") requires " +
                    "_lastRev recovery which is currently performed by " +
                    "another cluster node (" + otherId + "). Recovery is " +
                    "still ongoing after " + recoveryWaitTimeoutMS + " ms. " +
                    "Failing startup of this DocumentNodeStore now!";
            throw new DocumentStoreException(msg);
        }
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
                backgroundSweepThread);

        // create a tombstone commit revision after isDisposed is set to true.
        // commits created earlier than this revision will be able to finish
        // and we'll get a headOfQueue() callback when they are done
        // after this call there are no more pending trunk commits and
        // the commit queue is empty and stays empty
        Revision tombstone = commitQueue.createRevision();
        commitQueue.done(tombstone, new CommitQueue.Callback() {
            @Override
            public void headOfQueue(@Nonnull Revision revision) {
                setRoot(getHeadRevision().update(revision));
                unsavedLastRevisions.put(ROOT_PATH, revision);
            }
        });

        try {
            bundlingConfigHandler.close();
        } catch (IOException e) {
            LOG.warn("Error closing bundlingConfigHandler", bundlingConfigHandler, e);
        }

        DocumentStoreException ex = null;
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

        Utils.joinQuietly(clusterUpdateThread, leaseUpdateThread);

        // now mark this cluster node as inactive by disposing the
        // clusterNodeInfo, but only if final background operations
        // were successful
        if (ex == null) {
            clusterNodeInfo.dispose();
        }
        store.dispose();

        if (blobStore instanceof Closeable) {
            try {
                ((Closeable) blobStore).close();
            } catch (IOException e) {
                LOG.debug("Error closing blob store " + blobStore, e);
            }
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

    void setRoot(@Nonnull RevisionVector newHead) {
        checkArgument(!newHead.isBranch());
        root = getRoot(newHead);
    }

    @Nonnull
    public DocumentStore getDocumentStore() {
        return store;
    }

    /**
     * Creates a new commit. The caller must acknowledge the commit either with
     * {@link #done(Commit, boolean, CommitInfo)} or {@link #canceled(Commit)},
     * depending on the result of the commit.
     *
     * @param base the base revision for the commit or <code>null</code> if the
     *             commit should use the current head revision as base.
     * @param branch the branch instance if this is a branch commit. The life
     *               time of this branch commit is controlled by the
     *               reachability of this parameter. Once {@code branch} is
     *               weakly reachable, the document store implementation is
     *               free to remove the commits associated with the branch.
     * @return a new commit.
     */
    @Nonnull
    Commit newCommit(@Nullable RevisionVector base,
                     @Nullable DocumentNodeStoreBranch branch) {
        if (base == null) {
            base = getHeadRevision();
        }
        if (base.isBranch()) {
            return newBranchCommit(base, branch);
        } else {
            return newTrunkCommit(base);
        }
    }

    /**
     * Creates a new merge commit. The caller must acknowledge the commit either with
     * {@link #done(Commit, boolean, CommitInfo)} or {@link #canceled(Commit)},
     * depending on the result of the commit.
     *
     * @param base the base revision for the commit or <code>null</code> if the
     *             commit should use the current head revision as base.
     * @param numBranchCommits the number of branch commits to merge.
     * @return a new merge commit.
     */
    @Nonnull
    MergeCommit newMergeCommit(@Nullable RevisionVector base, int numBranchCommits) {
        if (base == null) {
            base = getHeadRevision();
        }
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

    RevisionVector done(final @Nonnull Commit c, boolean isBranch, final @Nonnull CommitInfo info) {
        if (commitQueue.contains(c.getRevision())) {
            try {
                inDoubtTrunkCommits.remove(c.getRevision());
                final RevisionVector[] newHead = new RevisionVector[1];
                commitQueue.done(c.getRevision(), new CommitQueue.Callback() {
                    @Override
                    public void headOfQueue(@Nonnull Revision revision) {
                        // remember before revision
                        RevisionVector before = getHeadRevision();
                        // apply changes to cache based on before revision
                        c.applyToCache(before, false);
                        // track modified paths
                        changes.modified(c.getModifiedPaths());
                        changes.readFrom(info);
                        changes.addChangeSet(getChangeSet(info));
                        // update head revision
                        Revision r = c.getRevision();
                        newHead[0] = before.update(r);
                        if (changes.getNumChangedNodes() >= journalPushThreshold) {
                            LOG.info("Pushing journal entry at {} as number of changes ({}) have reached {}",
                                    r, changes.getNumChangedNodes(), journalPushThreshold);
                            pushJournalEntry(r);
                        }
                        setRoot(newHead[0]);
                        commitQueue.headRevisionChanged();
                        dispatcher.contentChanged(getRoot(), info);
                    }
                });
                return newHead[0];
            } finally {
                backgroundOperationLock.readLock().unlock();
            }
        } else {
            // branch commit
            try {
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
        if (commitQueue.contains(c.getRevision())) {
            try {
                if (!c.rollbackFailed() || c.isEmpty()) {
                    inDoubtTrunkCommits.remove(c.getRevision());
                }
                commitQueue.canceled(c.getRevision());
            } finally {
                backgroundOperationLock.readLock().unlock();
            }
        } else {
            try {
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

    @Nonnull
    public ClusterNodeInfo getClusterInfo() {
        return clusterNodeInfo;
    }

    public CacheStats getNodeCacheStats() {
        return nodeCacheStats;
    }

    public CacheStats getNodeChildrenCacheStats() {
        return nodeChildrenCacheStats;
    }

    @Nonnull
    public Iterable<CacheStats> getDiffCacheStats() {
        return diffCache.getStats();
    }

    public Cache<PathRev, DocumentNodeState> getNodeCache() {
        return nodeCache;
    }

    public Cache<PathRev, DocumentNodeState.Children> getNodeChildrenCache() {
        return nodeChildrenCache;
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
        nodeCache.invalidate(new PathRev(path, revision));
    }

    public int getPendingWriteCount() {
        return unsavedLastRevisions.getPaths().size();
    }

    public boolean isDisableBranches() {
        return disableBranches;
    }

    /**
     * Enqueue the document with the given id as a split candidate.
     *
     * @param id the id of the document to check if it needs to be split.
     */
    void addSplitCandidate(String id) {
        splitCandidates.put(id, id);
    }

    @CheckForNull
    AbstractDocumentNodeState getSecondaryNodeState(@Nonnull final String path,
                              @Nonnull final RevisionVector rootRevision,
                              @Nonnull final RevisionVector rev) {
        //Check secondary cache first
        return nodeStateCache.getDocumentNodeState(path, rootRevision, rev);
    }

    PropertyState createPropertyState(String name, String value){
        return new DocumentPropertyState(this, name, checkNotNull(value));
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
    @CheckForNull
    public DocumentNodeState getNode(@Nonnull final String path,
                              @Nonnull final RevisionVector rev) {
        checkNotNull(rev);
        checkNotNull(path);
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

    @Nonnull
    DocumentNodeState.Children getChildren(@Nonnull final AbstractDocumentNodeState parent,
                              @Nullable final String name,
                              final int limit)
            throws DocumentStoreException {
        if (checkNotNull(parent).hasNoChildren()) {
            return DocumentNodeState.NO_CHILDREN;
        }
        final String path = checkNotNull(parent).getPath();
        final RevisionVector readRevision = parent.getLastRevision();
        try {
            PathRev key = childNodeCacheKey(path, readRevision, name);
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
     * @param name the name of the lower bound child node (exclusive) or
     *              {@code null} if no lower bound is given.
     * @param limit the maximum number of child nodes to return.
     * @return the children of {@code parent}.
     */
    DocumentNodeState.Children readChildren(AbstractDocumentNodeState parent,
                                            String name, int limit) {
        String queriedName = name;
        String path = parent.getPath();
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
                String p = doc.getPath();
                // remember name of last returned document for
                // potential next round of readChildDocs()
                name = PathUtils.getName(p);
                // filter out deleted children
                DocumentNodeState child = getNode(p, rev);
                if (child == null) {
                    continue;
                }
                if (c.children.size() < limit) {
                    // add to children until limit is reached
                    c.children.add(PathUtils.getName(p));
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
                if (queriedName == null) {
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
     * @param name the lower exclusive bound or {@code null}.
     * @param limit the maximum number of child documents to return.
     * @return the child documents.
     */
    @Nonnull
    private Iterable<NodeDocument> readChildDocs(@Nonnull final String path,
                                                 @Nullable String name,
                                                 final int limit) {
        final String to = Utils.getKeyUpperLimit(checkNotNull(path));
        final String from;
        if (name != null) {
            from = Utils.getIdFromPath(concat(path, name));
        } else {
            from = Utils.getKeyLowerLimit(path);
        }
        return store.query(Collection.NODES, from, to, limit);
    }

    /**
     * Returns up to {@code limit} child nodes, starting at the given
     * {@code name} (exclusive).
     *
     * @param parent the parent node.
     * @param name the name of the lower bound child node (exclusive) or
     *             {@code null}, if the method should start with the first known
     *             child node.
     * @param limit the maximum number of child nodes to return.
     * @return the child nodes.
     */
    @Nonnull
    Iterable<DocumentNodeState> getChildNodes(@Nonnull final DocumentNodeState parent,
                    @Nullable final String name,
                    final int limit) {
        // Preemptive check. If we know there are no children then
        // return straight away
        if (checkNotNull(parent).hasNoChildren()) {
            return Collections.emptyList();
        }

        final RevisionVector readRevision = parent.getLastRevision();
        return transform(getChildren(parent, name, limit).children, new Function<String, DocumentNodeState>() {
            @Override
            public DocumentNodeState apply(String input) {
                String p = concat(parent.getPath(), input);
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
        });
    }

    @CheckForNull
    DocumentNodeState readNode(String path, RevisionVector readRevision) {
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
                      Revision rev, String path,
                      boolean isNew, List<String> added,
                      List<String> removed, List<String> changed) {
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
                    PathRev key = childNodeCacheKey(path, afterLastRev, null);
                    LOG.debug("nodeChildrenCache.put({},{})", key, "NO_CHILDREN");
                    nodeChildrenCache.put(key, DocumentNodeState.NO_CHILDREN);
                }
            } else {
                DocumentNodeState.Children c = new DocumentNodeState.Children();
                Set<String> set = Sets.newTreeSet();
                for (String p : added) {
                    set.add(PathUtils.getName(p));
                }
                c.children.addAll(set);
                PathRev key = childNodeCacheKey(path, afterLastRev, null);
                LOG.debug("nodeChildrenCache.put({},{})", key, c);
                nodeChildrenCache.put(key, c);
            }
        } else {
            // existed before
            DocumentNodeState beforeState = getRoot(before);
            // do we have a cached before state that can be used
            // to calculate the new children?
            int depth = PathUtils.getDepth(path);
            for (int i = 1; i <= depth && beforeState != null; i++) {
                String p = PathUtils.getAncestorPath(path, depth - i);
                RevisionVector lastRev = beforeState.getLastRevision();
                PathRev key = new PathRev(p, lastRev);
                beforeState = nodeCache.getIfPresent(key);
                if (missing.equals(beforeState)) {
                    // This is unexpected. The before state should exist.
                    // Invalidate the relevant cache entries. (OAK-6294)
                    LOG.warn("Before state is missing {}. Invalidating " +
                            "affected cache entries.", key.asString());
                    store.invalidateCache(NODES, Utils.getIdFromPath(p));
                    nodeCache.invalidate(key);
                    nodeChildrenCache.invalidate(childNodeCacheKey(path, lastRev, null));
                    beforeState = null;
                }
            }
            DocumentNodeState.Children children = null;
            if (beforeState != null) {
                if (beforeState.hasNoChildren()) {
                    children = DocumentNodeState.NO_CHILDREN;
                } else {
                    PathRev key = childNodeCacheKey(path, beforeState.getLastRevision(), null);
                    children = nodeChildrenCache.getIfPresent(key);
                }
            }
            if (children != null) {
                PathRev afterKey = new PathRev(path, beforeState.getLastRevision().update(rev));
                // are there any added or removed children?
                if (added.isEmpty() && removed.isEmpty()) {
                    // simply use the same list
                    LOG.debug("nodeChildrenCache.put({},{})", afterKey, children);
                    nodeChildrenCache.put(afterKey, children);
                } else if (!children.hasMore){
                    // list is complete. use before children as basis
                    Set<String> afterChildren = Sets.newTreeSet(children.children);
                    for (String p : added) {
                        afterChildren.add(PathUtils.getName(p));
                    }
                    for (String p : removed) {
                        afterChildren.remove(PathUtils.getName(p));
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
                    Set<String> afterChildren = Sets.newLinkedHashSet(children.children);
                    for (String p : removed) {
                        afterChildren.remove(PathUtils.getName(p));
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
    void revisionsMerged(@Nonnull Iterable<Revision> revisions) {
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
    @CheckForNull
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
    @Nonnull
    DocumentNodeState getRoot(@Nonnull RevisionVector revision) {
        DocumentNodeState root = getNode("/", revision);
        if (root == null) {
            throw new IllegalStateException(
                    "root node does not exist at revision " + revision);
        }
        return root;
    }

    @Nonnull
    DocumentNodeStoreBranch createBranch(DocumentNodeState base) {
        DocumentNodeStoreBranch b = DocumentNodeStoreBranch.getCurrentBranch();
        if (b != null) {
            return b;
        }
        return new DocumentNodeStoreBranch(this, base, mergeLock);
    }

    @Nonnull
    RevisionVector rebase(@Nonnull RevisionVector branchHead,
                          @Nonnull RevisionVector base) {
        checkNotNull(branchHead);
        checkNotNull(base);
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

    @Nonnull
    RevisionVector reset(@Nonnull RevisionVector branchHead,
                         @Nonnull RevisionVector ancestor) {
        checkNotNull(branchHead);
        checkNotNull(ancestor);
        Branch b = getBranches().getBranch(branchHead);
        if (b == null) {
            throw new DocumentStoreException("Empty branch cannot be reset");
        }
        if (!b.getCommits().last().equals(branchHead.getRevision(getClusterId()))) {
            throw new DocumentStoreException(branchHead + " is not the head " +
                    "of a branch");
        }
        if (!b.containsCommit(ancestor.getBranchRevision())
                && !b.getBase().asBranchRevision(getClusterId()).equals(ancestor)) {
            throw new DocumentStoreException(ancestor + " is not " +
                    "an ancestor revision of " + branchHead);
        }
        // tailSet is inclusive -> use an ancestorRev with a
        // counter incremented by one to make the call exclusive
        Revision ancestorRev = ancestor.getBranchRevision();
        ancestorRev = new Revision(ancestorRev.getTimestamp(),
                ancestorRev.getCounter() + 1, ancestorRev.getClusterId(), true);
        List<Revision> revs = newArrayList(b.getCommits().tailSet(ancestorRev));
        if (revs.isEmpty()) {
            // trivial
            return branchHead;
        }
        UpdateOp rootOp = new UpdateOp(Utils.getIdFromPath("/"), false);
        // reset each branch commit in reverse order
        Map<String, UpdateOp> operations = Maps.newHashMap();
        for (Revision r : reverse(revs)) {
            NodeDocument.removeCollision(rootOp, r.asTrunkRevision());
            NodeDocument.removeRevision(rootOp, r.asTrunkRevision());
            operations.clear();
            BranchCommit bc = b.getCommit(r);
            if (bc.isRebase()) {
                continue;
            }
            getRoot(bc.getBase().update(r))
                    .compareAgainstBaseState(getRoot(bc.getBase()),
                            new ResetDiff(r.asTrunkRevision(), operations));
            // apply reset operations
            for (UpdateOp op : operations.values()) {
                store.findAndUpdate(Collection.NODES, op);
            }
        }
        store.findAndUpdate(Collection.NODES, rootOp);
        // clean up in-memory branch data
        for (Revision r : revs) {
            b.removeCommit(r);
        }
        return ancestor;
    }

    @Nonnull
    RevisionVector merge(@Nonnull RevisionVector branchHead,
                         @Nonnull CommitInfo info)
            throws CommitFailedException {
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
            UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), false);
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
                    throw new ConflictException(msg, conflictRevs).asCommitFailedException();
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
    public boolean compare(@Nonnull final AbstractDocumentNodeState node,
                           @Nonnull final AbstractDocumentNodeState base,
                           @Nonnull NodeStateDiff diff) {
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
    LastRevTracker createTracker(final @Nonnull Revision r,
                                 final boolean isBranchCommit) {
        if (isBranchCommit && !disableBranches) {
            Revision branchRev = r.asBranchRevision();
            return branches.getBranchCommit(branchRev);
        } else {
            return new LastRevTracker() {
                @Override
                public void track(String path) {
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
     */
    void suspendUntilAll(@Nonnull Set<Revision> conflictRevisions) {
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
    }

    //------------------------< Observable >------------------------------------

    @Override
    public Closeable addObserver(Observer observer) {
        return dispatcher.addObserver(observer);
    }

    //-------------------------< NodeStore >------------------------------------

    @Nonnull
    @Override
    public DocumentNodeState getRoot() {
        return root;
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull NodeBuilder builder,
                           @Nonnull CommitHook commitHook,
                           @Nonnull CommitInfo info)
            throws CommitFailedException {
        return asDocumentRootBuilder(builder).merge(commitHook, info);
    }

    @Nonnull
    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        return asDocumentRootBuilder(builder).rebase();
    }

    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        return asDocumentRootBuilder(builder).reset();
    }

    @Override
    @Nonnull
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
    public Blob getBlob(@Nonnull String reference) {
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

    @Nonnull
    @Override
    public String checkpoint(long lifetime, @Nonnull Map<String, String> properties) {
        return checkpoints.create(lifetime, properties).toString();
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime) {
        Map<String, String> empty = Collections.emptyMap();
        return checkpoint(lifetime, empty);
    }

    @Nonnull
    @Override
    public Map<String, String> checkpointInfo(@Nonnull String checkpoint) {
        Revision r = Revision.fromString(checkpoint);
        Checkpoints.Info info = checkpoints.getCheckpoints().get(r);
        if (info == null) {
            // checkpoint does not exist
            return Collections.emptyMap();
        } else {
            return info.get();
        }
    }

    @Nonnull
    @Override
    public Iterable<String> checkpoints() {
        final long now = clock.getTime();
        return Iterables.transform(Iterables.filter(checkpoints.getCheckpoints().entrySet(),
                new Predicate<Map.Entry<Revision,Checkpoints.Info>>() {
            @Override
            public boolean apply(Map.Entry<Revision,Checkpoints.Info> cp) {
                return cp.getValue().getExpiryTime() > now;
            }
        }), new Function<Map.Entry<Revision,Checkpoints.Info>, String>() {
            @Override
            public String apply(Map.Entry<Revision,Checkpoints.Info> cp) {
                return cp.getKey().toString();
            }
        });
    }

    @CheckForNull
    @Override
    public NodeState retrieve(@Nonnull String checkpoint) {
        RevisionVector rv = getCheckpoints().retrieve(checkpoint);
        if (rv == null) {
            return null;
        }
        // make sure all changes up to checkpoint are visible
        suspendUntilAll(Sets.newHashSet(rv));
        return getRoot(rv);
    }

    @Override
    public boolean release(@Nonnull String checkpoint) {
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
    @Nonnull
    public RevisionVector getHeadRevision() {
        return root.getRootRevision();
    }

    @Override
    @Nonnull
    public Revision newRevision() {
        if (simpleRevisionCounter != null) {
            return new Revision(simpleRevisionCounter.getAndIncrement(), 0, clusterId);
        }
        return Revision.newRevision(clusterId);
    }

    @Override
    @Nonnull
    public Clock getClock() {
        return clock;
    }

    @Override
    public String getCommitValue(@Nonnull Revision changeRevision,
                                 @Nonnull NodeDocument doc) {
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
            cleanCollisions();
            long cleanTime = clock.getTime() - time;
            time = clock.getTime();
            // split documents (does not create new revisions)
            backgroundSplit();
            long splitTime = clock.getTime() - time;
            maybeRefreshHeadRevision();
            // write back pending updates to _lastRev
            stats = backgroundWrite();
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
        return clusterNodeInfo.renewLease();
    }

    /**
     * Updates the state about cluster nodes in {@link #clusterNodes}.
     *
     * @return true if the cluster state has changed, false if the cluster state
     * remained unchanged
     */
    boolean updateClusterState() {
        boolean hasChanged = false;
        Set<Integer> clusterIds = Sets.newHashSet();
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
    @Nonnull
    private RevisionVector getMinExternalRevisions() {
        return new RevisionVector(transform(filter(clusterNodes.values(),
                new Predicate<ClusterNodeInfoDocument>() {
                    @Override
                    public boolean apply(ClusterNodeInfoDocument input) {
                        return input.getClusterId() != getClusterId();
                    }
                }),
                new Function<ClusterNodeInfoDocument, Revision>() {
            @Override
            public Revision apply(ClusterNodeInfoDocument input) {
                return new Revision(input.getStartTime(), 0, input.getClusterId());
            }
        }));
    }

    /**
     * Perform a background read and make external changes visible.
     */
    private BackgroundReadStats backgroundRead() {
        return new ExternalChange(this) {
            @Override
            void invalidateCache(@Nonnull Iterable<String> paths) {
                stats.cacheStats = store.invalidateCache(pathToId(paths));
            }

            @Override
            void invalidateCache() {
                stats.cacheStats = store.invalidateCache();
            }

            @Override
            void updateHead(@Nonnull Set<Revision> externalChanges,
                            @Nonnull RevisionVector sweepRevs,
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
                                    PathUtils.ROOT_PATH, oldHead, newHead);
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

    private static CommitInfo newCommitInfo(@Nonnull ChangeSet changeSet, JournalPropertyHandler journalPropertyHandler) {
        CommitContext commitContext = new SimpleCommitContext();
        commitContext.set(COMMIT_CONTEXT_OBSERVATION_CHANGESET, changeSet);
        journalPropertyHandler.addTo(commitContext);
        Map<String, Object> info = ImmutableMap.<String, Object>of(CommitContext.NAME, commitContext);
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
            UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), false);
            for (Revision r : b.getCommits()) {
                r = r.asTrunkRevision();
                NodeDocument.removeRevision(op, r);
                NodeDocument.removeBranchCommit(op, r);
            }
            store.findAndUpdate(NODES, op);
        }
    }
    
    private void cleanCollisions() {
        String id = Utils.getIdFromPath("/");
        NodeDocument root = store.find(NODES, id);
        if (root == null) {
            return;
        }
        RevisionVector head = getHeadRevision();
        Map<Revision, String> map = root.getLocalMap(NodeDocument.COLLISIONS);
        UpdateOp op = new UpdateOp(id, false);
        for (Revision r : map.keySet()) {
            if (r.getClusterId() == clusterId) {
                // remove collision if there is no active branch with
                // this revision and the revision is before the current
                // head. That is, the collision cannot be related to commit
                // which is progress.
                if (branches.getBranchCommit(r) == null 
                        && !head.isRevisionNewer(r)) {
                    NodeDocument.removeCollision(op, r);
                }
            }
        }
        if (op.hasChanges()) {
            LOG.debug("Removing collisions {}", op.getChanges().keySet());
            store.findAndUpdate(NODES, op);
        }
    }

    private void backgroundSplit() {
        RevisionVector head = getHeadRevision();
        for (Iterator<String> it = splitCandidates.keySet().iterator(); it.hasNext();) {
            String id = it.next();
            NodeDocument doc = store.find(Collection.NODES, id);
            if (doc == null) {
                continue;
            }
            for (UpdateOp op : doc.split(this, head, binarySize)) {
                NodeDocument before = null;
                if (!op.isNew() ||
                        !store.create(Collection.NODES, Collections.singletonList(op))) {
                    before = store.createOrUpdate(Collection.NODES, op);
                }
                if (before != null) {
                    if (LOG.isDebugEnabled()) {
                        NodeDocument after = store.find(Collection.NODES, op.getId());
                        if (after != null) {
                            LOG.debug("Split operation on {}. Size before: {}, after: {}",
                                    id, before.getMemory(), after.getMemory());
                        }
                    }
                } else {
                    LOG.debug("Split operation created {}", op.getId());
                }
            }
            it.remove();
        }
    }

    @Nonnull
    Set<String> getSplitCandidates() {
        return Collections.unmodifiableSet(splitCandidates.keySet());
    }

    @Nonnull
    RevisionVector getSweepRevisions() {
        return sweepRevisions;
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
                pushJournalEntry(mostRecent);
            }
        }, backgroundOperationLock.writeLock());
    }

    private void maybeRefreshHeadRevision() {
        if (isDisposed.get() || isDisableBranches()) {
            return;
        }
        // check if local head revision is outdated and needs an update
        // this ensures the head and sweep revisions are recent and the
        // revision garbage collector can remove old documents
        Revision head = getHeadRevision().getRevision(clusterId);
        if (head != null && head.getTimestamp() + ONE_MINUTE_MS < clock.getTime()) {
            // head was not updated for more than a minute
            // create an empty commit that updates the head
            boolean success = false;
            Commit c = newTrunkCommit(getHeadRevision());
            try {
                done(c, false, CommitInfo.EMPTY);
                success = true;
            } finally {
                if (!success) {
                    canceled(c);
                }
            }
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
        SortedSet<Revision> garbage = Sets.newTreeSet(StableRevisionComparator.INSTANCE);
        for (Revision r : inDoubtTrunkCommits) {
            if (r.compareRevisionTime(head) < 0) {
                garbage.add(r);
            }
        }
        // skip if there is no garbage and we have at least some sweep
        // revision for the local clusterId. A sweep is needed even
        // without garbage when an upgrade happened and no sweep revision
        // exists for the local clusterId
        if (garbage.isEmpty() && sweepRevisions.getRevision(clusterId) != null) {
            updateSweepRevision(head);
            return 0;
        }

        Revision startRev = new Revision(0, 0, clusterId);
        if (!garbage.isEmpty()) {
            startRev = garbage.first();
        }

        int num = forceBackgroundSweep(startRev);
        inDoubtTrunkCommits.removeAll(garbage);
        return num;
    }

    private int forceBackgroundSweep(Revision startRev) throws DocumentStoreException {
        NodeDocumentSweeper sweeper = new NodeDocumentSweeper(this, false);
        LOG.debug("Starting document sweep. Head: {}, starting at {}",
                sweeper.getHeadRevision(), startRev);
        Iterable<NodeDocument> docs = lastRevSeeker.getCandidates(startRev.getTimestamp());
        try {
            final AtomicInteger numUpdates = new AtomicInteger();

            Revision newSweepRev = sweeper.sweep(docs, new NodeDocumentSweepListener() {
                @Override
                public void sweepUpdate(final Map<String, UpdateOp> updates)
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
                                public void headOfQueue(@Nonnull Revision revision) {
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

                private void writeUpdates(Map<String, UpdateOp> updates,
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
                    unsavedLastRevisions.put(ROOT_PATH, revision);
                    RevisionVector newHead = getHeadRevision().update(revision);
                    setRoot(newHead);
                    commitQueue.headRevisionChanged();

                    store.createOrUpdate(NODES, Lists.newArrayList(updates.values()));
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

    private void pushJournalEntry(Revision r) {
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
        PropertyState p = new DocumentPropertyState(
                DocumentNodeStore.this, "p", json);
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
        checkState(root == null);

        try {
            alignWithExternalRevisions(rootDoc, clock, clusterId);
        } catch (InterruptedException e) {
            throw new DocumentStoreException("Interrupted while aligning with " +
                    "external revision: " + rootDoc.getLastRev());
        }
        RevisionVector headRevision = new RevisionVector(
                rootDoc.getLastRev().values()).update(newRevision());
        setRoot(headRevision);
    }

    @Nonnull
    private Commit newTrunkCommit(@Nonnull RevisionVector base) {
        checkArgument(!checkNotNull(base).isBranch(),
                "base must not be a branch revision: " + base);

        boolean success = false;
        Commit c;
        backgroundOperationLock.readLock().lock();
        try {
            checkOpen();
            c = new Commit(this, commitQueue.createRevision(), base);
            inDoubtTrunkCommits.add(c.getRevision());
            success = true;
        } finally {
            if (!success) {
                backgroundOperationLock.readLock().unlock();
            }
        }
        return c;
    }

    @Nonnull
    private Commit newBranchCommit(@Nonnull RevisionVector base,
                                   @Nullable DocumentNodeStoreBranch branch) {
        checkArgument(checkNotNull(base).isBranch(),
                "base must be a branch revision: " + base);

        checkOpen();
        Commit c = new Commit(this, newRevision(), base);
        if (isDisableBranches()) {
            // Regular branch commits do not need to acquire the background
            // operation lock because the head is not updated and no pending
            // lastRev updates are done on trunk. When branches are disabled,
            // a branch commit becomes a pseudo trunk commit and the lock
            // must be acquired.
            backgroundOperationLock.readLock().lock();
        } else {
            Revision rev = c.getRevision().asBranchRevision();
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
        return c;
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
     * @param path
     * @param rev revision at which check is performed
     * @return <code>true</code> if and only if the children cache entry for parent path is complete
     * and that list does not have the given child node. A <code>false</code> indicates that node <i>might</i>
     * exist
     */
    private boolean checkNodeNotExistsFromChildrenCache(String path,
                                                        RevisionVector rev) {
        if (PathUtils.denotesRoot(path)) {
            return false;
        }

        final String parentPath = PathUtils.getParentPath(path);
        PathRev key = childNodeCacheKey(parentPath, rev, null);//read first child cache entry
        DocumentNodeState.Children children = nodeChildrenCache.getIfPresent(key);
        String lookupChildName = PathUtils.getName(path);

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
                LOG.warn("diffJournalChildren failed with " +
                        e.getClass().getSimpleName() +
                        ", falling back to classic diff", e);
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
                fromChildren = getChildren(from, null, max);
                toChildren = getChildren(to, null, max);
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
                        fromChildren = getChildren(from, null, max);
                        toChildren = getChildren(to, null, max);
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

    private void diffManyChildren(JsopWriter w, String path,
                                  RevisionVector fromRev,
                                  RevisionVector toRev) {
        long minTimestamp = Utils.getMinTimestampForDiff(
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
        Set<String> paths = Sets.newHashSet();

        LOG.debug("diffManyChildren: path: {}, fromRev: {}, toRev: {}", path, fromRev, toRev);

        for (NodeDocument doc : store.query(Collection.NODES, fromKey, toKey,
                NodeDocument.MODIFIED_IN_SECS, minValue, Integer.MAX_VALUE)) {
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
        for (String p : paths) {
            DocumentNodeState fromNode = getNode(p, fromRev);
            DocumentNodeState toNode = getNode(p, toRev);
            String name = PathUtils.getName(p);

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

    private static void addPathsForDiff(String path,
                                        Set<String> paths,
                                        Iterable<String> modified) {
        for (String p : modified) {
            if (PathUtils.denotesRoot(p)) {
                continue;
            }
            String parent = PathUtils.getParentPath(p);
            if (path.equals(parent)) {
                paths.add(p);
            }
        }
    }

    private void diffFewChildren(JsopWriter w, String parentPath,
                                 DocumentNodeState.Children fromChildren,
                                 RevisionVector fromRev,
                                 DocumentNodeState.Children toChildren,
                                 RevisionVector toRev) {
        Set<String> childrenSet = Sets.newHashSet(toChildren.children);
        for (String n : fromChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('-').value(n);
            } else {
                String path = concat(parentPath, n);
                DocumentNodeState n1 = getNode(path, fromRev);
                DocumentNodeState n2 = getNode(path, toRev);
                // this is not fully correct:
                // a change is detected if the node changed recently,
                // even if the revisions are well in the past
                // if this is a problem it would need to be changed
                checkNotNull(n1, "Node at [%s] not found for fromRev [%s]", path, fromRev);
                checkNotNull(n2, "Node at [%s] not found for toRev [%s]", path, toRev);
                if (!n1.getLastRevision().equals(n2.getLastRevision())) {
                    w.tag('^').key(n).object().endObject();
                }
            }
        }
        childrenSet = Sets.newHashSet(fromChildren.children);
        for (String n : toChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('+').key(n).object().endObject();
            }
        }
    }

    private static PathRev childNodeCacheKey(@Nonnull String path,
                                             @Nonnull RevisionVector readRevision,
                                             @Nullable String name) {
        String p = (name == null ? "" : name) + path;
        return new PathRev(p, readRevision);
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
     * @return garbage collector of the BlobStore supports GC otherwise null
     */
    @CheckForNull
    public MarkSweepGarbageCollector createBlobGarbageCollector(long blobGcMaxAgeInSecs,
            String repositoryId) {
        MarkSweepGarbageCollector blobGC = null;
        if(blobStore instanceof GarbageCollectableBlobStore){
            try {
                blobGC = new MarkSweepGarbageCollector(
                        new DocumentBlobReferenceRetriever(this),
                            (GarbageCollectableBlobStore) blobStore,
                        executor,
                        SECONDS.toMillis(blobGcMaxAgeInSecs),
                        repositoryId);
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

    void signalClusterStateChange() {
        if (clusterStateChangeListener != null) {
            clusterStateChangeListener.handleClusterStateChange();
        }
    }

    public DocumentNodeStoreMBean getMBean() {
        return mbean;
    }

    private DocumentNodeStoreMBean createMBean(DocumentMK.Builder builder) {
        try {
            return new DocumentNodeStoreMBeanImpl(this,
                    builder.getStatisticsProvider().getStats(),
                    clusterNodes.values());
        } catch (NotCompliantMBeanException e) {
            throw new IllegalStateException(e);
        }
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

        protected abstract void execute(@Nonnull DocumentNodeStore nodeStore);

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
        protected void execute(@Nonnull DocumentNodeStore nodeStore) {
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
        protected void execute(@Nonnull DocumentNodeStore nodeStore) {
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
        protected void execute(@Nonnull DocumentNodeStore nodeStore) {
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

    private static class BackgroundLeaseUpdate extends NodeStoreTask {

        /** OAK-4859 : log if time between two renewClusterIdLease calls is too long **/
        private long lastRenewClusterIdLeaseCall = -1;

        BackgroundLeaseUpdate(DocumentNodeStore nodeStore,
                              AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed, Suppliers.ofInstance(1000));
        }

        @Override
        protected void execute(@Nonnull DocumentNodeStore nodeStore) {
            // OAK-4859 : keep track of invocation time of renewClusterIdLease
            // and warn if time since last call is longer than 5sec
            final long now = System.currentTimeMillis();
            if (lastRenewClusterIdLeaseCall <= 0) {
                lastRenewClusterIdLeaseCall = now;
            } else {
                final long diff = now - lastRenewClusterIdLeaseCall;
                if (diff > 5000) {
                    LOG.warn("BackgroundLeaseUpdate.execute: time since last renewClusterIdLease() call longer than expected: {}ms", diff);
                }
                lastRenewClusterIdLeaseCall = now;
            }
            // first renew the clusterId lease
            nodeStore.renewClusterIdLease();
        }
    }

    private static class BackgroundClusterUpdate extends NodeStoreTask {

        BackgroundClusterUpdate(DocumentNodeStore nodeStore,
                              AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed, Suppliers.ofInstance(1000));
        }

        @Override
        protected void execute(@Nonnull DocumentNodeStore nodeStore) {
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

    @Nonnull
    public VersionGarbageCollector getVersionGarbageCollector() {
        return versionGarbageCollector;
    }

    @Nonnull
    public JournalGarbageCollector getJournalGarbageCollector() {
        return journalGarbageCollector;
    }
    
    @Nonnull
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
    public boolean isVisible(@Nonnull String visibilityToken, long maxWaitMillis) throws InterruptedException {
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
        commitQueue.suspendUntilAll(Sets.newHashSet(visibilityTokenRv), maxWaitMillis);
        
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
}