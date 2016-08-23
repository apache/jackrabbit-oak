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
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.reverse;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.document.Collection.CLUSTER_NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.FAST_DIFF;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.MANY_CHILDREN_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.JournalEntry.fillExternalChanges;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.pathToId;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
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
import javax.management.openmbean.CompositeData;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.Branch.BranchCommit;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast.DynamicBroadcastConfig;
import org.apache.jackrabbit.oak.plugins.document.util.ReadOnlyDocumentStoreWrapperFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
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
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.jackrabbit.stats.TimeSeriesStatsUtil;
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
     * Do not cache more than this number of children for a document.
     */
    static final int NUM_CHILDREN_CACHE_LIMIT = Integer.getInteger("oak.documentMK.childrenCacheLimit", 16 * 1024);

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

    /**
     * Feature flag to disable the journal diff mechanism. See OAK-4528.
     */
    private boolean disableJournalDiff =
            Boolean.getBoolean("oak.disableJournalDiff");

    /**
     * The document store (might be used by multiple node stores).
     */
    protected final DocumentStore store;

    /**
     * Marker node, indicating a node does not exist at a given revision.
     */
    protected final DocumentNodeState missing;

    /**
     * The commit queue to coordinate the commits.
     */
    protected final CommitQueue commitQueue;

    /**
     * Commit queue for batch updates.
     */
    protected final BatchCommitQueue batchCommitQueue;

    /**
     * The change dispatcher for this node store.
     */
    protected final ChangeDispatcher dispatcher;

    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    protected int asyncDelay = 1000;

    /**
     * The maximum back off time in milliseconds when merges are retried. The
     * default value is twice the {@link #asyncDelay}.
     */
    protected int maxBackOffMillis =
            Integer.getInteger("oak.maxBackOffMS", asyncDelay * 2);

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
    private final Predicate<String> isBinary = new Predicate<String>() {
        @Override
        public boolean apply(@Nullable String input) {
            if (input == null) {
                return false;
            }
            return new DocumentPropertyState(DocumentNodeStore.this,
                    "p", input).getType().tag() == PropertyType.BINARY;
        }
    };

    private final Clock clock;

    private final Checkpoints checkpoints;

    private final VersionGarbageCollector versionGarbageCollector;

    private final JournalGarbageCollector journalGarbageCollector;

    private final Iterable<ReferencedBlob> referencedBlobs;
    
    private final Executor executor;

    private final LastRevRecoveryAgent lastRevRecoveryAgent;

    private final boolean disableBranches;

    private PersistentCache persistentCache;

    private PersistentCache journalCache;

    private final DocumentNodeStoreMBean mbean;

    private final boolean readOnlyMode;

    private DocumentNodeStateCache nodeStateCache = DocumentNodeStateCache.NOOP;

    private final DocumentNodeStoreStatsCollector nodeStoreStatsCollector;

    private final StatisticsProvider statisticsProvider;

    public DocumentNodeStore(DocumentMK.Builder builder) {
        this.blobStore = builder.getBlobStore();
        this.statisticsProvider = builder.getStatisticsProvider();
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

        if (builder.getLeaseCheck()) {
            s = new LeaseCheckDocumentStoreWrapper(s, clusterNodeInfo);
            clusterNodeInfo.setLeaseFailureHandler(builder.getLeaseFailureHandler());
        } else {
            clusterNodeInfo.setLeaseCheckDisabled(true);
        }

        this.store = s;
        this.changes = newJournalEntry();
        this.clusterId = cid;
        this.branches = new UnmergedBranches();
        this.asyncDelay = builder.getAsyncDelay();
        this.versionGarbageCollector = new VersionGarbageCollector(
                this, builder.createVersionGCSupport());
        this.journalGarbageCollector = new JournalGarbageCollector(this);
        this.referencedBlobs = builder.createReferencedBlobs(this);
        this.lastRevRecoveryAgent = new LastRevRecoveryAgent(this,
                builder.createMissingLastRevSeeker());
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

        nodeChildrenCache = builder.buildChildrenCache();
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
            setRoot(head);
            // make sure _lastRev is written back to store
            backgroundWrite();
            rootDoc = store.find(NODES, Utils.getIdFromPath("/"));
            // at this point the root document must exist
            if (rootDoc == null) {
                throw new IllegalStateException("Root document does not exist");
            }
        } else {
            if (!readOnlyMode) {
                checkLastRevRecovery();
            }
            initializeRootState(rootDoc);
            // check if _lastRev for our clusterId exists
            if (!rootDoc.getLastRev().containsKey(clusterId)) {
                unsavedLastRevisions.put("/", getRoot().getRootRevision().getRevision(clusterId));
                if (!readOnlyMode) {
                    backgroundWrite();
                }
            }
        }

        // Renew the lease because it may have been stale
        renewClusterIdLease();

        // initialize branchCommits
        branches.init(store, this);

        dispatcher = new ChangeDispatcher(getRoot());
        commitQueue = new CommitQueue(this);
        String threadNamePostfix = "(" + clusterId + ")";
        batchCommitQueue = new BatchCommitQueue(store);
        backgroundReadThread = new Thread(
                new BackgroundReadOperation(this, isDisposed),
                "DocumentNodeStore background read thread " + threadNamePostfix);
        backgroundReadThread.setDaemon(true);
        backgroundUpdateThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "DocumentNodeStore background update thread " + threadNamePostfix);
        backgroundUpdateThread.setDaemon(true);

        backgroundReadThread.start();
        if (!readOnlyMode) {
            backgroundUpdateThread.start();
        }

        leaseUpdateThread = new Thread(new BackgroundLeaseUpdate(this, isDisposed),
                "DocumentNodeStore lease update thread " + threadNamePostfix);
        leaseUpdateThread.setDaemon(true);
        // OAK-3398 : make lease updating more robust by ensuring it
        // has higher likelihood of succeeding than other threads
        // on a very busy machine - so as to prevent lease timeout.
        leaseUpdateThread.setPriority(Thread.MAX_PRIORITY);
        if (!readOnlyMode) {
            leaseUpdateThread.start();
        }
        
        persistentCache = builder.getPersistentCache();
        if (!readOnlyMode && persistentCache != null) {
            DynamicBroadcastConfig broadcastConfig = new DocumentBroadcastConfig(this);
            persistentCache.setBroadcastConfig(broadcastConfig);
        }
        journalCache = builder.getJournalCache();

        this.mbean = createMBean();
        LOG.info("Initialized DocumentNodeStore with clusterNodeId: {} ({})", clusterId,
                getClusterNodeInfoDisplayString());
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
        try {
            backgroundReadThread.join();
        } catch (InterruptedException e) {
            // ignore
        }
        try {
            backgroundUpdateThread.join();
        } catch (InterruptedException e) {
            // ignore
        }

        // do a final round of background operations after
        // the background thread stopped
        if (!readOnlyMode) {
            try {
                internalRunBackgroundUpdateOperations();
            } catch (AssertionError ae) {
                // OAK-3250 : when a lease check fails, subsequent modifying requests
                // to the DocumentStore will throw an AssertionError. Since as a result
                // of a failing lease check a bundle.stop is done and thus a dispose of the
                // DocumentNodeStore happens, it is very likely that in that case
                // you run into an AssertionError. We should still continue with disposing
                // though - thus catching and logging..
                LOG.error("dispose: an AssertionError happened during dispose's last background ops: " + ae, ae);
            }
        }

        try {
            leaseUpdateThread.join();
        } catch (InterruptedException e) {
            // ignore
        }

        // now mark this cluster node as inactive by
        // disposing the clusterNodeInfo
        clusterNodeInfo.dispose();
        store.dispose();

        if (blobStore instanceof Closeable) {
            try {
                ((Closeable) blobStore).close();
            } catch (IOException ex) {
                LOG.debug("Error closing blob store " + blobStore, ex);
            }
        }
        if (persistentCache != null) {
            persistentCache.close();
        }
        if (journalCache != null) {
            journalCache.close();
        }
        LOG.info("Disposed DocumentNodeStore with clusterNodeId: {}", clusterId);
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

    RevisionVector done(final @Nonnull Commit c, boolean isBranch, final @Nullable CommitInfo info) {
        if (commitQueue.contains(c.getRevision())) {
            try {
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
                        // update head revision
                        newHead[0] = before.update(c.getRevision());
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
            c.applyToCache(c.getBaseRevision(), isBranch);
            return c.getBaseRevision().update(c.getRevision().asBranchRevision());
        }
    }

    void canceled(Commit c) {
        if (commitQueue.contains(c.getRevision())) {
            try {
                commitQueue.canceled(c.getRevision());
            } finally {
                backgroundOperationLock.readLock().unlock();
            }
        } else {
            Branch b = branches.getBranch(c.getBaseRevision());
            if (b != null) {
                b.removeCommit(c.getRevision().asBranchRevision());
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

    void setEnableConcurrentAddRemove(boolean b) {
        enableConcurrentAddRemove = b;
    }

    boolean getEnableConcurrentAddRemove() {
        return enableConcurrentAddRemove;
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
    DocumentNodeState getNode(@Nonnull final String path,
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
                    c.children.add(Utils.unshareString(PathUtils.getName(p)));
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
                    //This is very unexpected situation - parent's child list declares the child to exist, while
                    //its node state is null. Let's put some extra effort to do some logging
                    String id = Utils.getIdFromPath(p);
                    String cachedDocStr, uncachedDocStr;
                    try {
                        cachedDocStr = store.find(Collection.NODES, id).asString();
                    } catch (DocumentStoreException dse) {
                        cachedDocStr = dse.toString();
                    }
                    try {
                        uncachedDocStr = store.find(Collection.NODES, id, 0).asString();
                    } catch (DocumentStoreException dse) {
                        uncachedDocStr = dse.toString();
                    }
                    String exceptionMsg = String.format(
                            "Aborting getChildNodes() - DocumentNodeState is null for %s at %s " +
                                    "{\"cachedDoc\":{%s}, \"uncachedDoc\":{%s}}",
                            readRevision, p, cachedDocStr, uncachedDocStr);
                    throw new DocumentStoreException(exceptionMsg);
                }
                return result;
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
                      List<String> removed, List<String> changed,
                      DiffCache.Entry cacheEntry) {
        if (isNew) {
            if (added.isEmpty()) {
                // this is a leaf node.
                // check if it has the children flag set
                NodeDocument doc = store.find(NODES, getIdFromPath(path));
                if (doc != null && doc.hasChildren()) {
                    PathRev key = childNodeCacheKey(path, after, null);
                    LOG.debug("nodeChildrenCache.put({},{})", key, "NO_CHILDREN");
                    nodeChildrenCache.put(key, DocumentNodeState.NO_CHILDREN);
                }
            } else {
                DocumentNodeState.Children c = new DocumentNodeState.Children();
                Set<String> set = Sets.newTreeSet();
                for (String p : added) {
                    set.add(Utils.unshareString(PathUtils.getName(p)));
                }
                c.children.addAll(set);
                PathRev key = childNodeCacheKey(path, after, null);
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
                PathRev key = new PathRev(p, beforeState.getLastRevision());
                beforeState = nodeCache.getIfPresent(key);
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
                PathRev afterKey = new PathRev(path, before.update(rev));
                // are there any added or removed children?
                if (added.isEmpty() && removed.isEmpty()) {
                    // simply use the same list
                    LOG.debug("nodeChildrenCache.put({},{})", afterKey, children);
                    nodeChildrenCache.put(afterKey, children);
                } else if (!children.hasMore){
                    // list is complete. use before children as basis
                    Set<String> afterChildren = Sets.newTreeSet(children.children);
                    for (String p : added) {
                        afterChildren.add(Utils.unshareString(PathUtils.getName(p)));
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

        // update diff cache
        JsopWriter w = new JsopStream();
        for (String p : added) {
            w.tag('+').key(PathUtils.getName(p)).object().endObject();
        }
        for (String p : removed) {
            w.tag('-').value(PathUtils.getName(p));
        }
        for (String p : changed) {
            w.tag('^').key(PathUtils.getName(p)).object().endObject();
        }
        cacheEntry.append(path, w.toString());
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
                         @Nullable CommitInfo info)
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

    @Nonnull
    public RevisionVector getHeadRevision() {
        return root.getRootRevision();
    }

    @Nonnull
    public Revision newRevision() {
        if (simpleRevisionCounter != null) {
            return new Revision(simpleRevisionCounter.getAndIncrement(), 0, clusterId);
        }
        return Revision.newRevision(clusterId);
    }

    //----------------------< background operations >---------------------------

    /** Used for testing only */
    public void runBackgroundOperations() {
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
                LOG.warn("Background update operation failed (will be retried with next run): " + e.toString(), e);
                return;
            }
            throw e;
        }
    }

    private void internalRunBackgroundUpdateOperations() {
        BackgroundWriteStats stats = null;
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
            // write back pending updates to _lastRev
            stats = backgroundWrite();
            stats.split = splitTime;
            stats.clean = cleanTime;
            stats.totalWriteTime = clock.getTime() - start;
            String msg = "Background operations stats ({})";
            if (stats.totalWriteTime > TimeUnit.SECONDS.toMillis(10)) {
                // log as info if it took more than 10 seconds
                LOG.info(msg, stats);
            } else {
                LOG.debug(msg, stats);
            }
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
                LOG.warn("Background read operation failed: " + e.toString(), e);
                return;
            }
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
            if (clock.getTime() - start > TimeUnit.SECONDS.toMillis(10)) {
                // log as info if it took more than 10 seconds
                LOG.info(msg, readStats.totalReadTime, readStats);
            } else {
                LOG.debug(msg, readStats.totalReadTime, readStats);
            }
        }
        nodeStoreStatsCollector.doneBackgroundRead(readStats);
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
        for (ClusterNodeInfoDocument doc : ClusterNodeInfoDocument.all(store)) {
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
    RevisionVector getMinExternalRevisions() {
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
    BackgroundReadStats backgroundRead() {
        BackgroundReadStats stats = new BackgroundReadStats();
        long time = clock.getTime();
        String id = Utils.getIdFromPath("/");
        NodeDocument doc = store.find(Collection.NODES, id, asyncDelay);
        if (doc == null) {
            return stats;
        }
        alignWithExternalRevisions(doc);

        StringSort externalSort = JournalEntry.newSorter();

        Map<Integer, Revision> lastRevMap = doc.getLastRev();
        try {
            RevisionVector headRevision = getHeadRevision();
            Set<Revision> externalChanges = Sets.newHashSet();
            for (Map.Entry<Integer, Revision> e : lastRevMap.entrySet()) {
                int machineId = e.getKey();
                if (machineId == clusterId) {
                    // ignore own lastRev
                    continue;
                }
                Revision r = e.getValue();
                Revision last = headRevision.getRevision(machineId);
                if (last == null) {
                    // make sure we see all changes when a cluster node joins
                    last = new Revision(0, 0, machineId);
                }
                if (r.compareRevisionTime(last) > 0) {
                    // OAK-2345
                    // only consider as external change if
                    // the revision changed for the machineId
                    externalChanges.add(r);
                    // collect external changes
                    if (externalSort != null) {
                        // add changes for this particular clusterId to the externalSort
                        try {
                            fillExternalChanges(externalSort, last, r, store);
                        } catch (IOException e1) {
                            LOG.error("backgroundRead: Exception while reading external changes from journal: " + e1, e1);
                            IOUtils.closeQuietly(externalSort);
                            externalSort = null;
                        }
                    }
                }
            }

            stats.readHead = clock.getTime() - time;
            time = clock.getTime();

            if (!externalChanges.isEmpty()) {
                // invalidate caches
                if (externalSort == null) {
                    // if no externalSort available, then invalidate the classic way: everything
                    stats.cacheStats = store.invalidateCache();
                } else {
                    try {
                        externalSort.sort();
                        stats.numExternalChanges = externalSort.getSize();
                        stats.cacheStats = store.invalidateCache(pathToId(externalSort));
                    } catch (Exception ioe) {
                        LOG.error("backgroundRead: got IOException during external sorting/cache invalidation (as a result, invalidating entire cache): "+ioe, ioe);
                        stats.cacheStats = store.invalidateCache();
                    }
                }
                stats.cacheInvalidationTime = clock.getTime() - time;
                time = clock.getTime();

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
                    commitQueue.headRevisionChanged();
                    time = clock.getTime();
                    if (externalSort != null) {
                        // then there were external changes and reading them
                        // was successful -> apply them to the diff cache
                        try {
                            JournalEntry.applyTo(externalSort, diffCache, oldHead, newHead);
                        } catch (Exception e1) {
                            LOG.error("backgroundRead: Exception while processing external changes from journal: {}", e1, e1);
                        }
                    }
                    stats.populateDiffCache = clock.getTime() - time;
                    time = clock.getTime();

                    dispatcher.contentChanged(getRoot().fromExternalChange(), null);
                } finally {
                    backgroundOperationLock.writeLock().unlock();
                }
                stats.dispatchChanges = clock.getTime() - time;
            }
        } finally {
            IOUtils.closeQuietly(externalSort);
        }

        return stats;
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
            for (UpdateOp op : doc.split(this, head, isBinary)) {
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

    BackgroundWriteStats backgroundWrite() {
        return unsavedLastRevisions.persist(this, new UnsavedModifications.Snapshot() {
            @Override
            public void acquiring(Revision mostRecent) {
                if (store.create(JOURNAL, singletonList(changes.asUpdateOp(mostRecent)))) {
                    // success: start with a new document
                    changes = newJournalEntry();
                } else {
                    // fail: log and keep the changes
                    LOG.error("Failed to write to journal, accumulating changes for future write (~" + changes.getMemory()
                            + " bytes).");
                }
            }
        }, backgroundOperationLock.writeLock());
    }

    //-----------------------------< internal >---------------------------------

    private JournalEntry newJournalEntry() {
        return new JournalEntry(store, true);
    }

    /**
     * Performs an initial read of the _lastRevs on the root document and sets
     * the root state.
     *
     * @param rootDoc the current root document.
     */
    private void initializeRootState(NodeDocument rootDoc) {
        checkState(root == null);

        alignWithExternalRevisions(rootDoc);
        RevisionVector headRevision = new RevisionVector(
                rootDoc.getLastRev().values()).update(newRevision());
        setRoot(headRevision);
    }

    /**
     * Makes sure the current time is after the most recent external revision
     * timestamp in the _lastRev map of the given root document. If necessary
     * the current thread waits until {@link #clock} is after the external
     * revision timestamp.
     *
     * @param rootDoc the root document.
     */
    private void alignWithExternalRevisions(@Nonnull NodeDocument rootDoc) {
        Map<Integer, Revision> lastRevMap = checkNotNull(rootDoc).getLastRev();
        try {
            long externalTime = Utils.getMaxExternalTimestamp(lastRevMap.values(), clusterId);
            long localTime = clock.getTime();
            if (localTime < externalTime) {
                LOG.warn("Detected clock differences. Local time is '{}', " +
                                "while most recent external time is '{}'. " +
                                "Current _lastRev entries: {}",
                        new Date(localTime), new Date(externalTime), lastRevMap.values());
                double delay = ((double) externalTime - localTime) / 1000d;
                String msg = String.format("Background read will be delayed by %.1f seconds. " +
                        "Please check system time on cluster nodes.", delay);
                LOG.warn(msg);
                clock.waitUntil(externalTime + 1);
            } else if (localTime == externalTime) {
                // make sure local time is past external time
                // but only log at debug
                LOG.debug("Local and external time are equal. Waiting until local" +
                        "time is more recent than external reported time.");
                clock.waitUntil(externalTime + 1);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Background read interrupted", e);
        }
    }

    @Nonnull
    private Commit newTrunkCommit(@Nonnull RevisionVector base) {
        checkArgument(!checkNotNull(base).isBranch(),
                "base must not be a branch revision: " + base);

        backgroundOperationLock.readLock().lock();
        boolean success = false;
        Commit c;
        try {
            checkOpen();
            c = new Commit(this, commitQueue.createRevision(), base);
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
        if (!isDisableBranches()) {
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

        String diff;
        String diffAlgo;
        RevisionVector fromRev = from.getLastRevision();
        RevisionVector toRev = to.getLastRevision();
        long minTimestamp = Utils.getMinTimestampForDiff(
                fromRev, toRev, getMinExternalRevisions());

        // use journal if possible
        Revision tailRev = journalGarbageCollector.getTailRevision();
        if (!disableJournalDiff
                && tailRev.getTimestamp() < minTimestamp) {
            diffAlgo = "diffJournalChildren";
            diff = new JournalDiffLoader(from, to, this).call();
        } else {
            DocumentNodeState.Children fromChildren, toChildren;
            fromChildren = getChildren(from, null, max);
            toChildren = getChildren(to, null, max);
            getChildrenDoneIn = debug ? now() : 0;

            JsopWriter w = new JsopStream();
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
                        TimeUnit.SECONDS.toMillis(blobGcMaxAgeInSecs),
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

    //-----------------------------< DocumentNodeStoreMBean >---------------------------------

    public DocumentNodeStoreMBean getMBean() {
        return mbean;
    }

    private DocumentNodeStoreMBean createMBean(){
        try {
            return new MBeanImpl();
        } catch (NotCompliantMBeanException e) {
            throw new IllegalStateException(e);
        }
    }

    private class MBeanImpl extends AnnotatedStandardMBean implements DocumentNodeStoreMBean {
        private final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
        private final TimeZone TZ_UTC = TimeZone.getTimeZone("UTC");

        protected MBeanImpl() throws NotCompliantMBeanException {
            super(DocumentNodeStoreMBean.class);
        }

        @Override
        public String getRevisionComparatorState() {
            return "";
        }

        @Override
        public String getHead(){
            return getRoot().getRootRevision().toString();
        }

        @Override
        public int getClusterId() {
            return clusterId;
        }

        @Override
        public int getUnmergedBranchCount() {
            return branches.size();
        }

        @Override
        public String[] getInactiveClusterNodes() {
            return toArray(transform(filter(clusterNodes.values(),
                    new Predicate<ClusterNodeInfoDocument>() {
                        @Override
                        public boolean apply(ClusterNodeInfoDocument input) {
                            return !input.isActive();
                        }
                    }),
                    new Function<ClusterNodeInfoDocument, String>() {
                        @Override
                        public String apply(ClusterNodeInfoDocument input) {
                            return input.getClusterId() + "=" + input.getCreated();
                        }
                    }), String.class);
        }

        @Override
        public String[] getActiveClusterNodes() {
            return toArray(transform(filter(clusterNodes.values(),
                    new Predicate<ClusterNodeInfoDocument>() {
                        @Override
                        public boolean apply(ClusterNodeInfoDocument input) {
                            return input.isActive();
                        }
                    }),
                    new Function<ClusterNodeInfoDocument, String>() {
                        @Override
                        public String apply(ClusterNodeInfoDocument input) {
                            return input.getClusterId() + "=" + input.getLeaseEndTime();
                        }
                    }), String.class);
        }

        @Override
        public String[] getLastKnownRevisions() {
            return toArray(transform(filter(getHeadRevision(), new Predicate<Revision>() {
                        @Override
                        public boolean apply(Revision input) {
                            return input.getClusterId() != getClusterId();
                        }
                    }),
                    new Function<Revision, String>() {
                        @Override
                        public String apply(Revision input) {
                            return input.getClusterId() + "=" + input.toString();
                        }
                    }), String.class);
        }

        @Override
        public String formatRevision(String rev, boolean utc){
            Revision r = Revision.fromString(rev);
            final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
            if (utc) {
                sdf.setTimeZone(TZ_UTC);
            }
            return sdf.format(r.getTimestamp());
        }

        @Override
        public long determineServerTimeDifferenceMillis() {
            return store.determineServerTimeDifferenceMillis();
        }

        @Override
        public CompositeData getMergeSuccessHistory() {
            return getTimeSeriesData(DocumentNodeStoreStats.MERGE_SUCCESS_COUNT, "Merge Success Count");
        }

        @Override
        public CompositeData getMergeFailureHistory() {
            return getTimeSeriesData(DocumentNodeStoreStats.MERGE_FAILED_EXCLUSIVE, "Merge failure count");
        }

        @Override
        public CompositeData getExternalChangeCountHistory() {
            return getTimeSeriesData(DocumentNodeStoreStats.BGR_NUM_CHANGES_RATE, "Count of nodes modified by other " +
                    "cluster nodes since last background read");
        }

        @Override
        public CompositeData getBackgroundUpdateCountHistory() {
            return getTimeSeriesData(DocumentNodeStoreStats.BGW_NUM_WRITES_RATE, "Count of nodes updated as part of " +
                    "background update");
        }

        private CompositeData getTimeSeriesData(String name, String desc){
            return TimeSeriesStatsUtil.asCompositeData(getTimeSeries(name), desc);
        }

        private TimeSeries getTimeSeries(String name) {
            return statisticsProvider.getStats().getTimeSeries(name, true);
        }
    }

    static abstract class NodeStoreTask implements Runnable {
        final WeakReference<DocumentNodeStore> ref;
        private final AtomicBoolean isDisposed;
        private final Supplier<Integer> delaySupplier;

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
                    } catch (Throwable t) {
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
     * Background operations.
     */
    static class BackgroundOperation extends NodeStoreTask {

        BackgroundOperation(DocumentNodeStore nodeStore,
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

    static class BackgroundLeaseUpdate extends NodeStoreTask {

        BackgroundLeaseUpdate(DocumentNodeStore nodeStore,
                              AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed, Suppliers.ofInstance(1000));
        }

        @Override
        protected void execute(@Nonnull DocumentNodeStore nodeStore) {
            // first renew the clusterId lease
            nodeStore.renewClusterIdLease();

            // then, independently if the lease had to be updated or not, check
            // the status:
            if (nodeStore.updateClusterState()) {
                // then inform the discovery lite listener - if it is registered
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
     * callers should check for such iterator and close them</p>
     *
     * @see org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobReferenceIterator
     * @return an iterator for all the blobs
     */
    public Iterator<ReferencedBlob> getReferencedBlobsIterator() {
        return referencedBlobs.iterator();
    }

    public DiffCache getDiffCache() {
        return diffCache;
    }

    public Clock getClock() {
        return clock;
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

    public DocumentNodeStoreStatsCollector getStatsCollector() {
        return nodeStoreStatsCollector;
    }

    public void setNodeStateCache(DocumentNodeStateCache nodeStateCache) {
        this.nodeStateCache = nodeStateCache;
    }
}