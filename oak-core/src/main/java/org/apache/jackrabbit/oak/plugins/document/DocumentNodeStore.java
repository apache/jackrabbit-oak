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
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Iterables.transform;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.plugins.document.Collection.JOURNAL;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.FAST_DIFF;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.MANY_CHILDREN_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.JournalEntry.fillExternalChanges;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.pathToId;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
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
import javax.management.NotCompliantMBeanException;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobReferenceIterator;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.commons.sort.StringSort;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.BlobSerializer;
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
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a NodeStore on {@link DocumentStore}.
 */
public final class DocumentNodeStore
        implements NodeStore, RevisionContext, Observable {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStore.class);

    private static final PerfLogger PERFLOG = new PerfLogger(
            LoggerFactory.getLogger(DocumentNodeStore.class.getName() + ".perf"));

    /**
     * Do not cache more than this number of children for a document.
     */
    static final int NUM_CHILDREN_CACHE_LIMIT = Integer.getInteger("oak.documentMK.childrenCacheLimit", 16 * 1024);

    /**
     * When trying to access revisions that are older than this many
     * milliseconds, a warning is logged. The default is one minute.
     */
    private static final int WARN_REVISION_AGE =
            Integer.getInteger("oak.documentMK.revisionAge", 60 * 1000);

    /**
     * Enable background operations
     */
    private static final boolean ENABLE_BACKGROUND_OPS = Boolean.parseBoolean(System.getProperty("oak.documentMK.backgroundOps", "true"));

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
            Boolean.getBoolean("oak.fairBackgroundOperationLock");

    /**
     * How long to remember the relative order of old revision of all cluster
     * nodes, in milliseconds. The default is one hour.
     */
    static final int REMEMBER_REVISION_ORDER_MILLIS = 60 * 60 * 1000;

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
    private final ClusterNodeInfo clusterNodeInfo;

    /**
     * The unique cluster id, similar to the unique machine id in MongoDB.
     */
    private final int clusterId;

    /**
     * Map of inactive cluster nodes and when the cluster node was last seen
     * as inactive.
     * Key: clusterId, value: timeInMillis
     */
    private final ConcurrentMap<Integer, Long> inactiveClusterNodes
            = new ConcurrentHashMap<Integer, Long>();

    /**
     * The comparator for revisions.
     */
    private final Revision.RevisionComparator revisionComparator;

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
     * The last known revision for each cluster instance.
     *
     * Key: the machine id, value: revision.
     */
    private final Map<Integer, Revision> lastKnownRevision =
            new ConcurrentHashMap<Integer, Revision>();

    /**
     * The last known head revision. This is the last-known revision.
     */
    private volatile Revision headRevision;

    private Thread backgroundReadThread;

    private Thread backgroundUpdateThread;

    /**
     * Background thread performing the clusterId lease renew.
     * Will be {@code null} if {@link #clusterNodeInfo} is {@code null}.
     */
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

    private final Executor executor;

    private final LastRevRecoveryAgent lastRevRecoveryAgent;

    private final boolean disableBranches;

    private PersistentCache persistentCache;

    private final DocumentNodeStoreMBean mbean;

    public DocumentNodeStore(DocumentMK.Builder builder) {
        this.blobStore = builder.getBlobStore();
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
        this.store = s;
        this.changes = Collection.JOURNAL.newDocument(s);
        this.executor = builder.getExecutor();
        this.clock = builder.getClock();
        int cid = builder.getClusterId();
        cid = Integer.getInteger("oak.documentMK.clusterId", cid);
        if (cid == 0) {
            clusterNodeInfo = ClusterNodeInfo.getInstance(store);
            // TODO we should ensure revisions generated from now on
            // are never "older" than revisions already in the repository for
            // this cluster id
            cid = clusterNodeInfo.getId();
        } else {
            clusterNodeInfo = null;
        }
        this.clusterId = cid;
        this.revisionComparator = new Revision.RevisionComparator(clusterId);
        this.branches = new UnmergedBranches(getRevisionComparator());
        this.asyncDelay = builder.getAsyncDelay();
        this.versionGarbageCollector = new VersionGarbageCollector(
                this, builder.createVersionGCSupport());
        this.journalGarbageCollector = new JournalGarbageCollector(this);
        this.lastRevRecoveryAgent = new LastRevRecoveryAgent(this);
        this.disableBranches = builder.isDisableBranches();
        this.missing = new DocumentNodeState(this, "MISSING", new Revision(0, 0, 0)) {
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
            Revision head = newRevision();
            Commit commit = new Commit(this, head, null, null);
            DocumentNodeState n = new DocumentNodeState(this, "/", head);
            commit.addNode(n);
            commit.applyToDocumentStore();
            // use dummy Revision as before
            commit.applyToCache(new Revision(0, 0, clusterId), false);
            setHeadRevision(commit.getRevision());
            // make sure _lastRev is written back to store
            backgroundWrite();
        } else {
            // initialize branchCommits
            branches.init(store, this);
            // initial reading of the revisions of other cluster nodes
            backgroundRead(false);
            if (headRevision == null) {
                // no revision read from other cluster nodes
                setHeadRevision(newRevision());
            }
            // check if _lastRev for our clusterId exists
            if (!rootDoc.getLastRev().containsKey(clusterId)) {
                unsavedLastRevisions.put("/", headRevision);
                backgroundWrite();
            }
        }
        checkLastRevRecovery();
        // Renew the lease because it may have been stale
        renewClusterIdLease();

        getRevisionComparator().add(headRevision, Revision.newRevision(0));

        dispatcher = new ChangeDispatcher(getRoot());
        commitQueue = new CommitQueue() {
            @Override
            protected Revision newRevision() {
                return DocumentNodeStore.this.newRevision();
            }
        };
        String threadNamePostfix = "(" + clusterId + ")";
        batchCommitQueue = new BatchCommitQueue(store, revisionComparator);
        backgroundReadThread = new Thread(
                new BackgroundReadOperation(this, isDisposed),
                "DocumentNodeStore background read thread " + threadNamePostfix);
        backgroundReadThread.setDaemon(true);
        backgroundUpdateThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "DocumentNodeStore background update thread " + threadNamePostfix);
        backgroundUpdateThread.setDaemon(true);

        backgroundReadThread.start();
        backgroundUpdateThread.start();

        if (clusterNodeInfo != null) {
            leaseUpdateThread = new Thread(
                    new BackgroundLeaseUpdate(this, isDisposed),
                    "DocumentNodeStore lease update thread " + threadNamePostfix);
            leaseUpdateThread.start();
        }

        this.mbean = createMBean();
        LOG.info("Initialized DocumentNodeStore with clusterNodeId: {} ({})", clusterId,
                getClusterNodeInfoDisplayString());
    }

    /**
     * Recover _lastRev recovery if needed.
     */
    private void checkLastRevRecovery() {
        lastRevRecoveryAgent.recover(clusterId);
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
        internalRunBackgroundUpdateOperations();

        if (leaseUpdateThread != null) {
            try {
                leaseUpdateThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        // now mark this cluster node as inactive by
        // disposing the clusterNodeInfo
        if (clusterNodeInfo != null) {
            clusterNodeInfo.dispose();
        }
        store.dispose();
        unsavedLastRevisions.close();

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
        LOG.info("Disposed DocumentNodeStore with clusterNodeId: {}", clusterId);
    }

    private String getClusterNodeInfoDisplayString() {
        return clusterNodeInfo == null ? "no cluster node info" : clusterNodeInfo.toString().replaceAll("[\r\n\t]", " ").trim();
    }

    Revision setHeadRevision(@Nonnull Revision newHead) {
        checkArgument(!newHead.isBranch());
        Revision previous = headRevision;
        if (!checkNotNull(newHead).equals(previous)) {
            // head changed
            headRevision = newHead;
        }
        return previous;
    }

    @Nonnull
    public DocumentStore getDocumentStore() {
        return store;
    }

    /**
     * Create a new revision.
     *
     * @return the revision
     */
    @Nonnull
    Revision newRevision() {
        if (simpleRevisionCounter != null) {
            return new Revision(simpleRevisionCounter.getAndIncrement(), 0, clusterId);
        }
        return Revision.newRevision(clusterId);
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
    Commit newCommit(@Nullable Revision base,
                     @Nullable DocumentNodeStoreBranch branch) {
        if (base == null) {
            base = headRevision;
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
    MergeCommit newMergeCommit(@Nullable Revision base, int numBranchCommits) {
        if (base == null) {
            base = headRevision;
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

    void done(final @Nonnull Commit c, boolean isBranch, final @Nullable CommitInfo info) {
        if (commitQueue.contains(c.getRevision())) {
            try {
                commitQueue.done(c.getRevision(), new CommitQueue.Callback() {
                    @Override
                    public void headOfQueue(@Nonnull Revision revision) {
                        // remember before revision
                        Revision before = getHeadRevision();
                        // apply changes to cache based on before revision
                        c.applyToCache(before, false);
                        // track modified paths
                        changes.modified(c.getModifiedPaths());
                        // update head revision
                        setHeadRevision(c.getRevision());
                        dispatcher.contentChanged(getRoot(), info);
                    }
                });
            } finally {
                backgroundOperationLock.readLock().unlock();
            }
        } else {
            // branch commit
            c.applyToCache(c.getBaseRevision(), isBranch);
        }
    }

    void canceled(Commit c) {
        if (commitQueue.contains(c.getRevision())) {
            try {
                commitQueue.canceled(c.getRevision());
            } finally {
                backgroundOperationLock.readLock().unlock();
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

    @CheckForNull
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

    void invalidateNodeChildrenCache() {
        nodeChildrenCache.invalidateAll();
    }

    void invalidateNodeCache(String path, Revision revision){
        nodeCache.invalidate(new PathRev(path, revision));
    }

    public int getPendingWriteCount() {
        return unsavedLastRevisions.getPaths().size();
    }

    public boolean isDisableBranches() {
        return disableBranches;
    }

    /**
     * Checks that revision x is newer than another revision.
     *
     * @param x the revision to check
     * @param previous the presumed earlier revision
     * @return true if x is newer
     */
    boolean isRevisionNewer(@Nonnull Revision x, @Nonnull Revision previous) {
        return getRevisionComparator().compare(x, previous) > 0;
    }

    /**
     * Enqueue the document with the given id as a split candidate.
     *
     * @param id the id of the document to check if it needs to be split.
     */
    void addSplitCandidate(String id) {
        splitCandidates.put(id, id);
    }

    void copyNode(DocumentNodeState source, String targetPath, Commit commit) {
        moveOrCopyNode(false, source, targetPath, commit);
    }

    void moveNode(DocumentNodeState source, String targetPath, Commit commit) {
        moveOrCopyNode(true, source, targetPath, commit);
    }

    void markAsDeleted(DocumentNodeState node, Commit commit, boolean subTreeAlso) {
        commit.removeNode(node.getPath());

        if (subTreeAlso) {
            // recurse down the tree
            // TODO causes issue with large number of children
            for (DocumentNodeState child : getChildNodes(node, null, Integer.MAX_VALUE)) {
                markAsDeleted(child, commit, true);
            }
        }
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
    DocumentNodeState getNode(@Nonnull final String path, @Nonnull final Revision rev) {
        checkRevisionAge(checkNotNull(rev), checkNotNull(path));
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
        } catch (ExecutionException e) {
            throw DocumentStoreException.convert(e.getCause());
        }
    }

    @Nonnull
    DocumentNodeState.Children getChildren(@Nonnull final DocumentNodeState parent,
                              @Nullable final String name,
                              final int limit)
            throws DocumentStoreException {
        if (checkNotNull(parent).hasNoChildren()) {
            return DocumentNodeState.NO_CHILDREN;
        }
        final String path = checkNotNull(parent).getPath();
        final Revision readRevision = parent.getLastRevision();
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
    DocumentNodeState.Children readChildren(DocumentNodeState parent,
                                            String name, int limit) {
        String queriedName = name;
        String path = parent.getPath();
        Revision rev = parent.getLastRevision();
        LOG.trace("Reading children for [{}] ast rev [{}]", path, rev);
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

        final Revision readRevision = parent.getLastRevision();
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
                return result;
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
    DocumentNodeState readNode(String path, Revision readRevision) {
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
     * @param rev the commit revision
     * @param path the path
     * @param isNew whether this is a new node
     * @param pendingLastRev whether the node has a pending _lastRev to write
     * @param isBranchCommit whether this is from a branch commit
     * @param added the list of added child nodes
     * @param removed the list of removed child nodes
     * @param changed the list of changed child nodes.
     *
     */
    void applyChanges(Revision rev, String path,
                      boolean isNew, boolean pendingLastRev,
                      boolean isBranchCommit, List<String> added,
                      List<String> removed, List<String> changed,
                      DiffCache.Entry cacheEntry) {
        LastRevTracker tracker = createTracker(rev);
        if (disableBranches) {
            if (pendingLastRev) {
                tracker.track(path);
            }
        } else {
            if (isBranchCommit) {
                Revision branchRev = rev.asBranchRevision();
                tracker = branches.getBranchCommit(branchRev);
            }
            if (isBranchCommit || pendingLastRev) {
                // write back _lastRev with background thread
                tracker.track(path);
            }
        }
        if (isNew && !added.isEmpty()) {
            DocumentNodeState.Children c = new DocumentNodeState.Children();
            Set<String> set = Sets.newTreeSet();
            for (String p : added) {
                set.add(Utils.unshareString(PathUtils.getName(p)));
            }
            c.children.addAll(set);
            PathRev key = childNodeCacheKey(path, rev, null);
            nodeChildrenCache.put(key, c);
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
     * @throws DocumentStoreException  if the update fails with an error.
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
            throws DocumentStoreException  {
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
    DocumentNodeState getRoot(@Nonnull Revision revision) {
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
    Revision rebase(@Nonnull Revision branchHead, @Nonnull Revision base) {
        checkNotNull(branchHead);
        checkNotNull(base);
        if (disableBranches) {
            return branchHead;
        }
        // TODO conflict handling
        Branch b = getBranches().getBranch(branchHead);
        if (b == null) {
            // empty branch
            return base.asBranchRevision();
        }
        if (b.getBase(branchHead).equals(base)) {
            return branchHead;
        }
        // add a pseudo commit to make sure current head of branch
        // has a higher revision than base of branch
        Revision head = newRevision().asBranchRevision();
        b.rebase(head, base);
        return head;
    }

    @Nonnull
    Revision reset(@Nonnull Revision branchHead,
                   @Nonnull Revision ancestor,
                   @Nullable DocumentNodeStoreBranch branch) {
        checkNotNull(branchHead);
        checkNotNull(ancestor);
        Branch b = getBranches().getBranch(branchHead);
        if (b == null) {
            throw new DocumentStoreException("Empty branch cannot be reset");
        }
        if (!b.getCommits().last().equals(branchHead)) {
            throw new DocumentStoreException(branchHead + " is not the head " +
                    "of a branch");
        }
        if (!b.containsCommit(ancestor)) {
            throw new DocumentStoreException(ancestor + " is not " +
                    "an ancestor revision of " + branchHead);
        }
        if (branchHead.equals(ancestor)) {
            // trivial
            return branchHead;
        }
        boolean success = false;
        Commit commit = newCommit(branchHead, branch);
        try {
            Iterator<Revision> it = b.getCommits().tailSet(ancestor).iterator();
            // first revision is the ancestor (tailSet is inclusive)
            // do not undo changes for this revision
            Revision base = it.next();
            Map<String, UpdateOp> operations = Maps.newHashMap();
            while (it.hasNext()) {
                Revision reset = it.next();
                getRoot(reset).compareAgainstBaseState(getRoot(base),
                        new ResetDiff(reset.asTrunkRevision(), operations));
                UpdateOp rootOp = operations.get("/");
                if (rootOp == null) {
                    rootOp = new UpdateOp(Utils.getIdFromPath("/"), false);
                    NodeDocument.setModified(rootOp, commit.getRevision());
                    operations.put("/", rootOp);
                }
                NodeDocument.removeCollision(rootOp, reset.asTrunkRevision());
                NodeDocument.removeRevision(rootOp, reset.asTrunkRevision());
            }
            // update root document first
            if (store.findAndUpdate(Collection.NODES, operations.get("/")) != null) {
                // clean up in-memory branch data
                // first revision is the ancestor (tailSet is inclusive)
                List<Revision> revs = Lists.newArrayList(b.getCommits().tailSet(ancestor));
                for (Revision r : revs.subList(1, revs.size())) {
                    b.removeCommit(r);
                }
                // successfully updating the root document can be considered
                // as success because the changes are not marked as committed
                // anymore
                success = true;
            }
            operations.remove("/");
            // update remaining documents
            for (UpdateOp op : operations.values()) {
                store.findAndUpdate(Collection.NODES, op);
            }
        } finally {
            if (!success) {
                canceled(commit);
            } else {
                done(commit, true, null);
            }
        }
        return ancestor;
    }

    @Nonnull
    Revision merge(@Nonnull Revision branchHead, @Nullable CommitInfo info)
            throws CommitFailedException {
        Branch b = getBranches().getBranch(branchHead);
        Revision base = branchHead;
        if (b != null) {
            base = b.getBase(branchHead);
        }
        int numBranchCommits = b != null ? b.getCommits().size() : 1;
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
                    throw new CommitFailedException(MERGE, 2,
                            "Conflicting concurrent change. Update operation failed: " + op);
                }
            } else {
                // no commits in this branch -> do nothing
            }
            success = true;
        } finally {
            if (!success) {
                canceled(commit);
            } else {
                done(commit, false, info);
            }
        }
        return commit.getRevision();
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
    boolean compare(@Nonnull final DocumentNodeState node,
                    @Nonnull final DocumentNodeState base,
                    @Nonnull NodeStateDiff diff) {
        if (!AbstractNodeState.comparePropertiesAgainstBaseState(node, base, diff)) {
            return false;
        }
        if (node.hasNoChildren() && base.hasNoChildren()) {
            return true;
        }
        return dispatch(diffCache.getChanges(base.getRootRevision(),
                node.getRootRevision(), node.getPath(),
                new DiffCache.Loader() {
                    @Override
                    public String call() {
                        return diffImpl(base, node);
                    }
                }), node, base, diff);
    }

    String diff(@Nonnull final String fromRevisionId,
                @Nonnull final String toRevisionId,
                @Nonnull final String path) throws DocumentStoreException {
        if (fromRevisionId.equals(toRevisionId)) {
            return "";
        }
        Revision fromRev = Revision.fromString(fromRevisionId);
        Revision toRev = Revision.fromString(toRevisionId);
        final DocumentNodeState from = getNode(path, fromRev);
        final DocumentNodeState to = getNode(path, toRev);
        if (from == null || to == null) {
            // TODO implement correct behavior if the node doesn't/didn't exist
            String msg = String.format("Diff is only supported if the node exists in both cases. " +
                    "Node [%s], fromRev [%s] -> %s, toRev [%s] -> %s",
                    path, fromRev, from != null, toRev, to != null);
            throw new DocumentStoreException(msg);
        }
        String compactDiff = diffCache.getChanges(fromRev, toRev, path,
                new DiffCache.Loader() {
            @Override
            public String call() {
                // calculate the diff
                return diffImpl(from, to);
            }
        });
        JsopWriter writer = new JsopStream();
        diffProperties(from, to, writer);
        JsopTokenizer t = new JsopTokenizer(compactDiff);
        int r;
        do {
            r = t.read();
            switch (r) {
                case '+':
                case '^': {
                    String name = t.readString();
                    t.read(':');
                    t.read('{');
                    t.read('}');
                    writer.tag((char) r).key(concat(path, name));
                    writer.object().endObject().newline();
                    break;
                }
                case '-': {
                    String name = t.readString();
                    writer.tag('-').value(concat(path, name));
                    writer.newline();
                }
            }
        } while (r != JsopReader.END);
        return writer.toString();
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
        return getRoot(headRevision);
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
    public Blob getBlob(String reference) {
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
    public String checkpoint(long lifetime) {
        return checkpoints.create(lifetime).toString();
    }

    @CheckForNull
    @Override
    public NodeState retrieve(@Nonnull String checkpoint) {
        Revision r = Revision.fromString(checkpoint);
        SortedMap<Revision, String> checkpoints = this.checkpoints.getCheckpoints();
        if (checkpoints != null && checkpoints.containsKey(r)) {
            return getRoot(r);
        } else {
            return null;
        }
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
    public Revision.RevisionComparator getRevisionComparator() {
        return revisionComparator;
    }

    @Override
    public int getClusterId() {
        return clusterId;
    }

    @Nonnull
    public Revision getHeadRevision() {
        return headRevision;
    }

    //----------------------< background operations >---------------------------

    /** Used for testing only */
    public void runBackgroundOperations() {
        runBackgroundUpdateOperations();
        runBackgroundReadOperations();
    }

    void runBackgroundUpdateOperations() {
        if (isDisposed.get()) {
            return;
        }
        if (!ENABLE_BACKGROUND_OPS) {
            return;
        }
        try {
            internalRunBackgroundUpdateOperations();
        } catch (RuntimeException e) {
            if (isDisposed.get()) {
                LOG.warn("Background update operation failed: " + e.toString(), e);
                return;
            }
            throw e;
        }
    }

    private synchronized void internalRunBackgroundUpdateOperations() {
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
        BackgroundWriteStats stats = backgroundWrite();
        stats.split = splitTime;
        stats.clean = cleanTime;
        String msg = "Background operations stats ({})";
        if (clock.getTime() - start > TimeUnit.SECONDS.toMillis(10)) {
            // log as info if it took more than 10 seconds
            LOG.info(msg, stats);
        } else {
            LOG.debug(msg, stats);
        }
    }

    //----------------------< background read operations >----------------------

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
    private synchronized void internalRunBackgroundReadOperations() {
        long start = clock.getTime();
        // pull in changes from other cluster nodes
        BackgroundReadStats readStats = backgroundRead(true);
        long readTime = clock.getTime() - start;
        String msg = "Background read operations stats (read:{} {})";
        if (clock.getTime() - start > TimeUnit.SECONDS.toMillis(10)) {
            // log as info if it took more than 10 seconds
            LOG.info(msg, readTime, readStats);
        } else {
            LOG.debug(msg, readTime, readStats);
        }
    }

    /**
     * Renews the cluster lease if necessary.
     *
     * @return {@code true} if the lease was renewed; {@code false} otherwise.
     */
    boolean renewClusterIdLease() {
        return clusterNodeInfo != null && clusterNodeInfo.renewLease();
    }

    /**
     * Updates the info about inactive cluster nodes in
     * {@link #inactiveClusterNodes}.
     */
    void updateClusterState() {
        long now = clock.getTime();
        Set<Integer> inactive = Sets.newHashSet();
        for (ClusterNodeInfoDocument doc : ClusterNodeInfoDocument.all(store)) {
            int cId = doc.getClusterId();
            if (cId != this.clusterId && !doc.isActive()) {
                inactive.add(cId);
            }
        }
        inactiveClusterNodes.keySet().retainAll(inactive);
        for (Integer clusterId : inactive) {
            inactiveClusterNodes.putIfAbsent(clusterId, now);
        }
    }

    /**
     * Returns the cluster nodes currently known to be inactive.
     *
     * @return a map with the cluster id as key and the time in millis when it
     *          was first seen inactive.
     */
    Map<Integer, Long> getInactiveClusterNodes() {
        return new HashMap<Integer, Long>(inactiveClusterNodes);
    }

    /**
     * Perform a background read and make external changes visible.
     *
     * @param dispatchChange whether to dispatch external changes
     *                       to {@link #dispatcher}.
     */
    BackgroundReadStats backgroundRead(boolean dispatchChange) {
        BackgroundReadStats stats = new BackgroundReadStats();
        long time = clock.getTime();
        String id = Utils.getIdFromPath("/");
        NodeDocument doc = store.find(Collection.NODES, id, asyncDelay);
        if (doc == null) {
            return stats;
        }
        Map<Integer, Revision> lastRevMap = doc.getLastRev();
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

        Revision.RevisionComparator revisionComparator = getRevisionComparator();
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node)
        Revision otherSeen = Revision.newRevision(0);

        StringSort externalSort = JournalEntry.newSorter();

        try {
            Map<Revision, Revision> externalChanges = Maps.newHashMap();
            for (Map.Entry<Integer, Revision> e : lastRevMap.entrySet()) {
                int machineId = e.getKey();
                if (machineId == clusterId) {
                    // ignore own lastRev
                    continue;
                }
                Revision r = e.getValue();
                Revision last = lastKnownRevision.get(machineId);
                if (last == null || r.compareRevisionTime(last) > 0) {
                    lastKnownRevision.put(machineId, r);
                    // OAK-2345
                    // only consider as external change if
                    // - the revision changed for the machineId
                    // or
                    // - the revision is within the time frame we remember revisions
                    if (last != null
                            || r.getTimestamp() > revisionPurgeMillis()) {
                        externalChanges.put(r, otherSeen);
                    }
                    // collect external changes
                    if (last != null && externalSort != null) {
                        // add changes for this particular clusterId to the externalSort
                        try {
                            fillExternalChanges(externalSort, last, r, store);
                        } catch (Exception e1) { // OAK-5601 : catch any Exception, not only IOException
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
                        stats.cacheStats = store.invalidateCache(pathToId(externalSort));
                    } catch (Exception ioe) {
                        LOG.error("backgroundRead: got IOException during external sorting/cache invalidation (as a result, invalidating entire cache): "+ioe, ioe);
                        stats.cacheStats = store.invalidateCache();
                    }
                }
                stats.cacheInvalidationTime = clock.getTime() - time;
                time = clock.getTime();

                // make sure update to revision comparator is atomic
                // and no local commit is in progress
                backgroundOperationLock.writeLock().lock();
                try {
                    stats.lock = clock.getTime() - time;

                    // the latest revisions of the current cluster node
                    // happened before the latest revisions of other cluster nodes
                    revisionComparator.add(newRevision(), headSeen);
                    // then we saw other revisions
                    for (Map.Entry<Revision, Revision> e : externalChanges.entrySet()) {
                        revisionComparator.add(e.getKey(), e.getValue());
                    }

                    Revision oldHead = headRevision;
                    // the new head revision is after other revisions
                    setHeadRevision(newRevision());
                    if (dispatchChange) {
                        time = clock.getTime();
                        if (externalSort != null) {
                            // then there were external changes and reading them
                            // was successful -> apply them to the diff cache
                            try {
                                JournalEntry.applyTo(externalSort, diffCache, oldHead, headRevision);
                            } catch (Exception e1) {
                                LOG.error("backgroundRead: Exception while processing external changes from journal: "+e1, e1);
                            }
                        }
                        stats.populateDiffCache = clock.getTime() - time;
                        time = clock.getTime();

                        dispatcher.contentChanged(getRoot().fromExternalChange(), null);
                    }
                } finally {
                    backgroundOperationLock.writeLock().unlock();
                }
                stats.dispatchChanges = clock.getTime() - time;
                time = clock.getTime();
            }
        } finally {
            IOUtils.closeQuietly(externalSort);
        }
        revisionComparator.purge(revisionPurgeMillis());
        stats.purge = clock.getTime() - time;

        return stats;
    }

    private static class BackgroundReadStats {
        CacheInvalidationStats cacheStats;
        long readHead;
        long cacheInvalidationTime;
        long populateDiffCache;
        long lock;
        long dispatchChanges;
        long purge;

        @Override
        public String toString() {
            String cacheStatsMsg = "NOP";
            if (cacheStats != null){
                cacheStatsMsg = cacheStats.summaryReport();
            }
            return  "ReadStats{" +
                    "cacheStats:" + cacheStatsMsg +
                    ", head:" + readHead +
                    ", cache:" + cacheInvalidationTime +
                    ", diff: " + populateDiffCache +
                    ", lock:" + lock +
                    ", dispatch:" + dispatchChanges +
                    ", purge:" + purge +
                    '}';
        }
    }

    /**
     * Returns the time in milliseconds when revisions can be purged from the
     * revision comparator.
     *
     * @return time in milliseconds.
     */
    private static long revisionPurgeMillis() {
        return Revision.getCurrentTimestamp() - REMEMBER_REVISION_ORDER_MILLIS;
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
        Revision head = getHeadRevision();
        Map<Revision, String> map = root.getLocalMap(NodeDocument.COLLISIONS);
        UpdateOp op = new UpdateOp(id, false);
        for (Revision r : map.keySet()) {
            if (r.getClusterId() == clusterId) {
                // remove collision if there is no active branch with
                // this revision and the revision is before the current
                // head. That is, the collision cannot be related to commit
                // which is progress.
                if (branches.getBranchCommit(r) == null
                        && isRevisionNewer(head, r)) {
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
        Revision head = getHeadRevision();
        for (Iterator<String> it = splitCandidates.keySet().iterator(); it.hasNext();) {
            String id = it.next();
            NodeDocument doc = store.find(Collection.NODES, id);
            if (doc == null) {
                continue;
            }
            for (UpdateOp op : doc.split(this, head)) {
                NodeDocument before = store.createOrUpdate(Collection.NODES, op);
                if (before != null) {
                    NodeDocument after = store.find(Collection.NODES, op.getId());
                    if (after != null) {
                        LOG.debug("Split operation on {}. Size before: {}, after: {}",
                                id, before.getMemory(), after.getMemory());
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
                    changes = JOURNAL.newDocument(getDocumentStore());
                } else {
                    // fail: log and keep the changes
                    LOG.error("Failed to write to journal, accumulating changes for future write (~" + changes.getMemory()
                            + " bytes).");
                }
            }
        }, backgroundOperationLock.writeLock());
    }

    //-----------------------------< internal >---------------------------------

    @Nonnull
    private Commit newTrunkCommit(@Nonnull Revision base) {
        checkArgument(!checkNotNull(base).isBranch(),
                "base must not be a branch revision: " + base);

        backgroundOperationLock.readLock().lock();
        boolean success = false;
        Commit c;
        try {
            checkOpen();
            c = new Commit(this, commitQueue.createRevision(), base, null);
            success = true;
        } finally {
            if (!success) {
                backgroundOperationLock.readLock().unlock();
            }
        }
        return c;
    }

    @Nonnull
    private Commit newBranchCommit(@Nonnull Revision base,
                                   @Nullable DocumentNodeStoreBranch branch) {
        checkArgument(checkNotNull(base).isBranch(),
                "base must be a branch revision: " + base);

        checkOpen();
        return new Commit(this, newRevision(), base, branch);
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
    private boolean checkNodeNotExistsFromChildrenCache(String path, Revision rev) {
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

    private boolean dispatch(@Nonnull final String jsonDiff,
                             @Nonnull final DocumentNodeState node,
                             @Nonnull final DocumentNodeState base,
                             @Nonnull final NodeStateDiff diff) {
        return DiffCache.parseJsopDiff(jsonDiff, new DiffCache.Diff() {
            @Override
            public boolean childNodeAdded(String name) {
                return diff.childNodeAdded(name,
                        node.getChildNode(name));
            }

            @Override
            public boolean childNodeChanged(String name) {
                boolean continueComparison = true;
                NodeState baseChild = base.getChildNode(name);
                NodeState nodeChild = node.getChildNode(name);
                if (baseChild.exists()) {
                    if (nodeChild.exists()) {
                        continueComparison = diff.childNodeChanged(name,
                                baseChild, nodeChild);
                    } else {
                        continueComparison = diff.childNodeDeleted(name,
                                baseChild);
                    }
                } else {
                    if (nodeChild.exists()) {
                        continueComparison = diff.childNodeAdded(name,
                                nodeChild);
                    }
                }
                return continueComparison;
            }

            @Override
            public boolean childNodeDeleted(String name) {
                return diff.childNodeDeleted(name,
                        base.getChildNode(name));
            }
        });
    }

    /**
     * Creates a tracker for the given commit revision.
     *
     * @param r a commit revision.
     * @return a _lastRev tracker for the given commit revision.
     */
    private LastRevTracker createTracker(final @Nonnull Revision r) {
        return new LastRevTracker() {
            @Override
            public void track(String path) {
                unsavedLastRevisions.put(path, r);
            }
        };
    }

    private static void diffProperties(DocumentNodeState from,
                                       DocumentNodeState to,
                                       JsopWriter w) {
        for (PropertyState fromValue : from.getProperties()) {
            String name = fromValue.getName();
            // changed or removed properties
            PropertyState toValue = to.getProperty(name);
            if (!fromValue.equals(toValue)) {
                w.tag('^').key(concat(from.getPath(), name));
                if (toValue == null) {
                    w.value(null);
                } else {
                    w.encodedValue(to.getPropertyAsString(name)).newline();
                }
            }
        }
        for (String name : to.getPropertyNames()) {
            // added properties
            if (!from.hasProperty(name)) {
                w.tag('^').key(concat(from.getPath(), name))
                        .encodedValue(to.getPropertyAsString(name)).newline();
            }
        }
    }

    private String diffImpl(DocumentNodeState from, DocumentNodeState to)
            throws DocumentStoreException {
        JsopWriter w = new JsopStream();
        // TODO this does not work well for large child node lists
        // use a document store index instead
        int max = MANY_CHILDREN_THRESHOLD;

        final boolean debug = LOG.isDebugEnabled();
        final long start = debug ? now() : 0;

        DocumentNodeState.Children fromChildren, toChildren;
        fromChildren = getChildren(from, null, max);
        toChildren = getChildren(to, null, max);

        final long getChildrenDoneIn = debug ? now() : 0;

        String diffAlgo;
        Revision fromRev = from.getLastRevision();
        Revision toRev = to.getLastRevision();
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

        String diff = w.toString();
        if (debug) {
            long end = now();
            LOG.debug("Diff performed via '{}' at [{}] between revisions [{}] => [{}] took {} ms ({} ms), diff '{}'",
                    diffAlgo, from.getPath(), fromRev, toRev,
                    end - start, getChildrenDoneIn - start, diff);
        }
        return diff;
    }

    private void diffManyChildren(JsopWriter w, String path, Revision fromRev, Revision toRev) {
        long minTimestamp = Math.min(
                revisionComparator.getMinimumTimestamp(fromRev, inactiveClusterNodes),
                revisionComparator.getMinimumTimestamp(toRev, inactiveClusterNodes));
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
        for (Revision r : new Revision[]{fromRev, toRev}) {
            if (r.isBranch()) {
                Branch b = branches.getBranch(r);
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
                    Revision a = fromNode.getLastRevision();
                    Revision b = toNode.getLastRevision();
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

    private void diffFewChildren(JsopWriter w, String parentPath, DocumentNodeState.Children fromChildren, Revision fromRev, DocumentNodeState.Children toChildren, Revision toRev) {
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
                                             @Nonnull Revision readRevision,
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

    private void moveOrCopyNode(boolean move,
                                DocumentNodeState source,
                                String targetPath,
                                Commit commit) {
        // TODO Optimize - Move logic would not work well with very move of very large subtrees
        // At minimum we can optimize by traversing breadth wise and collect node id
        // and fetch them via '$in' queries

        // TODO Transient Node - Current logic does not account for operations which are part
        // of this commit i.e. transient nodes. If its required it would need to be looked
        // into

        DocumentNodeState newNode = new DocumentNodeState(this, targetPath, commit.getRevision());
        source.copyTo(newNode);

        commit.addNode(newNode);
        if (move) {
            markAsDeleted(source, commit, false);
        }
        for (DocumentNodeState child : getChildNodes(source, null, Integer.MAX_VALUE)) {
            String childName = PathUtils.getName(child.getPath());
            String destChildPath = concat(targetPath, childName);
            moveOrCopyNode(move, child, destChildPath, commit);
        }
    }

    private void checkRevisionAge(Revision r, String path) {
        if (LOG.isDebugEnabled()) {
            if ("/".equals(path) && headRevision.getTimestamp() - r.getTimestamp() > WARN_REVISION_AGE) {
                LOG.debug("Requesting an old revision for path " + path + ", " +
                        ((headRevision.getTimestamp() - r.getTimestamp()) / 1000) + " seconds old");
            }
        }
    }

    /**
     * Creates and returns a MarkSweepGarbageCollector if the current BlobStore
     * supports garbage collection
     *
     * @return garbage collector of the BlobStore supports GC otherwise null
     * @param blobGcMaxAgeInSecs
     */
    @CheckForNull
    public MarkSweepGarbageCollector createBlobGarbageCollector(long blobGcMaxAgeInSecs) {
        MarkSweepGarbageCollector blobGC = null;
        if(blobStore instanceof GarbageCollectableBlobStore){
            try {
                blobGC = new MarkSweepGarbageCollector(
                        new DocumentBlobReferenceRetriever(this),
                            (GarbageCollectableBlobStore) blobStore,
                        executor,
                        TimeUnit.SECONDS.toMillis(blobGcMaxAgeInSecs));
            } catch (IOException e) {
                throw new RuntimeException("Error occurred while initializing " +
                        "the MarkSweepGarbageCollector",e);
            }
        }
        return blobGC;
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
            return revisionComparator.toString();
        }

        @Override
        public String getHead(){
            return headRevision.toString();
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
            return toArray(transform(inactiveClusterNodes.entrySet(),
                    new Function<Map.Entry<Integer, Long>, String>() {
                        @Override
                        public String apply(Map.Entry<Integer, Long> input) {
                            return input.toString();
                        }
                    }), String.class);
        }

        @Override
        public String[] getLastKnownRevisions() {
            return toArray(transform(lastKnownRevision.entrySet(),
                    new Function<Map.Entry<Integer, Revision>, String>() {
                        @Override
                        public String apply(Map.Entry<Integer, Revision> input) {
                            return input.toString();
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
    }

    static abstract class NodeStoreTask implements Runnable {
        final WeakReference<DocumentNodeStore> ref;
        private final AtomicBoolean isDisposed;
        private int delay;

        NodeStoreTask(DocumentNodeStore nodeStore, AtomicBoolean isDisposed) {
            ref = new WeakReference<DocumentNodeStore>(nodeStore);
            delay = nodeStore.getAsyncDelay();
            this.isDisposed = isDisposed;
        }

        protected abstract void execute(@Nonnull DocumentNodeStore nodeStore);

        @Override
        public void run() {
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
                    delay = nodeStore.getAsyncDelay();
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

        /** OAK-4859 : log if time between two renewClusterIdLease calls is too long **/
        private long lastRenewClusterIdLeaseCall = -1;
        
        BackgroundLeaseUpdate(DocumentNodeStore nodeStore,
                              AtomicBoolean isDisposed) {
            super(nodeStore, isDisposed);
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

            if (nodeStore.renewClusterIdLease()) {
                nodeStore.updateClusterState();
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
    public Iterator<Blob> getReferencedBlobsIterator() {
        if(store instanceof MongoDocumentStore){
            return new MongoBlobReferenceIterator(this, (MongoDocumentStore) store);
        }
        return new BlobReferenceIterator(this);
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

    public void setPersistentCache(PersistentCache persistentCache) {
        this.persistentCache = persistentCache;
    }
}
