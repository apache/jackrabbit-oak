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
import static org.apache.jackrabbit.oak.api.CommitFailedException.MERGE;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.FAST_DIFF;
import static org.apache.jackrabbit.oak.plugins.document.DocumentMK.MANY_CHILDREN_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobReferenceIterator;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.document.util.LoggingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a NodeStore on {@link DocumentStore}.
 */
public final class DocumentNodeStore
        implements NodeStore, RevisionContext, Observable {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentNodeStore.class);

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
     * How long to remember the relative order of old revision of all cluster
     * nodes, in milliseconds. The default is one hour.
     */
    private static final int REMEMBER_REVISION_ORDER_MILLIS = 60 * 60 * 1000;

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
     * The comparator for revisions.
     */
    private final Revision.RevisionComparator revisionComparator;

    /**
     * Unmerged branches of this DocumentNodeStore instance.
     */
    // TODO at some point, open (unmerged) branches
    // need to be garbage collected (in-memory and on disk)
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

    private Thread backgroundThread;

    /**
     * Read/Write lock for background operations. Regular commits will acquire
     * a shared lock, while a background write acquires an exclusive lock.
     */
    private final ReadWriteLock backgroundOperationLock = new ReentrantReadWriteLock();

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
    private final Cache<CacheValue, DocumentNodeState> nodeCache;
    private final CacheStats nodeCacheStats;

    /**
     * Child node cache.
     *
     * Key: PathRev, value: Children
     */
    private final Cache<CacheValue, DocumentNodeState.Children> nodeChildrenCache;
    private final CacheStats nodeChildrenCacheStats;

    /**
     * Child doc cache.
     *
     * Key: StringValue, value: Children
     */
    private final Cache<CacheValue, NodeDocument.Children> docChildrenCache;
    private final CacheStats docChildrenCacheStats;

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
                return ((BlobStoreBlob) blob).getBlobId();
            }
            String id;
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

    private final Executor executor;

    private final LastRevRecoveryAgent lastRevRecoveryAgent;

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
        this.versionGarbageCollector = new VersionGarbageCollector(this);
        this.lastRevRecoveryAgent = new LastRevRecoveryAgent(this);
        this.missing = new DocumentNodeState(this, "MISSING", new Revision(0, 0, 0)) {
            @Override
            public int getMemory() {
                return 8;
            }
        };

        //TODO Make stats collection configurable as it add slight overhead

        nodeCache = builder.buildCache(builder.getNodeCacheSize());
        nodeCacheStats = new CacheStats(nodeCache, "Document-NodeState",
                builder.getWeigher(), builder.getNodeCacheSize());

        nodeChildrenCache = builder.buildCache(builder.getChildrenCacheSize());
        nodeChildrenCacheStats = new CacheStats(nodeChildrenCache, "Document-NodeChildren",
                builder.getWeigher(), builder.getChildrenCacheSize());

        docChildrenCache = builder.buildCache(builder.getDocChildrenCacheSize());
        docChildrenCacheStats = new CacheStats(docChildrenCache, "Document-DocChildren",
                builder.getWeigher(), builder.getDocChildrenCacheSize());

        diffCache = builder.getDiffCache();
        checkpoints = new Checkpoints(this);

        // check if root node exists
        if (store.find(Collection.NODES, Utils.getIdFromPath("/")) == null) {
            // root node is missing: repository is not initialized
            Revision head = newRevision();
            Commit commit = new Commit(this, null, head);
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
        }
        getRevisionComparator().add(headRevision, Revision.newRevision(0));

        dispatcher = new ChangeDispatcher(getRoot());
        commitQueue = new CommitQueue(this, dispatcher);
        batchCommitQueue = new BatchCommitQueue(store, revisionComparator);
        backgroundThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "DocumentNodeStore background thread");
        backgroundThread.setDaemon(true);
        checkLastRevRecovery();
        // Renew the lease because it may have been stale
        backgroundRenewClusterIdLease();

        backgroundThread.start();

        LOG.info("Initialized DocumentNodeStore with clusterNodeId: {}", clusterId);
    }

    /**
     * Recover _lastRev recovery if needed.
     */
    private void checkLastRevRecovery() {
        lastRevRecoveryAgent.recover(clusterId);
    }

    public void dispose() {
        runBackgroundOperations();
        if (!isDisposed.getAndSet(true)) {
            synchronized (isDisposed) {
                isDisposed.notifyAll();
            }
            try {
                backgroundThread.join();
            } catch (InterruptedException e) {
                // ignore
            }
            if (clusterNodeInfo != null) {
                clusterNodeInfo.dispose();
            }
            store.dispose();
            LOG.info("Disposed DocumentNodeStore with clusterNodeId: {}", clusterId);

            if (blobStore instanceof Closeable) {
                try {
                    ((Closeable) blobStore).close();
                } catch (IOException ex) {
                    LOG.debug("Error closing blob store " + blobStore, ex);
                }
            }
        }
    }

    @Nonnull
    Revision getHeadRevision() {
        return headRevision;
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
     * @return a new commit.
     */
    @Nonnull
    Commit newCommit(@Nullable Revision base) {
        if (base == null) {
            base = headRevision;
        }
        backgroundOperationLock.readLock().lock();
        boolean success = false;
        Commit c;
        try {
            c = new Commit(this, base, commitQueue.createRevision());
            success = true;
        } finally {
            if (!success) {
                backgroundOperationLock.readLock().unlock();
            }
        }
        return c;
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
            c = new MergeCommit(this, base, commitQueue.createRevisions(numBranchCommits));
            success = true;
        } finally {
            if (!success) {
                backgroundOperationLock.readLock().unlock();
            }
        }
        return c;
    }

    void done(@Nonnull Commit c, boolean isBranch, @Nullable CommitInfo info) {
        try {
            commitQueue.done(c, isBranch, info);
        } finally {
            backgroundOperationLock.readLock().unlock();
        }
    }

    void canceled(Commit c) {
        try {
            commitQueue.canceled(c.getRevision());
        } finally {
            backgroundOperationLock.readLock().unlock();
        }
    }

    public void setAsyncDelay(int delay) {
        this.asyncDelay = delay;
    }

    public int getAsyncDelay() {
        return asyncDelay;
    }

    public ClusterNodeInfo getClusterInfo() {
        return clusterNodeInfo;
    }

    public CacheStats getNodeCacheStats() {
        return nodeCacheStats;
    }

    public CacheStats getNodeChildrenCacheStats() {
        return nodeChildrenCacheStats;
    }

    public CacheStats getDocChildrenCacheStats() {
        return docChildrenCacheStats;
    }

    public int getPendingWriteCount() {
        return unsavedLastRevisions.getPaths().size();
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
        try {
            PathRev key = new PathRev(path, rev);
            DocumentNodeState node = nodeCache.get(key, new Callable<DocumentNodeState>() {
                @Override
                public DocumentNodeState call() throws Exception {
                    DocumentNodeState n = readNode(path, rev);
                    if (n == null) {
                        n = missing;
                    }
                    return n;
                }
            });
            return node == missing ? null : node;
        } catch (ExecutionException e) {
            throw new MicroKernelException(e);
        }
    }

    DocumentNodeState.Children getChildren(@Nonnull final DocumentNodeState parent,
                              @Nullable final String name,
                              final int limit)
            throws MicroKernelException {
        if (checkNotNull(parent).hasNoChildren()) {
            return DocumentNodeState.NO_CHILDREN;
        }
        final String path = checkNotNull(parent).getPath();
        final Revision readRevision = parent.getLastRevision();
        PathRev key = childNodeCacheKey(path, readRevision, name);
        DocumentNodeState.Children children;
        for (;;) {
            try {
                children = nodeChildrenCache.get(key, new Callable<DocumentNodeState.Children>() {
                    @Override
                    public DocumentNodeState.Children call() throws Exception {
                        return readChildren(parent, name, limit);
                    }
                });
            } catch (ExecutionException e) {
                throw new MicroKernelException(
                        "Error occurred while fetching children for path "
                                + path, e.getCause());
            }
            if (children.hasMore && limit > children.children.size()) {
                // there are potentially more children and
                // current cache entry contains less than requested limit
                // -> need to refresh entry with current limit
                nodeChildrenCache.invalidate(key);
            } else {
                // use this cache entry
                break;
            }
        }
        return children;
    }

    DocumentNodeState.Children readChildren(DocumentNodeState parent, String name, int limit) {
        // TODO use offset, to avoid O(n^2) and running out of memory
        // to do that, use the *name* of the last entry of the previous batch of children
        // as the starting point
        String path = parent.getPath();
        Revision rev = parent.getLastRevision();
        Iterable<NodeDocument> docs;
        DocumentNodeState.Children c = new DocumentNodeState.Children();
        // add one to the requested limit for the raw limit
        // this gives us a chance to detect whether there are more
        // child nodes than requested.
        int rawLimit = (int) Math.min(Integer.MAX_VALUE, ((long) limit) + 1);
        for (;;) {
            c.children.clear();
            docs = readChildDocs(path, name, rawLimit);
            int numReturned = 0;
            for (NodeDocument doc : docs) {
                numReturned++;
                // filter out deleted children
                String p = doc.getPath();
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
                return c;
            }
            // double rawLimit for next round
            rawLimit = (int) Math.min(((long) rawLimit) * 2, Integer.MAX_VALUE);
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
    Iterable<NodeDocument> readChildDocs(@Nonnull final String path,
                                         @Nullable String name,
                                         int limit) {
        String to = Utils.getKeyUpperLimit(checkNotNull(path));
        String from;
        if (name != null) {
            from = Utils.getIdFromPath(PathUtils.concat(path, name));
        } else {
            from = Utils.getKeyLowerLimit(path);
        }
        if (name != null || limit > NUM_CHILDREN_CACHE_LIMIT) {
            // do not use cache when there is a lower bound name
            // or more than 16k child docs are requested
            return store.query(Collection.NODES, from, to, limit);
        }
        CacheValue key = new StringValue(path);
        // check cache
        NodeDocument.Children c = docChildrenCache.getIfPresent(key);
        if (c == null) {
            c = new NodeDocument.Children();
            List<NodeDocument> docs = store.query(Collection.NODES, from, to, limit);
            for (NodeDocument doc : docs) {
                String p = doc.getPath();
                c.childNames.add(PathUtils.getName(p));
            }
            c.isComplete = docs.size() < limit;
            docChildrenCache.put(key, c);
            return docs;
        } else if (c.childNames.size() < limit && !c.isComplete) {
            // fetch more and update cache
            String lastName = c.childNames.get(c.childNames.size() - 1);
            String lastPath = PathUtils.concat(path, lastName);
            from = Utils.getIdFromPath(lastPath);
            int remainingLimit = limit - c.childNames.size();
            List<NodeDocument> docs = store.query(Collection.NODES,
                    from, to, remainingLimit);
            NodeDocument.Children clone = c.clone();
            for (NodeDocument doc : docs) {
                String p = doc.getPath();
                clone.childNames.add(PathUtils.getName(p));
            }
            clone.isComplete = docs.size() < remainingLimit;
            docChildrenCache.put(key, clone);
            c = clone;
        }
        Iterable<NodeDocument> it = Iterables.transform(c.childNames, new Function<String, NodeDocument>() {
            @Override
            public NodeDocument apply(String name) {
                String p = PathUtils.concat(path, name);
                NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath(p));
                if (doc == null) {
                    throw new NullPointerException("Document " + p + " not found");
                }
                return doc;
            }
        });
        if (c.childNames.size() > limit * 2) {
            it = Iterables.limit(it, limit * 2);
        }
        return it;
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
        return Iterables.transform(getChildren(parent, name, limit).children,
                new Function<String, DocumentNodeState>() {
            @Override
            public DocumentNodeState apply(String input) {
                String p = PathUtils.concat(parent.getPath(), input);
                return getNode(p, readRevision);
            }
        });
    }

    @CheckForNull
    DocumentNodeState readNode(String path, Revision readRevision) {
        String id = Utils.getIdFromPath(path);
        Revision lastRevision = getPendingModifications().get(path);
        NodeDocument doc = store.find(Collection.NODES, id);
        if (doc == null) {
            return null;
        }
        return doc.getNodeAtRevision(this, readRevision, lastRevision);
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
    public void applyChanges(Revision rev, String path,
                             boolean isNew, boolean pendingLastRev,
                             boolean isBranchCommit, List<String> added,
                             List<String> removed, List<String> changed,
                             DiffCache.Entry cacheEntry) {
        UnsavedModifications unsaved = unsavedLastRevisions;
        if (isBranchCommit) {
            Revision branchRev = rev.asBranchRevision();
            unsaved = branches.getBranch(branchRev).getModifications(branchRev);
        }
        if (isBranchCommit || pendingLastRev) {
            // write back _lastRev with background thread
            unsaved.put(path, rev);
        }
        if (isNew) {
            CacheValue key = childNodeCacheKey(path, rev, null);
            DocumentNodeState.Children c = new DocumentNodeState.Children();
            Set<String> set = Sets.newTreeSet();
            for (String p : added) {
                set.add(Utils.unshareString(PathUtils.getName(p)));
            }
            c.children.addAll(set);
            nodeChildrenCache.put(key, c);
        }

        // update diff cache
        JsopWriter w = new JsopStream();
        for (String p : added) {
            w.tag('+').key(PathUtils.getName(p)).object().endObject().newline();
        }
        for (String p : removed) {
            w.tag('-').value(PathUtils.getName(p)).newline();
        }
        for (String p : changed) {
            w.tag('^').key(PathUtils.getName(p)).object().endObject().newline();
        }
        cacheEntry.append(path, w.toString());

        // update docChildrenCache
        if (!added.isEmpty()) {
            CacheValue docChildrenKey = new StringValue(path);
            NodeDocument.Children docChildren = docChildrenCache.getIfPresent(docChildrenKey);
            if (docChildren != null) {
                int currentSize = docChildren.childNames.size();
                NavigableSet<String> names = Sets.newTreeSet(docChildren.childNames);
                // incomplete cache entries must not be updated with
                // names at the end of the list because there might be
                // a next name in DocumentStore smaller than the one added
                if (!docChildren.isComplete) {
                    for (String childPath : added) {
                        String name = PathUtils.getName(childPath);
                        if (names.higher(name) != null) {
                            names.add(Utils.unshareString(name));
                        }
                    }
                } else {
                    // add all
                    for (String childPath : added) {
                        names.add(Utils.unshareString(PathUtils.getName(childPath)));
                    }
                }
                // any changes?
                if (names.size() != currentSize) {
                    // create new cache entry with updated names
                    boolean complete = docChildren.isComplete;
                    docChildren = new NodeDocument.Children();
                    docChildren.isComplete = complete;
                    docChildren.childNames.addAll(names);
                    docChildrenCache.put(docChildrenKey, docChildren);
                }
            }
        }
    }

    /**
     * Updates a commit root document.
     *
     * @param commit the updates to apply on the commit root document.
     * @return the document before the update was applied or <code>null</code>
     *          if the update failed because of a collision.
     * @throws MicroKernelException if the update fails with an error.
     */
    @CheckForNull
    NodeDocument updateCommitRoot(UpdateOp commit) throws MicroKernelException {
        // use batch commit when there are only revision and modified updates
        // and collision checks
        boolean batch = true;
        for (Map.Entry<Key, Operation> op : commit.getChanges().entrySet()) {
            String name = op.getKey().getName();
            if (NodeDocument.isRevisionsEntry(name)
                    || NodeDocument.MODIFIED_IN_SECS.equals(name)
                    || NodeDocument.COLLISIONS.equals(name)) {
                continue;
            }
            batch = false;
            break;
        }
        if (batch) {
            return batchUpdateCommitRoot(commit);
        } else {
            return store.findAndUpdate(NODES, commit);
        }
    }

    private NodeDocument batchUpdateCommitRoot(UpdateOp commit)
            throws MicroKernelException {
        try {
            return batchCommitQueue.updateDocument(commit).call();
        } catch (InterruptedException e) {
            throw new MicroKernelException("Interrupted while updating commit root document");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MicroKernelException) {
                throw (MicroKernelException) e.getCause();
            } else {
                String msg = "Update of commit root document failed";
                throw new MicroKernelException(msg, e.getCause());
            }
        } catch (Exception e) {
            if (e instanceof MicroKernelException) {
                throw (MicroKernelException) e;
            } else {
                String msg = "Update of commit root document failed";
                throw new MicroKernelException(msg, e);
            }
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
        return new DocumentNodeStoreBranch(this, base, mergeLock);
    }

    @Nonnull
    Revision rebase(@Nonnull Revision branchHead, @Nonnull Revision base) {
        checkNotNull(branchHead);
        checkNotNull(base);
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
    Revision reset(@Nonnull Revision branchHead, @Nonnull Revision ancestor) {
        checkNotNull(branchHead);
        checkNotNull(ancestor);
        Branch b = getBranches().getBranch(branchHead);
        if (b == null) {
            throw new MicroKernelException("Empty branch cannot be reset");
        }
        if (!b.getCommits().last().equals(branchHead)) {
            throw new MicroKernelException(branchHead + " is not the head " +
                    "of a branch");
        }
        if (!b.containsCommit(ancestor)) {
            throw new MicroKernelException(ancestor + " is not " +
                    "an ancestor revision of " + branchHead);
        }
        if (branchHead.equals(ancestor)) {
            // trivial
            return branchHead;
        }
        boolean success = false;
        Commit commit = newCommit(branchHead);
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
     * reports the differences on the children as a json diff string. This
     * method does not report any property changes between the two nodes.
     *
     * @param node the node to compare.
     * @param base the base node to compare against.
     * @return the json diff.
     */
    String diffChildren(@Nonnull final DocumentNodeState node,
                        @Nonnull final DocumentNodeState base) {
        if (node.hasNoChildren() && base.hasNoChildren()) {
            return "";
        }
        String diff = diffCache.getChanges(base.getLastRevision(),
                node.getLastRevision(), node.getPath());
        if (diff == null) {
            diff = diffImpl(base, node);
        }
        return diff;
    }

    String diff(@Nonnull final String fromRevisionId,
                @Nonnull final String toRevisionId,
                @Nonnull final String path) throws MicroKernelException {
        if (fromRevisionId.equals(toRevisionId)) {
            return "";
        }
        Revision fromRev = Revision.fromString(fromRevisionId);
        Revision toRev = Revision.fromString(toRevisionId);
        final DocumentNodeState from = getNode(path, fromRev);
        final DocumentNodeState to = getNode(path, toRev);
        if (from == null || to == null) {
            // TODO implement correct behavior if the node does't/didn't exist
            String msg = String.format("Diff is only supported if the node exists in both cases. " +
                    "Node [%s], fromRev [%s] -> %s, toRev [%s] -> %s",
                    path, fromRev, from != null, toRev, to != null);
            throw new MicroKernelException(msg);
        }
        String compactDiff = diffCache.getChanges(fromRev, toRev, path);
        if (compactDiff == null) {
            // calculate the diff
            compactDiff = diffImpl(from, to);
        }
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
                    writer.tag((char) r).key(PathUtils.concat(path, name));
                    writer.object().endObject().newline();
                    break;
                }
                case '-': {
                    String name = t.readString();
                    writer.tag('-').value(PathUtils.concat(path, name));
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
        return getRoot(Revision.fromString(checkpoint));
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

    //----------------------< background operations >---------------------------

    public synchronized void runBackgroundOperations() {
        if (isDisposed.get()) {
            return;
        }
        backgroundRenewClusterIdLease();
        if (simpleRevisionCounter != null) {
            // only when using timestamp
            return;
        }
        if (!ENABLE_BACKGROUND_OPS) {
            return;
        }
        try {
            // split documents (does not create new revisions)
            backgroundSplit();
            // write back pending updates to _lastRev
            backgroundWrite();
            // pull in changes from other cluster nodes
            backgroundRead(true);
        } catch (RuntimeException e) {
            if (isDisposed.get()) {
                return;
            }
            throw e;
        }
    }

    private void backgroundRenewClusterIdLease() {
        if (clusterNodeInfo == null) {
            return;
        }
        clusterNodeInfo.renewLease(asyncDelay);
    }

    /**
     * Perform a background read and make external changes visible.
     *
     * @param dispatchChange whether to dispatch external changes
     *                       to {@link #dispatcher}.
     */
    void backgroundRead(boolean dispatchChange) {
        String id = Utils.getIdFromPath("/");
        NodeDocument doc = store.find(Collection.NODES, id, asyncDelay);
        if (doc == null) {
            return;
        }
        Map<Integer, Revision> lastRevMap = doc.getLastRev();

        Revision.RevisionComparator revisionComparator = getRevisionComparator();
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node)
        Revision otherSeen = Revision.newRevision(0);

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
                externalChanges.put(r, otherSeen);
            }
        }

        if (!externalChanges.isEmpty()) {
            // invalidate caches
            store.invalidateCache();
            // TODO only invalidate affected items
            docChildrenCache.invalidateAll();

            // make sure update to revision comparator is atomic
            // and no local commit is in progress
            backgroundOperationLock.writeLock().lock();
            try {
                // the latest revisions of the current cluster node
                // happened before the latest revisions of other cluster nodes
                revisionComparator.add(newRevision(), headSeen);
                // then we saw other revisions
                for (Map.Entry<Revision, Revision> e : externalChanges.entrySet()) {
                    revisionComparator.add(e.getKey(), e.getValue());
                }
                // the new head revision is after other revisions
                setHeadRevision(newRevision());
                if (dispatchChange) {
                    dispatcher.contentChanged(getRoot(), null);
                }
            } finally {
                backgroundOperationLock.writeLock().unlock();
            }
        }
        revisionComparator.purge(Revision.getCurrentTimestamp() - REMEMBER_REVISION_ORDER_MILLIS);
    }

    private void backgroundSplit() {
        for (Iterator<String> it = splitCandidates.keySet().iterator(); it.hasNext();) {
            String id = it.next();
            NodeDocument doc = store.find(Collection.NODES, id);
            if (doc == null) {
                continue;
            }
            for (UpdateOp op : doc.split(this)) {
                NodeDocument before = store.createOrUpdate(Collection.NODES, op);
                if (before != null) {
                    NodeDocument after = store.find(Collection.NODES, op.getId());
                    if (after != null) {
                        LOG.info("Split operation on {}. Size before: {}, after: {}",
                                id, before.getMemory(), after.getMemory());
                    }
                }
            }
            it.remove();
        }
    }

    void backgroundWrite() {
        unsavedLastRevisions.persist(this, backgroundOperationLock.writeLock());
    }

    //-----------------------------< internal >---------------------------------

    private static void diffProperties(DocumentNodeState from,
                                       DocumentNodeState to,
                                       JsopWriter w) {
        for (PropertyState fromValue : from.getProperties()) {
            String name = fromValue.getName();
            // changed or removed properties
            PropertyState toValue = to.getProperty(name);
            if (!fromValue.equals(toValue)) {
                w.tag('^').key(PathUtils.concat(from.getPath(), name));
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
                w.tag('^').key(PathUtils.concat(from.getPath(), name))
                        .encodedValue(to.getPropertyAsString(name)).newline();
            }
        }
    }

    private String diffImpl(DocumentNodeState from, DocumentNodeState to)
            throws MicroKernelException {
        JsopWriter w = new JsopStream();
        // TODO this does not work well for large child node lists
        // use a document store index instead
        int max = MANY_CHILDREN_THRESHOLD;
        DocumentNodeState.Children fromChildren, toChildren;
        fromChildren = getChildren(from, null, max);
        toChildren = getChildren(to, null, max);
        if (!fromChildren.hasMore && !toChildren.hasMore) {
            diffFewChildren(w, from.getPath(), fromChildren,
                    from.getLastRevision(), toChildren, to.getLastRevision());
        } else {
            if (FAST_DIFF) {
                diffManyChildren(w, from.getPath(),
                        from.getLastRevision(), to.getLastRevision());
            } else {
                max = Integer.MAX_VALUE;
                fromChildren = getChildren(from, null, max);
                toChildren = getChildren(to, null, max);
                diffFewChildren(w, from.getPath(), fromChildren,
                        from.getLastRevision(), toChildren, to.getLastRevision());
            }
        }
        return w.toString();
    }

    private void diffManyChildren(JsopWriter w, String path, Revision fromRev, Revision toRev) {
        long minTimestamp = Math.min(fromRev.getTimestamp(), toRev.getTimestamp());
        long minValue = Commit.getModifiedInSecs(minTimestamp);
        String fromKey = Utils.getKeyLowerLimit(path);
        String toKey = Utils.getKeyUpperLimit(path);
        Set<String> paths = Sets.newHashSet();
        for (NodeDocument doc : store.query(Collection.NODES, fromKey, toKey,
                NodeDocument.MODIFIED_IN_SECS, minValue, Integer.MAX_VALUE)) {
            paths.add(doc.getPath());
        }
        // also consider nodes with not yet stored modifications (OAK-1107)
        Revision minRev = new Revision(minTimestamp, 0, getClusterId());
        addPathsForDiff(path, paths, getPendingModifications(), minRev);
        for (Revision r : new Revision[]{fromRev, toRev}) {
            if (r.isBranch()) {
                Branch b = getBranches().getBranch(r);
                if (b != null) {
                    addPathsForDiff(path, paths, b.getModifications(r), r);
                }
            }
        }
        for (String p : paths) {
            DocumentNodeState fromNode = getNode(p, fromRev);
            DocumentNodeState toNode = getNode(p, toRev);
            String name = PathUtils.getName(p);
            if (fromNode != null) {
                // exists in fromRev
                if (toNode != null) {
                    // exists in both revisions
                    // check if different
                    if (!fromNode.getLastRevision().equals(toNode.getLastRevision())) {
                        w.tag('^').key(name).object().endObject().newline();
                    }
                } else {
                    // does not exist in toRev -> was removed
                    w.tag('-').value(name).newline();
                }
            } else {
                // does not exist in fromRev
                if (toNode != null) {
                    // exists in toRev
                    w.tag('+').key(name).object().endObject().newline();
                } else {
                    // does not exist in either revisions
                    // -> do nothing
                }
            }
        }
    }

    private static void addPathsForDiff(String path,
                                        Set<String> paths,
                                        UnsavedModifications pending,
                                        Revision minRev) {
        for (String p : pending.getPaths(minRev)) {
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
                w.tag('-').value(n).newline();
            } else {
                String path = PathUtils.concat(parentPath, n);
                DocumentNodeState n1 = getNode(path, fromRev);
                DocumentNodeState n2 = getNode(path, toRev);
                // this is not fully correct:
                // a change is detected if the node changed recently,
                // even if the revisions are well in the past
                // if this is a problem it would need to be changed
                checkNotNull(n1, "Node at [%s] not found for fromRev [%s]", path, fromRev);
                checkNotNull(n2, "Node at [%s] not found for toRev [%s]", path, toRev);
                if (!n1.getLastRevision().equals(n2.getLastRevision())) {
                    w.tag('^').key(n).object().endObject().newline();
                }
            }
        }
        childrenSet = Sets.newHashSet(fromChildren.children);
        for (String n : toChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('+').key(n).object().endObject().newline();
            }
        }
    }

    private static PathRev childNodeCacheKey(@Nonnull String path,
                                             @Nonnull Revision readRevision,
                                             @Nullable String name) {
        return new PathRev((name == null ? "" : name) + path, readRevision);
    }

    private static DocumentRootBuilder asDocumentRootBuilder(NodeBuilder builder)
            throws IllegalArgumentException {
        if (!(builder instanceof DocumentRootBuilder)) {
            throw new IllegalArgumentException("builder must be a " +
                    DocumentRootBuilder.class.getName());
        }
        return (DocumentRootBuilder) builder;
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
            String destChildPath = PathUtils.concat(targetPath, childName);
            moveOrCopyNode(move, child, destChildPath, commit);
        }
    }

    private void checkRevisionAge(Revision r, String path) {
        // TODO only log if there are new revisions available for the given node
        if (LOG.isDebugEnabled()) {
            if (headRevision.getTimestamp() - r.getTimestamp() > WARN_REVISION_AGE) {
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
     */
    @CheckForNull
    public MarkSweepGarbageCollector createBlobGarbageCollector() {
        MarkSweepGarbageCollector blobGC = null;
        if(blobStore instanceof GarbageCollectableBlobStore){
            try {
                blobGC = new MarkSweepGarbageCollector(
                        new DocumentBlobReferenceRetriever(this),
                            (GarbageCollectableBlobStore) blobStore,
                        executor);
            } catch (IOException e) {
                throw new RuntimeException("Error occurred while initializing " +
                        "the MarkSweepGarbageCollector",e);
            }
        }
        return blobGC;
    }

    /**
     * A background thread.
     */
    static class BackgroundOperation implements Runnable {
        final WeakReference<DocumentNodeStore> ref;
        private final AtomicBoolean isDisposed;
        private int delay;

        BackgroundOperation(DocumentNodeStore nodeStore, AtomicBoolean isDisposed) {
            ref = new WeakReference<DocumentNodeStore>(nodeStore);
            delay = nodeStore.getAsyncDelay();
            this.isDisposed = isDisposed;
        }

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
                        nodeStore.runBackgroundOperations();
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
    public LastRevRecoveryAgent getLastRevRecoveryAgent() {
        return lastRevRecoveryAgent;
    }
}
