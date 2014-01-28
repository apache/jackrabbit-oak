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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
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
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.BlobSerializer;
import org.apache.jackrabbit.oak.plugins.document.util.LoggingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.ChangeDispatcher;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a NodeStore on MongoDB.
 */
public final class MongoNodeStore
        implements NodeStore, RevisionContext, Observable {
    
    /**
     * The maximum number of document to update at once in a multi update.
     */
    static final int BACKGROUND_MULTI_UPDATE_LIMIT = 10000;

    private static final Logger LOG = LoggerFactory.getLogger(MongoNodeStore.class);

    /**
     * Do not cache more than this number of children for a document.
     */
    private static final int NUM_CHILDREN_CACHE_LIMIT = Integer.getInteger("oak.mongoMK.childrenCacheLimit", 16 * 1024);

    /**
     * When trying to access revisions that are older than this many
     * milliseconds, a warning is logged. The default is one minute.
     */
    private static final int WARN_REVISION_AGE =
            Integer.getInteger("oak.mongoMK.revisionAge", 60 * 1000);

    /**
     * Enable background operations
     */
    private static final boolean ENABLE_BACKGROUND_OPS = Boolean.parseBoolean(System.getProperty("oak.mongoMK.backgroundOps", "true"));

    /**
     * How long to remember the relative order of old revision of all cluster
     * nodes, in milliseconds. The default is one hour.
     */
    private static final int REMEMBER_REVISION_ORDER_MILLIS = 60 * 60 * 1000;

    /**
     * The MongoDB store (might be used by multiple MongoMKs).
     */
    protected final DocumentStore store;

    /**
     * The commit queue to coordinate the commits.
     */
    protected final CommitQueue commitQueue;

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
     * Unmerged branches of this MongoNodeStore instance.
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
     * Enable using simple revisions (just a counter). This feature is useful
     * for testing.
     */
    private AtomicInteger simpleRevisionCounter;

    /**
     * The node cache.
     *
     * Key: path@rev, value: node
     */
    private final Cache<String, Node> nodeCache;
    private final CacheStats nodeCacheStats;

    /**
     * Child node cache.
     *
     * Key: start-name/path@rev, value: children
     */
    private final Cache<String, Node.Children> nodeChildrenCache;
    private final CacheStats nodeChildrenCacheStats;

    /**
     * Child doc cache.
     */
    private final Cache<String, NodeDocument.Children> docChildrenCache;
    private final CacheStats docChildrenCacheStats;
    
    /**
     * The MongoDB blob store.
     */
    private final BlobStore blobStore;

    /**
     * The BlobSerializer.
     */
    private final BlobSerializer blobSerializer = new BlobSerializer() {
        @Override
        public String serialize(Blob blob) {
            if (blob instanceof MongoBlob) {
                return blob.toString();
            }
            String id;
            try {
                id = createBlob(blob.getNewStream()).toString();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return id;
        }
    };

    public MongoNodeStore(MongoMK.Builder builder) {
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
        int cid = builder.getClusterId();
        cid = Integer.getInteger("oak.mongoMK.clusterId", cid);
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

        //TODO Make stats collection configurable as it add slight overhead

        nodeCache = builder.buildCache(builder.getNodeCacheSize());
        nodeCacheStats = new CacheStats(nodeCache, "MongoMk-Node",
                builder.getWeigher(), builder.getNodeCacheSize());

        nodeChildrenCache = builder.buildCache(builder.getChildrenCacheSize());
        nodeChildrenCacheStats = new CacheStats(nodeChildrenCache, "MongoMk-NodeChildren",
                builder.getWeigher(), builder.getChildrenCacheSize());

        docChildrenCache = builder.buildCache(builder.getDocChildrenCacheSize());
        docChildrenCacheStats = new CacheStats(docChildrenCache, "MongoMk-DocChildren",
                builder.getWeigher(), builder.getDocChildrenCacheSize());

        // check if root node exists
        if (store.find(Collection.NODES, Utils.getIdFromPath("/")) == null) {
            // root node is missing: repository is not initialized
            Revision head = newRevision();
            Commit commit = new Commit(this, null, head);
            Node n = new Node("/", head);
            commit.addNode(n);
            commit.applyToDocumentStore();
            commit.applyToCache(false);
            setHeadRevision(commit.getRevision());
            // make sure _lastRev is written back to store
            backgroundWrite();
        } else {
            // initialize branchCommits
            branches.init(store, this);
            // initial reading of the revisions of other cluster nodes
            backgroundRead();
            if (headRevision == null) {
                // no revision read from other cluster nodes
                setHeadRevision(newRevision());
            }
        }
        getRevisionComparator().add(headRevision, Revision.newRevision(0));
        dispatcher = new ChangeDispatcher(getRoot());
        commitQueue = new CommitQueue(this, dispatcher);
        backgroundThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "MongoNodeStore background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();

        LOG.info("Initialized MongoNodeStore with clusterNodeId: {}", clusterId);
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
            LOG.info("Disposed MongoNodeStore with clusterNodeId: {}", clusterId);
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
            commitQueue.done(c.getRevision(), isBranch, info);
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

    void copyNode(Node source, String targetPath, Commit commit) {
        moveOrCopyNode(false, source, targetPath, commit);
    }

    void moveNode(Node source, String targetPath, Commit commit) {
        moveOrCopyNode(true, source, targetPath, commit);
    }

    void markAsDeleted(Node node, Commit commit, boolean subTreeAlso) {
        commit.removeNode(node.getPath());

        if (subTreeAlso) {
            // recurse down the tree
            // TODO causes issue with large number of children
            for (Node child : getChildNodes(node, null, Integer.MAX_VALUE)) {
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
    Node getNode(@Nonnull final String path, @Nonnull final Revision rev) {
        checkRevisionAge(checkNotNull(rev), checkNotNull(path));
        try {
            String key = path + "@" + rev;
            Node node = nodeCache.get(key, new Callable<Node>() {
                @Override
                public Node call() throws Exception {
                    Node n = readNode(path, rev);
                    if (n == null) {
                        n = Node.MISSING;
                    }
                    return n;
                }
            });
            return node == Node.MISSING ? null : node;
        } catch (ExecutionException e) {
            throw new MicroKernelException(e);
        }
    }

    Node.Children getChildren(@Nonnull final Node parent,
                              @Nullable final String name,
                              final int limit)
            throws MicroKernelException {
        if (checkNotNull(parent).hasNoChildren()) {
            return Node.NO_CHILDREN;
        }
        final String path = checkNotNull(parent).getPath();
        final Revision readRevision = parent.getLastRevision();
        String key = childNodeCacheKey(path, readRevision, name);
        Node.Children children;
        for (;;) {
            try {
                children = nodeChildrenCache.get(key, new Callable<Node.Children>() {
                    @Override
                    public Node.Children call() throws Exception {
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

    Node.Children readChildren(Node parent, String name, int limit) {
        // TODO use offset, to avoid O(n^2) and running out of memory
        // to do that, use the *name* of the last entry of the previous batch of children
        // as the starting point
        String path = parent.getPath();
        Revision rev = parent.getLastRevision();
        Iterable<NodeDocument> docs;
        Node.Children c = new Node.Children();
        // add one to the requested limit for the raw limit
        // this gives us a chance to detect whether there are more
        // child nodes than requested.
        int rawLimit = (int) Math.min(Integer.MAX_VALUE, ((long) limit) + 1);
        Set<Revision> validRevisions = new HashSet<Revision>();
        for (;;) {
            c.children.clear();
            docs = readChildDocs(path, name, rawLimit);
            int numReturned = 0;
            for (NodeDocument doc : docs) {
                numReturned++;
                // filter out deleted children
                if (doc.isDeleted(this, rev, validRevisions)) {
                    continue;
                }
                String p = Utils.getPathFromId(doc.getId());
                if (c.children.size() < limit) {
                    // add to children until limit is reached
                    c.children.add(p);
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
        // check cache
        NodeDocument.Children c = docChildrenCache.getIfPresent(path);
        if (c == null) {
            c = new NodeDocument.Children();
            List<NodeDocument> docs = store.query(Collection.NODES, from, to, limit);
            for (NodeDocument doc : docs) {
                String p = Utils.getPathFromId(doc.getId());
                c.childNames.add(PathUtils.getName(p));
            }
            c.isComplete = docs.size() < limit;
            docChildrenCache.put(path, c);
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
                String p = Utils.getPathFromId(doc.getId());
                clone.childNames.add(PathUtils.getName(p));
            }
            clone.isComplete = docs.size() < remainingLimit;
            docChildrenCache.put(path, clone);
            c = clone;
        }
        Iterable<NodeDocument> it = Iterables.transform(c.childNames, new Function<String, NodeDocument>() {
            @Override
            public NodeDocument apply(String name) {
                String p = PathUtils.concat(path, name);
                return store.find(Collection.NODES, Utils.getIdFromPath(p));
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
    Iterable<Node> getChildNodes(final @Nonnull Node parent,
                                 final @Nullable String name,
                                 final int limit) {
        // Preemptive check. If we know there are no children then
        // return straight away
        if (checkNotNull(parent).hasNoChildren()) {
            return Collections.emptyList();
        }

        final Revision readRevision = parent.getLastRevision();
        return Iterables.transform(getChildren(parent, name, limit).children,
                new Function<String, Node>() {
            @Override
            public Node apply(String input) {
                return getNode(input, readRevision);
            }
        });
    }

    @CheckForNull
    Node readNode(String path, Revision readRevision) {
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
     * @param rev the revision
     * @param path the path
     * @param isNew whether this is a new node
     * @param isDelete whether the node is deleted
     * @param pendingLastRev whether the node has a pending _lastRev to write
     * @param isBranchCommit whether this is from a branch commit
     * @param added the list of added child nodes
     * @param removed the list of removed child nodes
     *
     */
    public void applyChanges(Revision rev, String path,
                             boolean isNew, boolean isDelete, boolean pendingLastRev,
                             boolean isBranchCommit, ArrayList<String> added,
                             ArrayList<String> removed) {
        UnsavedModifications unsaved = unsavedLastRevisions;
        if (isBranchCommit) {
            Revision branchRev = rev.asBranchRevision();
            unsaved = branches.getBranch(branchRev).getModifications(branchRev);
        }
        if (isBranchCommit || pendingLastRev) {
            // write back _lastRev with background thread
            Revision prev = unsaved.put(path, rev);
            if (prev != null) {
                if (isRevisionNewer(prev, rev)) {
                    // revert
                    unsaved.put(path, prev);
                    String msg = String.format("Attempt to update " +
                            "unsavedLastRevision for %s with %s, which is " +
                            "older than current %s.",
                            path, rev, prev);
                    throw new MicroKernelException(msg);
                }
            }
        }
        String key = childNodeCacheKey(path, rev, null);
        Node.Children c = nodeChildrenCache.getIfPresent(key);
        if (isNew || (!isDelete && c != null)) {
            Node.Children c2 = new Node.Children();
            TreeSet<String> set = new TreeSet<String>();
            if (c != null) {
                set.addAll(c.children);
            }
            set.removeAll(removed);
            for (String name : added) {
                set.add(Utils.unshareString(name));
            }
            c2.children.addAll(set);
            nodeChildrenCache.put(key, c2);
        }
        if (!added.isEmpty()) {
            NodeDocument.Children docChildren = docChildrenCache.getIfPresent(path);
            if (docChildren != null) {
                int currentSize = docChildren.childNames.size();
                TreeSet<String> names = new TreeSet<String>(docChildren.childNames);
                // incomplete cache entries must not be updated with
                // names at the end of the list because there might be
                // a next name in MongoDB smaller than the one added
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
                    docChildrenCache.put(path, docChildren);
                }
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
    MongoNodeState getRoot(@Nonnull Revision revision) {
        Node root = getNode("/", revision);
        if (root == null) {
            throw new IllegalStateException(
                    "root node does not exist at revision " + revision);
        }
        return new MongoNodeState(this, root);
    }

    @Nonnull
    MongoNodeStoreBranch createBranch(MongoNodeState base) {
        return new MongoNodeStoreBranch(this, base);
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
            Map<String, UpdateOp> operations = new HashMap<String, UpdateOp>();
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
     * Applies a commit to the store and updates the caches accordingly.
     *
     * @param commit the commit to apply.
     * @return the commit revision.
     * @throws MicroKernelException if the commit cannot be applied.
     *              TODO: use non-MK exception type
     */
    @Nonnull
    Revision apply(@Nonnull Commit commit) throws MicroKernelException {
        checkNotNull(commit);
        boolean success = false;
        Revision baseRev = commit.getBaseRevision();
        boolean isBranch = baseRev != null && baseRev.isBranch();
        Revision rev = commit.getRevision();
        if (isBranch) {
            rev = rev.asBranchRevision();
            // remember branch commit
            Branch b = getBranches().getBranch(baseRev);
            if (b == null) {
                // baseRev is marker for new branch
                b = getBranches().create(baseRev.asTrunkRevision(), rev);
            } else {
                b.addCommit(rev);
            }
            try {
                // prepare commit
                commit.prepare(baseRev);
                success = true;
            } finally {
                if (!success) {
                    b.removeCommit(rev);
                    if (!b.hasCommits()) {
                        getBranches().remove(b);
                    }
                }
            }
        } else {
            commit.apply();
        }
        return rev;
    }

    /**
     * Returns the {@link Blob} with the given blobId.
     *
     * @param blobId the blobId of the blob.
     * @return the blob.
     */
    @Nonnull
    Blob getBlob(String blobId) {
        return new MongoBlob(blobStore, blobId);
    }

    //------------------------< Observable >------------------------------------

    @Override
    public Closeable addObserver(Observer observer) {
        return dispatcher.addObserver(observer);
    }

    //-------------------------< NodeStore >------------------------------------

    @Nonnull
    @Override
    public MongoNodeState getRoot() {
        return getRoot(headRevision);
    }

    @Nonnull
    @Override
    public NodeState merge(@Nonnull NodeBuilder builder,
                           @Nonnull CommitHook commitHook,
                           @Nullable CommitInfo info)
            throws CommitFailedException {
        return asMongoRootBuilder(builder).merge(commitHook, info);
    }

    @Nonnull
    @Override
    public NodeState rebase(@Nonnull NodeBuilder builder) {
        return asMongoRootBuilder(builder).rebase();
    }

    @Override
    public NodeState reset(@Nonnull NodeBuilder builder) {
        return asMongoRootBuilder(builder).reset();
    }

    @Override
    public Blob createBlob(InputStream inputStream) throws IOException {
        String id;
        try {
            id = blobStore.writeBlob(inputStream);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Could not write blob", e);
        }
        return new MongoBlob(blobStore, id);
    }

    @Nonnull
    @Override
    public String checkpoint(long lifetime) {
        // FIXME: need to signal to the garbage collector that this revision
        // should not be collected until the requested lifetime is over
        return getHeadRevision().toString();
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

    public void runBackgroundOperations() {
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
            
            // does not create new revisions
            backgroundSplit();
            
            // we need to protect backgroundRead as well,
            // as increment set the head revision in the read operation
            // (the read operation might see changes from other cluster nodes,
            // and so create a new head revision for the current cluster node,
            // to order revisions)
            Lock writeLock = backgroundOperationLock.writeLock();
            writeLock.lock();
            try {
                backgroundWrite();
                backgroundRead();
            } finally {
                writeLock.unlock();
            }
            
        } catch (RuntimeException e) {
            if (isDisposed.get()) {
                return;
            }
            LOG.warn("Background operation failed: " + e.toString(), e);
        }
    }

    private void backgroundRenewClusterIdLease() {
        if (clusterNodeInfo == null) {
            return;
        }
        clusterNodeInfo.renewLease(asyncDelay);
    }

    void backgroundRead() {
        String id = Utils.getIdFromPath("/");
        NodeDocument doc = store.find(Collection.NODES, id, asyncDelay);
        if (doc == null) {
            return;
        }
        Map<Integer, Revision> lastRevMap = doc.getLastRev();

        Revision.RevisionComparator revisionComparator = getRevisionComparator();
        boolean hasNewRevisions = false;
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node)
        Revision otherSeen = Revision.newRevision(0);
        for (Map.Entry<Integer, Revision> e : lastRevMap.entrySet()) {
            int machineId = e.getKey();
            if (machineId == clusterId) {
                continue;
            }
            Revision r = e.getValue();
            Revision last = lastKnownRevision.get(machineId);
            if (last == null || r.compareRevisionTime(last) > 0) {
                if (!hasNewRevisions) {
                    // publish our revision once before any foreign revision

                    // the latest revisions of the current cluster node
                    // happened before the latest revisions of other cluster nodes
                    revisionComparator.add(Revision.newRevision(clusterId), headSeen);
                }
                hasNewRevisions = true;
                lastKnownRevision.put(machineId, r);
                revisionComparator.add(r, otherSeen);
            }
        }
        if (hasNewRevisions) {
            store.invalidateCache();
            // TODO only invalidate affected items
            docChildrenCache.invalidateAll();
            // the head revision is after other revisions
            setHeadRevision(Revision.newRevision(clusterId));
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
                                new Object[]{id, before.getMemory(), after.getMemory()});
                    }
                }
            }
            it.remove();
        }
    }

    void backgroundWrite() {
        if (unsavedLastRevisions.getPaths().size() == 0) {
            return;
        }
        ArrayList<String> paths = new ArrayList<String>(unsavedLastRevisions.getPaths());
        // sort by depth (high depth first), then path
        Collections.sort(paths, PathComparator.INSTANCE);

        UpdateOp updateOp = null;
        Revision lastRev = null;
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < paths.size(); i++) {
            String p = paths.get(i);
            Revision r = unsavedLastRevisions.get(p);
            if (r == null) {
                continue;
            }
            int size = ids.size();
            if (updateOp == null) {
                // create UpdateOp
                Commit commit = new Commit(this, null, r);
                commit.touchNode(p);
                updateOp = commit.getUpdateOperationForNode(p);
                lastRev = r;
                ids.add(Utils.getIdFromPath(p));
            } else if (r.equals(lastRev)) {
                // use multi update when possible
                ids.add(Utils.getIdFromPath(p));
            }
            // call update if any of the following is true:
            // - this is the second-to-last or last path (update last path, the
            //   root document, individually)
            // - revision is not equal to last revision (size of ids didn't change)
            // - the update limit is reached
            if (i + 2 >= paths.size()
                    || size == ids.size()
                    || ids.size() >= BACKGROUND_MULTI_UPDATE_LIMIT) {
                store.update(Collection.NODES, ids, updateOp);
                for (String id : ids) {
                    unsavedLastRevisions.remove(Utils.getPathFromId(id));
                }
                ids.clear();
                updateOp = null;
                lastRev = null;
            }
        }
    }

    //-----------------------------< internal >---------------------------------

    private static String childNodeCacheKey(@Nonnull String path,
                                            @Nonnull Revision readRevision,
                                            @Nullable String name) {
        return (name == null ? "" : name) + path + "@" + readRevision;
    }

    private static MongoRootBuilder asMongoRootBuilder(NodeBuilder builder)
            throws IllegalArgumentException {
        if (!(builder instanceof MongoRootBuilder)) {
            throw new IllegalArgumentException("builder must be a " +
                    MongoRootBuilder.class.getName());
        }
        return (MongoRootBuilder) builder;
    }

    private void moveOrCopyNode(boolean move,
                                Node source,
                                String targetPath,
                                Commit commit) {
        // TODO Optimize - Move logic would not work well with very move of very large subtrees
        // At minimum we can optimize by traversing breadth wise and collect node id
        // and fetch them via '$in' queries

        // TODO Transient Node - Current logic does not account for operations which are part
        // of this commit i.e. transient nodes. If its required it would need to be looked
        // into

        Node newNode = new Node(targetPath, commit.getRevision());
        source.copyTo(newNode);

        commit.addNode(newNode);
        if (move) {
            markAsDeleted(source, commit, false);
        }
        for (Node child : getChildNodes(source, null, Integer.MAX_VALUE)) {
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
     * A background thread.
     */
    static class BackgroundOperation implements Runnable {
        final WeakReference<MongoNodeStore> ref;
        private final AtomicBoolean isDisposed;
        private int delay;

        BackgroundOperation(MongoNodeStore nodeStore, AtomicBoolean isDisposed) {
            ref = new WeakReference<MongoNodeStore>(nodeStore);
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
                MongoNodeStore nodeStore = ref.get();
                if (nodeStore != null) {
                    nodeStore.runBackgroundOperations();
                    delay = nodeStore.getAsyncDelay();
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
    
    Iterator<Blob> getReferencedBlobsIterator() {
        return new BlobReferenceIterator(this);
    }
    
}
