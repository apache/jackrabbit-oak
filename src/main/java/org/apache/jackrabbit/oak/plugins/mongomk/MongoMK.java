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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.EmpiricalWeigher;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.mongomk.Node.Children;
import org.apache.jackrabbit.oak.plugins.mongomk.Revision.RevisionComparator;
import org.apache.jackrabbit.oak.plugins.mongomk.blob.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.mongomk.util.LoggingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.mongomk.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.mongodb.DB;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A MicroKernel implementation that stores the data in a MongoDB.
 */
public class MongoMK implements MicroKernel, RevisionContext {

    /**
     * The threshold where special handling for many child node starts.
     */
    static final int MANY_CHILDREN_THRESHOLD = Integer.getInteger(
            "oak.mongoMK.manyChildren", 50);
    
    /**
     * Enable the LIRS cache.
     */
    static final boolean LIRS_CACHE = Boolean.parseBoolean(
            System.getProperty("oak.mongoMK.lirsCache", "false"));

    private static final Logger LOG = LoggerFactory.getLogger(MongoMK.class);

    /**
     * Do not cache more than this number of children for a document.
     */
    private static final int NUM_CHILDREN_CACHE_LIMIT = Integer.getInteger(
            "oak.mongoMK.childrenCacheLimit", 16 * 1024);

    /**
     * When trying to access revisions that are older than this many
     * milliseconds, a warning is logged. The default is one minute.
     */
    private static final int WARN_REVISION_AGE = 
            Integer.getInteger("oak.mongoMK.revisionAge", 60 * 1000);

    /**
     * Enable background operations
     */
    private static final boolean ENABLE_BACKGROUND_OPS = Boolean.parseBoolean(
            System.getProperty("oak.mongoMK.backgroundOps", "true"));

    /**
     * Enable fast diff operations.
     */
    private static final boolean FAST_DIFF = Boolean.parseBoolean(
            System.getProperty("oak.mongoMK.fastDiff", "true"));
        
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
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    protected int asyncDelay = 1000;

    /**
     * Whether this instance is disposed.
     */
    private final AtomicBoolean isDisposed = new AtomicBoolean();

    /**
     * The MongoDB blob store.
     */
    private final BlobStore blobStore;
    
    /**
     * The cluster instance info.
     */
    private final ClusterNodeInfo clusterNodeInfo;

    /**
     * The unique cluster id, similar to the unique machine id in MongoDB.
     */
    private final int clusterId;
    
    /**
     * The splitting point in milliseconds. If a document is split, revisions
     * older than this number of milliseconds are moved to a different document.
     * The default is 0, meaning documents are never split. Revisions that are
     * newer than this are kept in the newest document.
     */
    private final long splitDocumentAgeMillis;

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
     * Key: path@rev, value: children
     */
    private final Cache<String, Node.Children> nodeChildrenCache;
    private final CacheStats nodeChildrenCacheStats;
    
    /**
     * Diff cache.
     */
    private final Cache<String, Diff> diffCache;
    private final CacheStats diffCacheStats;

    /**
     * Child doc cache.
     */
    private final Cache<String, NodeDocument.Children> docChildrenCache;
    private final CacheStats docChildrenCacheStats;

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
     * Enable using simple revisions (just a counter). This feature is useful
     * for testing.
     */
    private AtomicInteger simpleRevisionCounter;
    
    /**
     * The comparator for revisions.
     */
    private final RevisionComparator revisionComparator;

    /**
     * Unmerged branches of this MongoMK instance.
     */
    // TODO at some point, open (unmerged) branches
    // need to be garbage collected (in-memory and on disk)
    private final UnmergedBranches branches;

    private boolean stopBackground;

    MongoMK(Builder builder) {

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
        this.blobStore = builder.getBlobStore();
        int cid = builder.getClusterId();
        splitDocumentAgeMillis = builder.getSplitDocumentAgeMillis();
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

        this.revisionComparator = new RevisionComparator(clusterId);
        this.asyncDelay = builder.getAsyncDelay();
        this.branches = new UnmergedBranches(getRevisionComparator());

        //TODO Make stats collection configurable as it add slight overhead
        //TODO Expose the stats as JMX beans

        nodeCache = builder.buildCache(builder.getNodeCacheSize());
        nodeCacheStats = new CacheStats(nodeCache, "MongoMk-Node",
                builder.getWeigher(), builder.getNodeCacheSize());

        nodeChildrenCache = builder.buildCache(builder.getChildrenCacheSize());
        nodeChildrenCacheStats = new CacheStats(nodeChildrenCache, "MongoMk-NodeChildren",
                builder.getWeigher(), builder.getChildrenCacheSize());

        diffCache = builder.buildCache(builder.getDiffCacheSize());
        diffCacheStats = new CacheStats(diffCache, "MongoMk-DiffCache",
                builder.getWeigher(), builder.getDiffCacheSize());

        docChildrenCache = builder.buildCache(builder.getDocChildrenCacheSize());
        docChildrenCacheStats = new CacheStats(docChildrenCache, "MongoMk-DocChildren",
                builder.getWeigher(), builder.getDocChildrenCacheSize());

        init();
        // initial reading of the revisions of other cluster nodes
        backgroundRead();
        getRevisionComparator().add(headRevision, Revision.newRevision(0));
        headRevision = newRevision();
        LOG.info("Initialized MongoMK with clusterNodeId: {}", clusterId);
    }

    void init() {
        headRevision = newRevision();
        Node n = readNode("/", headRevision);
        if (n == null) {
            // root node is missing: repository is not initialized
            Commit commit = new Commit(this, null, headRevision);
            n = new Node("/", headRevision);
            commit.addNode(n);
            commit.applyToDocumentStore();
        } else {
            // initialize branchCommits
            branches.init(store, this);
        }
        backgroundThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "MongoMK background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }
    

    /**
     * Create a new revision.
     * 
     * @return the revision
     */
    Revision newRevision() {
        if (simpleRevisionCounter != null) {
            return new Revision(simpleRevisionCounter.getAndIncrement(), 0, clusterId);
        }
        return Revision.newRevision(clusterId);
    }
    
    void runBackgroundOperations() {
        if (isDisposed.get()) {
            return;
        }
        backgroundRenewClusterIdLease();
        if (simpleRevisionCounter != null) {
            // only when using timestamp
            return;
        }
        if (!ENABLE_BACKGROUND_OPS || stopBackground) {
            return;
        }
        synchronized (this) {
            try {
                backgroundSplit();
                backgroundWrite();
                backgroundRead();
            } catch (RuntimeException e) {
                if (isDisposed.get()) {
                    return;
                }
                LOG.warn("Background operation failed: " + e.toString(), e);
            }
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
        
        RevisionComparator revisionComparator = getRevisionComparator();
        boolean hasNewRevisions = false;
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node) 
        Revision otherSeen = Revision.newRevision(0);
        for (Entry<Integer, Revision> e : lastRevMap.entrySet()) {
            int machineId = e.getKey();
            if (machineId == clusterId) {
                continue;
            }
            Revision r = e.getValue();
            Revision last = lastKnownRevision.get(machineId);
            if (last == null || r.compareRevisionTime(last) > 0) {
                lastKnownRevision.put(machineId, r);
                hasNewRevisions = true;
                revisionComparator.add(r, otherSeen);
            }
        }
        if (hasNewRevisions) {
            // TODO invalidating the whole cache is not really needed,
            // instead only those children that are cached could be checked
            store.invalidateCache();
            // TODO only invalidate affected items
            docChildrenCache.invalidateAll();
            // add a new revision, so that changes are visible
            Revision r = Revision.newRevision(clusterId);
            // the latest revisions of the current cluster node
            // happened before the latest revisions of other cluster nodes
            revisionComparator.add(r, headSeen);
            // the head revision is after other revisions
            headRevision = Revision.newRevision(clusterId);
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
        Collections.sort(paths, new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                int d1 = Utils.pathDepth(o1);
                int d2 = Utils.pathDepth(o1);
                if (d1 != d2) {
                    return Integer.signum(d1 - d2);
                }
                return o1.compareTo(o2);
            }

        });
        
        long now = Revision.getCurrentTimestamp();
        UpdateOp updateOp = null;
        Revision lastRev = null;
        List<String> ids = new ArrayList<String>();
        for (int i = 0; i < paths.size(); i++) {
            String p = paths.get(i);
            Revision r = unsavedLastRevisions.get(p);
            if (r == null) {
                continue;
            }
            // FIXME: with below code fragment the root (and other nodes
            // 'close' to the root) will not be updated in MongoDB when there
            // are frequent changes.
            if (Revision.getTimestampDifference(now, r.getTimestamp()) < asyncDelay) {
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
            // update if this is the last path or
            // revision is not equal to last revision
            if (i + 1 >= paths.size() || size == ids.size()) {
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

    public void dispose() {
        // force background write (with asyncDelay > 0, the root wouldn't be written)
        // TODO make this more obvious / explicit
        // TODO tests should also work if this is not done
        asyncDelay = 0;
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
            LOG.info("Disposed MongoMK with clusterNodeId: {}", clusterId);
        }
    }

    /**
     * Get the node for the given path and revision. The returned object might
     * not be modified directly.
     *
     * @param path
     * @param rev
     * @return the node
     */
    Node getNode(@Nonnull String path, @Nonnull Revision rev) {
        checkRevisionAge(checkNotNull(rev), checkNotNull(path));
        String key = path + "@" + rev;
        Node node = nodeCache.getIfPresent(key);
        if (node == null) {
            node = readNode(path, rev);
            if (node != null) {
                nodeCache.put(key, node);
            }
        }
        return node;
    }

    /**
     * Enqueue the document with the given id as a split candidate.
     *
     * @param id the id of the document to check if it needs to be split.
     */
    void addSplitCandidate(String id) {
        splitCandidates.put(id, id);
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
     * Checks that revision x is newer than another revision.
     * 
     * @param x the revision to check
     * @param previous the presumed earlier revision
     * @return true if x is newer
     */
    boolean isRevisionNewer(@Nonnull Revision x, @Nonnull Revision previous) {
        return getRevisionComparator().compare(x, previous) > 0;
    }

    public long getSplitDocumentAgeMillis() {
        return this.splitDocumentAgeMillis;
    }

    public Children getChildren(final String path, final Revision rev, final int limit)  throws MicroKernelException {
        checkRevisionAge(rev, path);
        String key = path + "@" + rev;
        Children children;
        try {
            children = nodeChildrenCache.get(key, new Callable<Children>() {
                @Override
                public Children call() throws Exception {
                    return readChildren(path, rev, limit);
                }
            });
        } catch (ExecutionException e) {
            throw new MicroKernelException("Error occurred while fetching children nodes for path "+path, e);
        }

        //In case the limit > cached children size and there are more child nodes
        //available then refresh the cache
        if (children.hasMore) {
            if (limit > children.children.size()) {
                children = readChildren(path, rev, limit);
                if (children != null) {
                    nodeChildrenCache.put(key, children);
                }
            }
        }
        return children;        
    }
    
    Node.Children readChildren(String path, Revision rev, int limit) {
        // TODO use offset, to avoid O(n^2) and running out of memory
        // to do that, use the *name* of the last entry of the previous batch of children
        // as the starting point
        Iterable<NodeDocument> docs;
        Children c = new Children();
        int rawLimit = limit;
        Set<Revision> validRevisions = new HashSet<Revision>();
        do {
            c.children.clear();
            c.hasMore = true;
            docs = readChildren(path, rawLimit);
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
                }
            }
            if (numReturned < rawLimit) {
                // fewer documents returned than requested
                // -> no more documents
                c.hasMore = false;
            }
            // double rawLimit for next round
            rawLimit = (int) Math.min(((long) rawLimit) * 2, Integer.MAX_VALUE);
        } while (c.children.size() < limit && c.hasMore);
        return c;
    }

    @Nonnull
    Iterable<NodeDocument> readChildren(final String path, int limit) {
        String from = Utils.getKeyLowerLimit(path);
        String to = Utils.getKeyUpperLimit(path);
        if (limit > NUM_CHILDREN_CACHE_LIMIT) {
            // do not use cache
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
        return Iterables.transform(c.childNames, new Function<String, NodeDocument>() {
            @Override
            public NodeDocument apply(String name) {
                String p = PathUtils.concat(path, name);
                return store.find(Collection.NODES, Utils.getIdFromPath(p));
            }
        });
    }

    @CheckForNull
    private Node readNode(String path, Revision readRevision) {
        String id = Utils.getIdFromPath(path);
        NodeDocument doc = store.find(Collection.NODES, id);
        if (doc == null) {
            return null;
        }
        return doc.getNodeAtRevision(this, readRevision);
    }
    
    @Override
    public String getHeadRevision() throws MicroKernelException {
        return headRevision.toString();
    }

    @Override @Nonnull
    public String checkpoint(long lifetime) throws MicroKernelException {
        // FIXME: need to signal to the garbage collector that this revision
        // should not be collected until the requested lifetime is over
        return getHeadRevision();
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws MicroKernelException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) throws MicroKernelException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public String diff(final String fromRevisionId,
                       final String toRevisionId,
                       final String path,
                       final int depth) throws MicroKernelException {
        String key = fromRevisionId + "-" + toRevisionId + "-" + path + "-" + depth;
        try {
            return diffCache.get(key, new Callable<Diff>() {
                @Override
                public Diff call() throws Exception {
                    return new Diff(diffImpl(fromRevisionId, toRevisionId, path, depth));
                }
            }).diff;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof MicroKernelException) {
                throw (MicroKernelException) e.getCause();
            } else {
                throw new MicroKernelException(e.getCause());
            }
        }
    }
    
    synchronized String diffImpl(String fromRevisionId, String toRevisionId, String path,
            int depth) throws MicroKernelException {
        if (fromRevisionId.equals(toRevisionId)) {
            return "";
        }
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
        if (path == null || path.equals("")) {
            path = "/";
        }
        Revision fromRev = Revision.fromString(fromRevisionId);
        Revision toRev = Revision.fromString(toRevisionId);
        Node from = getNode(path, fromRev);
        Node to = getNode(path, toRev);

        if (from == null || to == null) {
            // TODO implement correct behavior if the node does't/didn't exist
            throw new MicroKernelException("Diff is only supported if the node exists in both cases");
        }
        JsopWriter w = new JsopStream();
        for (String p : from.getPropertyNames()) {
            // changed or removed properties
            String fromValue = from.getProperty(p);
            String toValue = to.getProperty(p);
            if (!fromValue.equals(toValue)) {
                w.tag('^').key(p);
                if (toValue == null) {
                    w.value(toValue);
                } else {
                    w.encodedValue(toValue).newline();
                }
            }
        }
        for (String p : to.getPropertyNames()) {
            // added properties
            if (from.getProperty(p) == null) {
                w.tag('^').key(p).encodedValue(to.getProperty(p)).newline();
            }
        }
        // TODO this does not work well for large child node lists 
        // use a MongoDB index instead
        int max = MANY_CHILDREN_THRESHOLD;
        Children fromChildren, toChildren;
        fromChildren = getChildren(path, fromRev, max);
        toChildren = getChildren(path, toRev, max);
        if (!fromChildren.hasMore && !toChildren.hasMore) {
            diffFewChildren(w, fromChildren, fromRev, toChildren, toRev);
        } else {
            if (FAST_DIFF) {
                diffManyChildren(w, path, fromRev, toRev);
            } else {
                max = Integer.MAX_VALUE;
                fromChildren = getChildren(path, fromRev, max);
                toChildren = getChildren(path, toRev, max);
                diffFewChildren(w, fromChildren, fromRev, toChildren, toRev);
            }
        }
        return w.toString();
    }
    
    private void diffManyChildren(JsopWriter w, String path, Revision fromRev, Revision toRev) {
        long minTimestamp = Math.min(fromRev.getTimestamp(), toRev.getTimestamp());
        long minValue = Commit.getModified(minTimestamp);
        String fromKey = Utils.getKeyLowerLimit(path);
        String toKey = Utils.getKeyUpperLimit(path);
        List<NodeDocument> list = store.query(Collection.NODES, fromKey, toKey,
                NodeDocument.MODIFIED, minValue, Integer.MAX_VALUE);
        for (NodeDocument doc : list) {
            String id = doc.getId();
            String p = Utils.getPathFromId(id);
            Node fromNode = getNode(p, fromRev);
            Node toNode = getNode(p, toRev);
            if (fromNode != null) {
                // exists in fromRev
                if (toNode != null) {
                    // exists in both revisions
                    // check if different
                    if (!fromNode.getLastRevision().equals(toNode.getLastRevision())) {
                        w.tag('^').key(p).object().endObject().newline();
                    }
                } else {
                    // does not exist in toRev -> was removed
                    w.tag('-').value(p).newline();
                }
            } else {
                // does not exist in fromRev
                if (toNode != null) {
                    // exists in toRev
                    w.tag('+').key(p).object().endObject().newline();
                } else {
                    // does not exist in either revisions
                    // -> do nothing
                }
            }
        }
    }
    
    private void diffFewChildren(JsopWriter w, Children fromChildren, Revision fromRev, Children toChildren, Revision toRev) {
        Set<String> childrenSet = new HashSet<String>(toChildren.children);
        for (String n : fromChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('-').value(n).newline();
            } else {
                Node n1 = getNode(n, fromRev);
                Node n2 = getNode(n, toRev);
                // this is not fully correct:
                // a change is detected if the node changed recently,
                // even if the revisions are well in the past
                // if this is a problem it would need to be changed
                if (!n1.getId().equals(n2.getId())) {
                    w.tag('^').key(n).object().endObject().newline();
                }
            }
        }
        childrenSet = new HashSet<String>(fromChildren.children);
        for (String n : toChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('+').key(n).object().endObject().newline();
            }
        }
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        if (!PathUtils.isAbsolute(path)) {
            throw new MicroKernelException("Path is not absolute: " + path);
        }
        revisionId = revisionId != null ? revisionId : headRevision.toString();
        Revision rev = Revision.fromString(revisionId);
        Node n = getNode(path, rev);
        return n != null;
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        // not currently called by oak-core
        throw new MicroKernelException("Not implemented");
    }

    @Override
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
        revisionId = revisionId != null ? revisionId : headRevision.toString();
        Revision rev = Revision.fromString(revisionId);
        Node n = getNode(path, rev);
        if (n == null) {
            return null;
        }
        JsopStream json = new JsopStream();
        boolean includeId = filter != null && filter.contains(":id");
        includeId |= filter != null && filter.contains(":hash");
        json.object();
        n.append(json, includeId);
        int max;
        if (maxChildNodes == -1) {
            max = Integer.MAX_VALUE;
            maxChildNodes = Integer.MAX_VALUE;
        } else {
            // use long to avoid overflows
            long m = ((long) maxChildNodes) + offset;
            max = (int) Math.min(m, Integer.MAX_VALUE);
        }
        Children c = getChildren(path, rev, max);
        for (long i = offset; i < c.children.size(); i++) {
            if (maxChildNodes-- <= 0) {
                break;
            }
            String name = PathUtils.getName(c.children.get((int) i));
            json.key(name).object().endObject();
        }
        if (c.hasMore) {
            // TODO use a better way to notify there are more children
            json.key(":childNodeCount").value(Long.MAX_VALUE);
        } else {
            json.key(":childNodeCount").value(c.children.size());
        }
        json.endObject();
        return json.toString();
    }

    @Override
    public synchronized String commit(String rootPath, String json, String baseRevId,
            String message) throws MicroKernelException {
        Revision baseRev;
        if (baseRevId == null) {
            baseRev = headRevision;
            baseRevId = baseRev.toString();
        } else {
            baseRev = Revision.fromString(baseRevId);
        }
        JsopReader t = new JsopTokenizer(json);
        Revision rev = newRevision();
        Commit commit = new Commit(this, baseRev, rev);
        while (true) {
            int r = t.read();
            if (r == JsopReader.END) {
                break;
            }
            String path = PathUtils.concat(rootPath, t.readString());
            switch (r) {
            case '+':
                t.read(':');
                t.read('{');
                parseAddNode(commit, t, path);
                break;
            case '-':
                commit.removeNode(path);
                markAsDeleted(path, commit, true);
                commit.removeNodeDiff(path);
                break;
            case '^':
                t.read(':');
                String value;
                if (t.matches(JsopReader.NULL)) {
                    value = null;
                } else {
                    value = t.readRawValue().trim();
                }
                String p = PathUtils.getParentPath(path);
                String propertyName = PathUtils.getName(path);
                commit.updateProperty(p, propertyName, value);
                commit.updatePropertyDiff(p, propertyName, value);
                break;
            case '>': {
                // TODO support moving nodes that were modified within this commit
                t.read(':');
                String sourcePath = path;
                String targetPath = t.readString();
                if (!PathUtils.isAbsolute(targetPath)) {
                    targetPath = PathUtils.concat(rootPath, targetPath);
                }
                if (!nodeExists(sourcePath, baseRevId)) {
                    throw new MicroKernelException("Node not found: " + sourcePath + " in revision " + baseRevId);
                } else if (nodeExists(targetPath, baseRevId)) {
                    throw new MicroKernelException("Node already exists: " + targetPath + " in revision " + baseRevId);
                }
                commit.moveNode(sourcePath, targetPath);
                moveNode(sourcePath, targetPath, commit);
                break;
            }
            case '*': {
                // TODO support copying nodes that were modified within this commit
                t.read(':');
                String sourcePath = path;
                String targetPath = t.readString();
                if (!PathUtils.isAbsolute(targetPath)) {
                    targetPath = PathUtils.concat(rootPath, targetPath);
                }
                if (!nodeExists(sourcePath, baseRevId)) {
                    throw new MicroKernelException("Node not found: " + sourcePath + " in revision " + baseRevId);
                } else if (nodeExists(targetPath, baseRevId)) {
                    throw new MicroKernelException("Node already exists: " + targetPath + " in revision " + baseRevId);
                }
                commit.copyNode(sourcePath, targetPath);
                copyNode(sourcePath, targetPath, commit);
                break;
            }
            default:
                throw new MicroKernelException("token: " + (char) t.getTokenType());
            }
        }
        if (baseRev.isBranch()) {
            rev = rev.asBranchRevision();
            // remember branch commit
            Branch b = branches.getBranch(baseRev);
            if (b == null) {
                // baseRev is marker for new branch
                b = branches.create(baseRev.asTrunkRevision(), rev);
            } else {
                b.addCommit(rev);
            }
            boolean success = false;
            try {
                // prepare commit
                commit.prepare(baseRev);
                success = true;
            } finally {
                if (!success) {
                    b.removeCommit(rev);
                    if (!b.hasCommits()) {
                        branches.remove(b);
                    }
                }
            }

            return rev.toString();
        } else {
            commit.apply();
            headRevision = commit.getRevision();
            return rev.toString();
        }
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
    public RevisionComparator getRevisionComparator() {
        return revisionComparator;
    }

    @Override
    public void publishRevision(Revision foreignRevision, Revision changeRevision) {
        RevisionComparator revisionComparator = getRevisionComparator();
        if (revisionComparator.compare(headRevision, foreignRevision) >= 0) {
            // already visible
            return;
        }
        int clusterNodeId = foreignRevision.getClusterId();
        if (clusterNodeId == this.clusterId) {
            return;
        }
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node)
        Revision otherSeen = Revision.newRevision(0);
        // and after that, the current change
        Revision changeSeen = Revision.newRevision(0);
        revisionComparator.add(foreignRevision, otherSeen);
        // TODO invalidating the whole cache is not really needed,
        // but how to ensure we invalidate the right part of the cache?
        // possibly simply wait for the background thread to pick
        // up the changes, but this depends on how often this method is called
        store.invalidateCache();
        // the latest revisions of the current cluster node
        // happened before the latest revisions of other cluster nodes
        revisionComparator.add(headRevision, headSeen);
        revisionComparator.add(changeRevision, changeSeen);
        // the head revision is after other revisions
        headRevision = Revision.newRevision(clusterId);
    }

    @Override
    public int getClusterId() {
        return clusterId;
    }

    private void copyNode(String sourcePath, String targetPath, Commit commit) {
        moveOrCopyNode(false, sourcePath, targetPath, commit);
    }
    
    private void moveNode(String sourcePath, String targetPath, Commit commit) {
        moveOrCopyNode(true, sourcePath, targetPath, commit);
    }
    
    private void moveOrCopyNode(boolean move,
                                String sourcePath,
                                String targetPath,
                                Commit commit) {
        // TODO Optimize - Move logic would not work well with very move of very large subtrees
        // At minimum we can optimize by traversing breadth wise and collect node id
        // and fetch them via '$in' queries

        // TODO Transient Node - Current logic does not account for operations which are part
        // of this commit i.e. transient nodes. If its required it would need to be looked
        // into

        Node n = getNode(sourcePath, commit.getBaseRevision());

        // Node might be deleted already
        if (n == null) {
            return;
        }

        Node newNode = new Node(targetPath, commit.getRevision());
        n.copyTo(newNode);

        commit.addNode(newNode);
        if (move) {
            markAsDeleted(sourcePath, commit, false);
        }
        Node.Children c = getChildren(sourcePath, commit.getBaseRevision(), Integer.MAX_VALUE);
        for (String srcChildPath : c.children) {
            String childName = PathUtils.getName(srcChildPath);
            String destChildPath = PathUtils.concat(targetPath, childName);
            moveOrCopyNode(move, srcChildPath, destChildPath, commit);
        }
    }

    private void markAsDeleted(String path, Commit commit, boolean subTreeAlso) {
        Revision rev = commit.getBaseRevision();
        checkState(rev != null, "Base revision of commit must not be null");
        commit.removeNode(path);

        if (subTreeAlso) {
            // recurse down the tree
            // TODO causes issue with large number of children
            Node n = getNode(path, rev);

            if (n != null) {
                Node.Children c = getChildren(path, rev, Integer.MAX_VALUE);
                for (String childPath : c.children) {
                    markAsDeleted(childPath, commit, true);
                }
                nodeChildrenCache.invalidate(n.getId());
            }
        }
    }

    public static void parseAddNode(Commit commit, JsopReader t, String path) {
        Node n = new Node(path, commit.getRevision());
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    String childPath = PathUtils.concat(path, key);
                    parseAddNode(commit, t, childPath);
                } else {
                    String value = t.readRawValue().trim();
                    n.setProperty(key, value);
                }
            } while (t.matches(','));
            t.read('}');
        }
        commit.addNode(n);
        commit.addNodeDiff(n);
    }

    @Override
    public String branch(@Nullable String trunkRevisionId) throws MicroKernelException {
        // nothing is written when the branch is created, the returned
        // revision simply acts as a reference to the branch base revision
        Revision revision = trunkRevisionId != null
                ? Revision.fromString(trunkRevisionId) : headRevision;
        return revision.asBranchRevision().toString();
    }

    @Override
    public synchronized String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO improve implementation if needed
        Revision revision = Revision.fromString(branchRevisionId);
        if (!revision.isBranch()) {
            throw new MicroKernelException("Not a branch: " + branchRevisionId);
        }

        // make branch commits visible
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), false);
        Branch b = branches.getBranch(revision);
        Revision mergeCommit = newRevision();
        NodeDocument.setModified(op, mergeCommit);
        if (b != null) {
            for (Revision rev : b.getCommits()) {
                rev = rev.asTrunkRevision();
                NodeDocument.setRevision(op, rev, "c-" + mergeCommit.toString());
                op.containsMapEntry(NodeDocument.COLLISIONS, rev, false);
            }
            if (store.findAndUpdate(Collection.NODES, op) != null) {
                // remove from branchCommits map after successful update
                b.applyTo(unsavedLastRevisions, mergeCommit);
                branches.remove(b);
            } else {
                throw new MicroKernelException("Conflicting concurrent change. Update operation failed: " + op);
            }
        } else {
            // no commits in this branch -> do nothing
        }
        headRevision = mergeCommit;
        return headRevision.toString();
    }

    @Override
    @Nonnull
    public String rebase(@Nonnull String branchRevisionId,
                         @Nullable String newBaseRevisionId)
            throws MicroKernelException {
        // TODO conflict handling
        Revision r = Revision.fromString(branchRevisionId);
        Revision base = newBaseRevisionId != null ?
                Revision.fromString(newBaseRevisionId) :
                headRevision;
        Branch b = branches.getBranch(r);
        if (b == null) {
            // empty branch
            return base.asBranchRevision().toString();
        }
        if (b.getBase().equals(base)) {
            return branchRevisionId;
        }
        // add a pseudo commit to make sure current head of branch
        // has a higher revision than base of branch
        Revision head = newRevision().asBranchRevision();
        b.rebase(head, base);
        return head.toString();
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        try {
            return blobStore.getBlobLength(blobId);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {
        try {
            return blobStore.readBlob(blobId, pos, buff, off, length);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        try {
            return blobStore.writeBlob(in);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public DocumentStore getDocumentStore() {
        return store;
    }
    
    public void setAsyncDelay(int delay) {
        this.asyncDelay = delay;
    }
    
    public int getAsyncDelay() {
        return asyncDelay;
    }

    /**
     * Apply the changes of a node to the cache.
     * 
     * @param rev the revision
     * @param path the path
     * @param isNew whether this is a new node
     * @param isDelete whether the node is deleted
     * @param isWritten whether the MongoDB documented was added / updated
     * @param isBranchCommit whether this is from a branch commit
     * @param added the list of added child nodes
     * @param removed the list of removed child nodes
     *
     */
    public void applyChanges(Revision rev, String path, 
            boolean isNew, boolean isDelete, boolean isWritten,
            boolean isBranchCommit, ArrayList<String> added,
            ArrayList<String> removed) {
        UnsavedModifications unsaved = unsavedLastRevisions;
        if (isBranchCommit) {
            Revision branchRev = rev.asBranchRevision();
            unsaved = branches.getBranch(branchRev).getModifications(branchRev);
        }
        // track unsaved modifications of nodes that were not
        // written in the commit (implicitly modified parent)
        // or any modification if this is a branch commit
        if (!isWritten || isBranchCommit) {
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
        } else {
            // the document was updated:
            // we no longer need to update it in a background process
            unsaved.remove(path);
        }
        Children c = nodeChildrenCache.getIfPresent(path + "@" + rev);
        if (isNew || (!isDelete && c != null)) {
            String key = path + "@" + rev;
            Children c2 = new Children();
            TreeSet<String> set = new TreeSet<String>();
            if (c != null) {
                set.addAll(c.children);
            }
            set.removeAll(removed);
            for (String name : added) {
                // make sure the name string does not contain
                // unnecessary baggage
                set.add(new String(name));
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
                            // make sure the name string does not contain
                            // unnecessary baggage
                            names.add(new String(name));
                        }
                    }
                } else {
                    // add all
                    for (String childPath : added) {
                        // make sure the name string does not contain
                        // unnecessary baggage
                        names.add(new String(PathUtils.getName(childPath)));
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

    public CacheStats getNodeCacheStats() {
        return nodeCacheStats;
    }

    public CacheStats getNodeChildrenCacheStats() {
        return nodeChildrenCacheStats;
    }

    public CacheStats getDiffCacheStats() {
        return diffCacheStats;
    }
    
    public CacheStats getDocChildrenCacheStats() {
        return docChildrenCacheStats;
    }

    public ClusterNodeInfo getClusterInfo() {
        return clusterNodeInfo;
    }

    public int getPendingWriteCount() {
        return unsavedLastRevisions.getPaths().size();
    }

    public boolean isCached(String path) {
        return store.isCached(Collection.NODES, Utils.getIdFromPath(path));
    }
    
    public void stopBackground() {
        stopBackground = true;
    }
    
    /**
     * A background thread.
     */
    static class BackgroundOperation implements Runnable {
        final WeakReference<MongoMK> ref;
        private final AtomicBoolean isDisposed;
        private int delay;
        
        BackgroundOperation(MongoMK mk, AtomicBoolean isDisposed) {
            ref = new WeakReference<MongoMK>(mk);
            delay = mk.getAsyncDelay();
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
                MongoMK mk = ref.get();
                if (mk != null) {
                    mk.runBackgroundOperations();
                    delay = mk.getAsyncDelay();
                }
            }
        }
    }
    
    /**
     * A (cached) result of the diff operation.
     */
    private static class Diff implements CacheValue {
        
        final String diff;
        
        Diff(String diff) {
            this.diff = diff;
        }

        @Override
        public int getMemory() {
            return diff.length() * 2;
        }
        
    }

    /**
     * A builder for a MongoMK instance.
     */
    public static class Builder {
        private static final long DEFAULT_MEMORY_CACHE_SIZE = 256 * 1024 * 1024;
        private DocumentStore documentStore;
        private BlobStore blobStore;
        private int clusterId  = Integer.getInteger("oak.mongoMK.clusterId", 0);
        private int asyncDelay = 1000;
        private boolean timing;
        private boolean logging;
        private Weigher<String, CacheValue> weigher = new EmpiricalWeigher();
        private long nodeCacheSize;
        private long childrenCacheSize;
        private long diffCacheSize;
        private long documentCacheSize;
        private long docChildrenCacheSize;
        private boolean useSimpleRevision;
        private long splitDocumentAgeMillis = 5 * 60 * 1000;

        public Builder() {
            memoryCacheSize(DEFAULT_MEMORY_CACHE_SIZE);
        }

        /**
         * Set the MongoDB connection to use. By default an in-memory store is used.
         * 
         * @param db the MongoDB connection
         * @return this
         */
        public Builder setMongoDB(DB db) {
            if (db != null) {
                this.documentStore = new MongoDocumentStore(db, this);
                this.blobStore = new MongoBlobStore(db);
            }
            return this;
        }
        
        /**
         * Use the timing document store wrapper.
         * 
         * @param timing whether to use the timing wrapper.
         * @return this
         */
        public Builder setTiming(boolean timing) {
            this.timing = timing;
            return this;
        }
        
        public boolean getTiming() {
            return timing;
        }

        public Builder setLogging(boolean logging) {
            this.logging = logging;
            return this;
        }

        public boolean getLogging() {
            return logging;
        }

        /**
         * Set the document store to use. By default an in-memory store is used.
         * 
         * @param documentStore the document store
         * @return this
         */
        public Builder setDocumentStore(DocumentStore documentStore) {
            this.documentStore = documentStore;
            return this;
        }
        
        public DocumentStore getDocumentStore() {
            if (documentStore == null) {
                documentStore = new MemoryDocumentStore();
            }
            return documentStore;
        }

        /**
         * Set the blob store to use. By default an in-memory store is used.
         * 
         * @param blobStore the blob store
         * @return this
         */
        public Builder setBlobStore(BlobStore blobStore) {
            this.blobStore = blobStore;
            return this;
        }

        public BlobStore getBlobStore() {
            if (blobStore == null) {
                blobStore = new MemoryBlobStore();
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
        public Builder setClusterId(int clusterId) {
            this.clusterId = clusterId;
            return this;
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
        public Builder setAsyncDelay(int asyncDelay) {
            this.asyncDelay = asyncDelay;
            return this;
        }
        
        public int getAsyncDelay() {
            return asyncDelay;
        }

        public Weigher<String, CacheValue> getWeigher() {
            return weigher;
        }

        public Builder withWeigher(Weigher<String, CacheValue> weigher) {
            this.weigher = weigher;
            return this;
        }

        public Builder memoryCacheSize(long memoryCacheSize) {
            this.nodeCacheSize = memoryCacheSize * 20 / 100;
            this.childrenCacheSize = memoryCacheSize * 10 / 100;
            this.diffCacheSize = memoryCacheSize * 2 / 100;
            this.docChildrenCacheSize = memoryCacheSize * 3 / 100;
            this.documentCacheSize = memoryCacheSize - nodeCacheSize - childrenCacheSize - diffCacheSize - docChildrenCacheSize;
            return this;
        }

        public long getNodeCacheSize() {
            return nodeCacheSize;
        }

        public long getChildrenCacheSize() {
            return childrenCacheSize;
        }

        public long getDocumentCacheSize() {
            return documentCacheSize;
        }

        public long getDocChildrenCacheSize() {
            return docChildrenCacheSize;
        }

        public long getDiffCacheSize() {
            return diffCacheSize;
        }

        public Builder setUseSimpleRevision(boolean useSimpleRevision) {
            this.useSimpleRevision = useSimpleRevision;
            return this;
        }

        public boolean isUseSimpleRevision() {
            return useSimpleRevision;
        }
        
        public Builder setSplitDocumentAgeMillis(long splitDocumentAgeMillis) {
            this.splitDocumentAgeMillis = splitDocumentAgeMillis;
            return this;
        }
        
        public long getSplitDocumentAgeMillis() {
            return splitDocumentAgeMillis;
        }

        /**
         * Open the MongoMK instance using the configured options.
         * 
         * @return the MongoMK instance
         */
        public MongoMK open() {
            return new MongoMK(this);
        }
        
        /**
         * Create a cache.
         * 
         * @param <V> the value type
         * @param maxWeight
         * @return the cache
         */
        public <V extends CacheValue> Cache<String, V> buildCache(long maxWeight) {
            if (LIRS_CACHE) {
                return CacheLIRS.newBuilder().weigher(weigher).
                        maximumWeight(maxWeight).recordStats().build();
            }
            return CacheBuilder.newBuilder().weigher(weigher).
                    maximumWeight(maxWeight).recordStats().build();
        }
    }

}
