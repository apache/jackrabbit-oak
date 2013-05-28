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
package org.apache.jackrabbit.mongomk;

import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mongomk.DocumentStore.Collection;
import org.apache.jackrabbit.mongomk.Node.Children;
import org.apache.jackrabbit.mongomk.Revision.RevisionComparator;
import org.apache.jackrabbit.mongomk.blob.MongoBlobStore;
import org.apache.jackrabbit.mongomk.util.Utils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

/**
 * A MicroKernel implementation that stores the data in a MongoDB.
 */
public class MongoMK implements MicroKernel {

    private static final Logger LOG = LoggerFactory.getLogger(MongoMK.class);

    /**
     * The number of child node list entries to cache.
     */
    private static final int CACHE_CHILDREN = 
            Integer.getInteger("oak.mongoMK.cacheChildren", 1024);
    
    /**
     * The number of nodes to cache.
     */
    private static final int CACHE_NODES = 
            Integer.getInteger("oak.mongoMK.cacheNodes", 1024);
    
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
     * How long to remember the relative order of old revision of all cluster
     * nodes, in milliseconds. The default is one hour.
     */
    private static final int REMEMBER_REVISION_ORDER_MILLIS = 60 * 60 * 1000;

    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    // TODO test observation with multiple Oak instances
    protected int asyncDelay = 1000;

    /**
     * Whether this instance is disposed.
     */
    private final AtomicBoolean isDisposed = new AtomicBoolean();

    /**
     * The MongoDB store (might be used by multiple MongoMKs).
     */
    private final DocumentStore store;

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
     * The node cache.
     *
     * Key: path@rev, value: node
     */
    private final Cache<String, Node> nodeCache;

    /**
     * Child node cache.
     * 
     * Key: path@rev, value: children
     */
    private final Cache<String, Node.Children> nodeChildrenCache;

    /**
     * The unsaved last revisions. This contains the parents of all changed
     * nodes, once those nodes are committed but the parent node itself wasn't
     * committed yet. The parents are not immediately persisted as this would
     * cause each commit to change all parents (including the root node), which
     * would limit write scalability.
     * 
     * Key: path, value: revision.
     */
    private final Map<String, Revision> unsavedLastRevisions = 
            new ConcurrentHashMap<String, Revision>();
    
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
        this.store = builder.getDocumentStore();
        this.blobStore = builder.getBlobStore();
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
        
        this.revisionComparator = new RevisionComparator(clusterId);
        this.asyncDelay = builder.getAsyncDelay();
        this.branches = new UnmergedBranches(revisionComparator);

        //TODO Use size based weigher
        nodeCache = CacheBuilder.newBuilder()
                        .maximumSize(CACHE_NODES)
                        .build();

        nodeChildrenCache =  CacheBuilder.newBuilder()
                        .maximumSize(CACHE_CHILDREN)
                        .build();
        
        init();
        // initial reading of the revisions of other cluster nodes
        backgroundRead();
        revisionComparator.add(headRevision, Revision.newRevision(0));
        headRevision = newRevision();
        LOG.info("Initialized MongoMK with clusterNodeId: {}", clusterId);
    }
    
    void init() {
        backgroundThread = new Thread(
                new BackgroundOperation(this, isDisposed),
                "MongoMK background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();

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
            branches.init(store, clusterId);
        }
    }
    
    /**
     * Enable using simple revisions (just a counter). This feature is useful
     * for testing.
     */
    void useSimpleRevisions() {
        this.simpleRevisionCounter = new AtomicInteger(1);
        init();
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
        Map<String, Object> map = store.find(DocumentStore.Collection.NODES, id, asyncDelay);
        @SuppressWarnings("unchecked")
        Map<String, String> lastRevMap = (Map<String, String>) map.get(UpdateOp.LAST_REV);
        
        boolean hasNewRevisions = false;
        // the (old) head occurred first
        Revision headSeen = Revision.newRevision(0);
        // then we saw this new revision (from another cluster node) 
        Revision otherSeen = Revision.newRevision(0);
        for (Entry<String, String> e : lastRevMap.entrySet()) {
            int machineId = Integer.parseInt(e.getKey());
            if (machineId == clusterId) {
                continue;
            }
            Revision r = Revision.fromString(e.getValue());
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
    
    void backgroundWrite() {
        if (unsavedLastRevisions.size() == 0) {
            return;
        }
        ArrayList<String> paths = new ArrayList<String>(unsavedLastRevisions.keySet());
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
        for (String p : paths) {
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
            Commit commit = new Commit(this, null, r);
            commit.touchNode(p);
            store.createOrUpdate(DocumentStore.Collection.NODES, commit.getUpdateOperationForNode(p));
            unsavedLastRevisions.remove(p);
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
    Node getNode(String path, Revision rev) {
        checkRevisionAge(rev, path);
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
    
    private void checkRevisionAge(Revision r, String path) {
        // TODO only log if there are new revisions available for the given node
        if (LOG.isDebugEnabled()) {
            if (headRevision.getTimestamp() - r.getTimestamp() > WARN_REVISION_AGE) {
                LOG.debug("Requesting an old revision for path " + path + ", " + 
                    ((headRevision.getTimestamp() - r.getTimestamp()) / 1000) + " seconds old");
            }
        }
    }
    
    private boolean includeRevision(Revision x, Revision requestRevision) {
        Branch b = branches.getBranch(x);
        if (b != null) {
            // only include if requested revision is also a branch revision
            // with a history including x
            if (b.containsCommit(requestRevision)) {
                // in same branch, include if the same revision or
                // requestRevision is newer
                return x.equals(requestRevision) || isRevisionNewer(requestRevision, x);
            }
            // not part of branch identified by requestedRevision
            return false;
        }
        // assert: x is not a branch commit
        b = branches.getBranch(requestRevision);
        if (b != null) {
            // reset requestRevision to branch base revision to make
            // sure we don't include revisions committed after branch
            // was created
            requestRevision = b.getBase();
        }
        return revisionComparator.compare(requestRevision, x) >= 0;
    }
    
    /**
     * Checks that revision x is newer than another revision.
     * 
     * @param x the revision to check
     * @param previous the presumed earlier revision
     * @return true if x is newer
     */
    boolean isRevisionNewer(@Nonnull Revision x, @Nonnull Revision previous) {
        return revisionComparator.compare(x, previous) > 0;
    }

    /**
     * Checks if the revision is valid for the given node map. A revision is
     * considered valid if the given node map is the root of the commit, or the
     * commit root has the revision set. This method may read further nodes to
     * perform this check.
     * This method also takes pending branches into consideration.
     * The <code>readRevision</code> identifies the read revision used by the
     * client, which may be a branch revision logged in {@link #branches}.
     * The revision <code>rev</code> is valid if it is part of the branch
     * history of <code>readRevision</code>.
     *
     * @param rev     revision to check.
     * @param readRevision the read revision of the client.
     * @param nodeMap the node to check.
     * @param validRevisions set of revisions already checked against
     *                       <code>readRevision</code> and considered valid.
     * @return <code>true</code> if the revision is valid; <code>false</code>
     *         otherwise.
     */
    boolean isValidRevision(@Nonnull Revision rev,
                            @Nonnull Revision readRevision,
                            @Nonnull Map<String, Object> nodeMap,
                            @Nonnull Set<Revision> validRevisions) {
        if (validRevisions.contains(rev)) {
            return true;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> revisions = (Map<String, String>) nodeMap.get(UpdateOp.REVISIONS);
        if (isCommitted(rev, readRevision, revisions)) {
            validRevisions.add(rev);
            return true;
        } else if (revisions != null && revisions.containsKey(rev.toString())) {
            // rev is in revisions map of this node, but not committed
            // no need to check _commitRoot field
            return false;
        }
        // check commit root
        @SuppressWarnings("unchecked")
        Map<String, Integer> commitRoot = (Map<String, Integer>) nodeMap.get(UpdateOp.COMMIT_ROOT);
        String commitRootPath = null;
        if (commitRoot != null) {
            Integer depth = commitRoot.get(rev.toString());
            if (depth != null) {
                String p = Utils.getPathFromId((String) nodeMap.get(UpdateOp.ID));
                commitRootPath = PathUtils.getAncestorPath(p, PathUtils.getDepth(p) - depth);
            }
        }
        if (commitRootPath == null) {
            // shouldn't happen, either node is commit root for a revision
            // or has a reference to the commit root
            LOG.warn("Node {} does not have commit root reference for revision {}",
                    nodeMap.get(UpdateOp.ID), rev);
            LOG.warn(nodeMap.toString());
            return false;
        }
        // get root of commit
        nodeMap = store.find(DocumentStore.Collection.NODES, 
                Utils.getIdFromPath(commitRootPath));
        if (nodeMap == null) {
            return false;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> rootRevisions = (Map<String, String>) nodeMap.get(UpdateOp.REVISIONS);
        if (isCommitted(rev, readRevision, rootRevisions)) {
            validRevisions.add(rev);
            return true;
        }
        return false;
    }

    /**
     * Returns <code>true</code> if the given revision is set to committed in
     * the revisions map. That is, the revision exists in the map and the string
     * value is <code>"true"</code> or equals the <code>readRevision</code>.
     *
     * @param revision  the revision to check.
     * @param readRevision the read revision.
     * @param revisions the revisions map, or <code>null</code> if none is set.
     * @return <code>true</code> if the revision is committed, otherwise
     *         <code>false</code>.
     */
    private boolean isCommitted(@Nonnull Revision revision,
                                @Nonnull Revision readRevision,
                                @Nullable Map<String, String> revisions) {
        if (revision.equals(readRevision)) {
            return true;
        }
        if (revisions == null) {
            return false;
        }
        String value = revisions.get(revision.toString());
        if (value == null) {
            return false;
        }
        if (value.equals("true")) {
            if (branches.getBranch(readRevision) == null) {
                return true;
            }
        } else {
            // branch commit
            if (Revision.fromString(value).getClusterId() != clusterId) {
                // this is an unmerged branch commit from another cluster node,
                // hence never visible to us
                return false;
            }
        }
        return includeRevision(revision, readRevision);
    }

    public Children getChildren(String path, Revision rev, int limit) {
        checkRevisionAge(rev, path);
        String key = path + "@" + rev;
        Children children = nodeChildrenCache.getIfPresent(key);
        if (children == null) {
            children = readChildren(path, rev, limit);
            if (children != null) {
                nodeChildrenCache.put(key, children);
            }
        }
        return children;        
    }
    
    Node.Children readChildren(String path, Revision rev, int limit) {
        String from = PathUtils.concat(path, "a");
        from = Utils.getIdFromPath(from);
        from = from.substring(0, from.length() - 1);
        String to = PathUtils.concat(path, "z");
        to = Utils.getIdFromPath(to);
        to = to.substring(0, to.length() - 2) + "0";
        List<Map<String, Object>> list = store.query(DocumentStore.Collection.NODES, from, to, limit);
        Children c = new Children(path, rev);
        Set<Revision> validRevisions = new HashSet<Revision>();
        for (Map<String, Object> e : list) {
            // filter out deleted children
            if (getLiveRevision(e, rev, validRevisions) == null) {
                continue;
            }
            // TODO put the whole node in the cache
            String id = e.get(UpdateOp.ID).toString();
            String p = Utils.getPathFromId(id);
            c.children.add(p);
        }
        return c;
    }

    private Node readNode(String path, Revision rev) {
        String id = Utils.getIdFromPath(path);
        Map<String, Object> map = store.find(DocumentStore.Collection.NODES, id);
        if (map == null) {
            return null;
        }
        Revision min = getLiveRevision(map, rev);
        if (min == null) {
            // deleted
            return null;
        }
        Node n = new Node(path, rev);
        Revision lastRevision = null;
        Revision revision =  unsavedLastRevisions.get(path);
        if (revision != null) {
            if (isRevisionNewer(revision, rev)) {
                // at most the read revision
                revision = rev;
            }
            lastRevision = revision;
        }
        for (String key : map.keySet()) {
            if (key.equals(UpdateOp.LAST_REV)) {
                Object v = map.get(key);
                @SuppressWarnings("unchecked")
                Map<String, String> valueMap = (Map<String, String>) v;
                for (String r : valueMap.keySet()) {
                    revision = Revision.fromString(valueMap.get(r));
                    if (isRevisionNewer(revision, rev)) {
                        // at most the read revision
                        revision = rev;
                    }
                    if (lastRevision == null || isRevisionNewer(revision, lastRevision)) {
                        lastRevision = revision;
                    }
                }
            }
            if (!Utils.isPropertyName(key)) {
                continue;
            }
            Object v = map.get(key);
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) v;
            if (valueMap != null) {
                if (valueMap instanceof TreeMap) {
                    // use descending keys (newest first) if map is sorted
                    valueMap = ((TreeMap<String, String>) valueMap).descendingMap();
                }
                String value = getLatestValue(valueMap, min, rev);
                String propertyName = Utils.unescapePropertyName(key);
                n.setProperty(propertyName, value);
            }
        }
        n.setLastRevision(lastRevision);
        return n;
    }
    
    /**
     * Get the latest property value that is larger or equal the min revision,
     * and smaller or equal the max revision.
     * 
     * @param valueMap the revision-value map
     * @param min the minimum revision (null meaning unlimited)
     * @param max the maximum revision
     * @return the value, or null if not found
     */
    private String getLatestValue(@Nonnull Map<String, String> valueMap,
                                  @Nullable Revision min,
                                  @Nonnull Revision max) {
        String value = null;
        Revision latestRev = null;
        for (String r : valueMap.keySet()) {
            Revision propRev = Revision.fromString(r);
            if (min != null && isRevisionNewer(min, propRev)) {
                continue;
            }
            if (latestRev != null && !isRevisionNewer(propRev, latestRev)) {
                continue;
            }
            if (includeRevision(propRev, max)) {
                latestRev = propRev;
                value = valueMap.get(r);
            }
        }
        return value;
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        return headRevision.toString();
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
    public String diff(String fromRevisionId, String toRevisionId, String path,
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
        fromRevisionId = stripBranchRevMarker(fromRevisionId);
        toRevisionId = stripBranchRevMarker(toRevisionId);
        Node from = getNode(path, Revision.fromString(fromRevisionId));
        Node to = getNode(path, Revision.fromString(toRevisionId));
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
        Revision fromRev = Revision.fromString(fromRevisionId);
        Revision toRev = Revision.fromString(toRevisionId);
        // TODO this does not work well for large child node lists 
        // use a MongoDB index instead
        Children fromChildren = getChildren(path, fromRev, Integer.MAX_VALUE);
        Children toChildren = getChildren(path, toRev, Integer.MAX_VALUE);
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
        return w.toString();
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        if (!PathUtils.isAbsolute(path)) {
            throw new MicroKernelException("Path is not absolute: " + path);
        }
        revisionId = revisionId != null ? revisionId : headRevision.toString();
        Revision rev = Revision.fromString(stripBranchRevMarker(revisionId));
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
    public synchronized String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
        revisionId = revisionId != null ? revisionId : headRevision.toString();
        if (revisionId.startsWith("b")) {
            // reading from the branch is reading from the trunk currently
            revisionId = stripBranchRevMarker(revisionId);
        }
        Revision rev = Revision.fromString(revisionId);
        Node n = getNode(path, rev);
        if (n == null) {
            return null;
            // throw new MicroKernelException("Node not found at path " + path);
        }
        JsopStream json = new JsopStream();
        boolean includeId = filter != null && filter.contains(":id");
        includeId |= filter != null && filter.contains(":hash");
        json.object();
        n.append(json, includeId);
        if (maxChildNodes == -1) {
            maxChildNodes = Integer.MAX_VALUE;
        }
        // FIXME: must not read all children!
        Children c = getChildren(path, rev, Integer.MAX_VALUE);
        for (long i = offset; i < c.children.size(); i++) {
            if (maxChildNodes-- <= 0) {
                break;
            }
            String name = PathUtils.getName(c.children.get((int) i));
            json.key(name).object().endObject();
        }
        json.key(":childNodeCount").value(c.children.size());
        json.endObject();
        String result = json.toString();
        // if (filter != null && filter.contains(":hash")) {
        //     result = result.replaceAll("\":id\"", "\":hash\"");
        // }
        return result;
    }

    @Override
    public synchronized String commit(String rootPath, String json, String baseRevId,
            String message) throws MicroKernelException {
        Revision baseRev;
        if (baseRevId == null) {
            baseRev = headRevision;
            baseRevId = baseRev.toString();
        } else {
            baseRev = Revision.fromString(stripBranchRevMarker(baseRevId));
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
                moveNode(sourcePath, targetPath, baseRev, commit);
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
                copyNode(sourcePath, targetPath, baseRev, commit);
                break;
            }
            default:
                throw new MicroKernelException("token: " + (char) t.getTokenType());
            }
        }
        if (baseRevId.startsWith("b")) {
            // remember branch commit
            Branch b = branches.getBranch(baseRev);
            if (b == null) {
                b = branches.create(baseRev, rev);
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
                    if (b.getCommits().isEmpty()) {
                        branches.remove(b);
                    }
                }
            }

            return "b" + rev.toString();
        }
        commit.apply();
        headRevision = commit.getRevision();
        return rev.toString();
    }

    private void copyNode(String sourcePath, String targetPath, Revision baseRev, Commit commit) {
        moveOrCopyNode(false, sourcePath, targetPath, baseRev, commit);
    }
    
    private void moveNode(String sourcePath, String targetPath, Revision baseRev, Commit commit) {
        moveOrCopyNode(true, sourcePath, targetPath, baseRev, commit);
    }
    
    private void moveOrCopyNode(boolean move,
                                String sourcePath,
                                String targetPath,
                                Revision baseRev,
                                Commit commit) {
        // TODO Optimize - Move logic would not work well with very move of very large subtrees
        // At minimum we can optimize by traversing breadth wise and collect node id
        // and fetch them via '$in' queries

        // TODO Transient Node - Current logic does not account for operations which are part
        // of this commit i.e. transient nodes. If its required it would need to be looked
        // into

        Node n = getNode(sourcePath, baseRev);

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
        Node.Children c = getChildren(sourcePath, baseRev, Integer.MAX_VALUE);
        for (String srcChildPath : c.children) {
            String childName = PathUtils.getName(srcChildPath);
            String destChildPath = PathUtils.concat(targetPath, childName);
            moveOrCopyNode(move, srcChildPath, destChildPath, baseRev, commit);
        }
    }

    private void markAsDeleted(String path, Commit commit, boolean subTreeAlso) {
        Revision rev = commit.getRevision();
        commit.removeNode(path);

        if (subTreeAlso) {

            // recurse down the tree
            // TODO causes issue with large number of children
            Node n = getNode(path, rev);

            // remove from the cache
            nodeCache.invalidate(path + "@" + rev);
            
            if (n != null) {
                Node.Children c = getChildren(path, rev,
                        Integer.MAX_VALUE);
                for (String childPath : c.children) {
                    markAsDeleted(childPath, commit, true);
                }
                nodeChildrenCache.invalidate(n.getId());
            }
        }

        // Remove the node from the cache
        nodeCache.invalidate(path + "@" + rev);
    }

    /**
     * Get the earliest (oldest) revision where the node was alive at or before
     * the provided revision, if the node was alive at the given revision.
     * 
     * @param nodeMap the node map
     * @param maxRev the maximum revision to return
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    private Revision getLiveRevision(Map<String, Object> nodeMap,
                                     Revision maxRev) {
        return getLiveRevision(nodeMap, maxRev, new HashSet<Revision>());
    }

    /**
     * Get the earliest (oldest) revision where the node was alive at or before
     * the provided revision, if the node was alive at the given revision.
     * 
     * @param nodeMap the node map
     * @param maxRev the maximum revision to return
     * @param validRevisions the set of revisions already checked against maxRev
     *            and considered valid.
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    private Revision getLiveRevision(Map<String, Object> nodeMap,
            Revision maxRev, Set<Revision> validRevisions) {
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) nodeMap
                .get(UpdateOp.DELETED);
        if (valueMap == null) {
            return null;
        }
        // first, search the newest deleted revision
        Revision deletedRev = null;
        if (valueMap instanceof TreeMap) {
            // use descending keys (newest first) if map is sorted
            valueMap = ((TreeMap<String, String>) valueMap).descendingMap();
        }
        for (String r : valueMap.keySet()) {
            String value = valueMap.get(r);
            if (!"true".equals(value)) {
                // only look at deleted revisions now
                continue;
            }
            Revision propRev = Revision.fromString(r);
            if (isRevisionNewer(propRev, maxRev)
                    || !isValidRevision(propRev, maxRev, nodeMap, validRevisions)) {
                continue;
            }
            if (deletedRev == null || isRevisionNewer(propRev, deletedRev)) {
                deletedRev = propRev;
            }
        }
        // now search the oldest non-deleted revision that is newer than the
        // newest deleted revision
        Revision liveRev = null;
        for (String r : valueMap.keySet()) {
            String value = valueMap.get(r);
            if ("true".equals(value)) {
                // ignore deleted revisions
                continue;
            }
            Revision propRev = Revision.fromString(r);
            if (deletedRev != null && isRevisionNewer(deletedRev, propRev)) {
                // the node was deleted later on
                continue;
            }
            if (isRevisionNewer(propRev, maxRev)
                    || !isValidRevision(propRev, maxRev, nodeMap, validRevisions)) {
                continue;
            }
            if (liveRev == null || isRevisionNewer(liveRev, propRev)) {
                liveRev = propRev;
            }
        }
        return liveRev;
    }
    
    /**
     * Get the revision of the latest change made to this node.
     * 
     * @param nodeMap the document
     * @param changeRev the revision of the current change
     * @param handler the conflict handler, which is called for un-committed revisions
     *                preceding <code>before</code>.
     * @return the revision, or null if deleted
     */
    @SuppressWarnings("unchecked")
    @Nullable Revision getNewestRevision(Map<String, Object> nodeMap,
                                         Revision changeRev, CollisionHandler handler) {
        if (nodeMap == null) {
            return null;
        }
        SortedSet<String> revisions = new TreeSet<String>(Collections.reverseOrder());
        if (nodeMap.containsKey(UpdateOp.REVISIONS)) {
            revisions.addAll(((Map<String, String>) nodeMap.get(UpdateOp.REVISIONS)).keySet());
        }
        if (nodeMap.containsKey(UpdateOp.COMMIT_ROOT)) {
            revisions.addAll(((Map<String, Integer>) nodeMap.get(UpdateOp.COMMIT_ROOT)).keySet());
        }
        Map<String, String> deletedMap = (Map<String, String>) nodeMap
                .get(UpdateOp.DELETED);
        if (deletedMap != null) {
            revisions.addAll(deletedMap.keySet());
        }
        Revision newestRev = null;
        for (String r : revisions) {
            Revision propRev = Revision.fromString(r);
            if (isRevisionNewer(propRev, changeRev)) {
                // we have seen a previous change from another cluster node
                // (which might be conflicting or not) - we need to make
                // sure this change is visible from now on
                publishRevision(propRev, changeRev);
            }
            if (newestRev == null || isRevisionNewer(propRev, newestRev)) {
                if (!propRev.equals(changeRev)) {
                    if (!isValidRevision(
                            propRev, changeRev, nodeMap, new HashSet<Revision>())) {
                        handler.uncommittedModification(propRev);
                    } else {
                        newestRev = propRev;
                    }
                }
            }
        }
        if (newestRev == null) {
            return null;
        }
        if (deletedMap != null) {
            String value = deletedMap.get(newestRev.toString());
            if ("true".equals(value)) {
                // deleted in the newest revision
                return null;
            }
        }
        return newestRev;
    }
    
    /**
     * Ensure the revision visible from now on, possibly by updating the head
     * revision, so that the changes that occurred are visible.
     * 
     * @param foreignRevision the revision from another cluster node
     * @param changeRevision the local revision that is sorted after the foreign revision
     */
    private void publishRevision(Revision foreignRevision, Revision changeRevision) {  
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
    
    private static String stripBranchRevMarker(String revisionId) {
        if (revisionId.startsWith("b")) {
            return revisionId.substring(1);
        }
        return revisionId;
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
        String revisionId = trunkRevisionId != null ? trunkRevisionId : headRevision.toString();
        return "b" + revisionId;
    }

    @Override
    public synchronized String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO improve implementation if needed
        if (!branchRevisionId.startsWith("b")) {
            throw new MicroKernelException("Not a branch: " + branchRevisionId);
        }

        String revisionId = stripBranchRevMarker(branchRevisionId);
        // make branch commits visible
        UpdateOp op = new UpdateOp("/", Utils.getIdFromPath("/"), false);
        Revision revision = Revision.fromString(revisionId);
        Branch b = branches.getBranch(revision);
        if (b != null) {
            for (Revision rev : b.getCommits()) {
                op.setMapEntry(UpdateOp.REVISIONS, rev.toString(), "true");
                op.containsMapEntry(UpdateOp.COLLISIONS, rev.toString(), false);
            }
            if (store.findAndUpdate(DocumentStore.Collection.NODES, op) != null) {
                // remove from branchCommits map after successful update
                branches.remove(b);
            } else {
                throw new MicroKernelException("Conflicting concurrent change. Update operation failed: " + op);
            }
        } else {
            // no commits in this branch -> do nothing
        }
        headRevision = newRevision();
        return headRevision.toString();
    }

    @Override
    @Nonnull
    public String rebase(String branchRevisionId, String newBaseRevisionId)
            throws MicroKernelException {
        // TODO improve implementation if needed
        return branchRevisionId;
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
     * @param added the list of added child nodes
     * @param removed the list of removed child nodes
     */
    public void applyChanges(Revision rev, String path, 
            boolean isNew, boolean isDelete, boolean isWritten, 
            ArrayList<String> added, ArrayList<String> removed) {
        if (!isWritten) {
            Revision prev = unsavedLastRevisions.put(path, rev);
            if (prev != null) {
                if (isRevisionNewer(prev, rev)) {
                    // revert
                    unsavedLastRevisions.put(path, prev);
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
            unsavedLastRevisions.remove(path);
        }
        Children c = nodeChildrenCache.getIfPresent(path + "@" + rev);
        if (isNew || (!isDelete && c != null)) {
            String key = path + "@" + rev;
            Children c2 = new Children(path, rev);
            TreeSet<String> set = new TreeSet<String>();
            if (c != null) {
                set.addAll(c.children);
            }
            set.removeAll(removed);
            set.addAll(added);
            c2.children.addAll(set);
            nodeChildrenCache.put(key, c2);
        }
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
     * A builder for a MongoMK instance.
     */
    public static class Builder {
        
        private DocumentStore documentStore;
        private BlobStore blobStore;
        private int clusterId  = Integer.getInteger("oak.mongoMK.clusterId", 0);
        private int asyncDelay = 1000;

        /**
         * Set the MongoDB connection to use. By default an in-memory store is used.
         * 
         * @param db the MongoDB connection
         * @return this
         */
        public Builder setMongoDB(DB db) {
            if (db != null) {
                this.documentStore = new MongoDocumentStore(db);
                this.blobStore = new MongoBlobStore(db);
            }
            return this;
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
        
        /**
         * Open the MongoMK instance using the configured options.
         * 
         * @return the MongoMK instance
         */
        public MongoMK open() {
            return new MongoMK(this);
        }
    }

    public ClusterNodeInfo getClusterInfo() {
        return clusterNodeInfo;
    }

    public int getPendingWriteCount() {
        return unsavedLastRevisions.size();
    }

    public boolean isCached(String path) {
        return store.isCached(Collection.NODES, Utils.getIdFromPath(path));
    }
    
    public void stopBackground() {
        stopBackground = true;
    }
    
    RevisionComparator getRevisionComparator() {
        return revisionComparator;
    }

}
