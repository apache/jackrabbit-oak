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
package org.apache.jackrabbit.mongomk.prototype;

import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.apache.jackrabbit.mongomk.prototype.Node.Children;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;

/**
 * A MicroKernel implementation that stores the data in a MongoDB.
 */
public class MongoMK implements MicroKernel {

    /**
     * The number of documents to cache.
     */
    static final int CACHE_DOCUMENTS = Integer.getInteger("oak.mongoMK.cacheDocs", 20 * 1024);
    
    /**
     * The number of child node list entries to cache.
     */
    private static final int CACHE_CHILDREN = Integer.getInteger("oak.mongoMK.cacheChildren", 1024);
    
    /**
     * The number of nodes to cache.
     */
    private static final int CACHE_NODES = Integer.getInteger("oak.mongoMK.cacheNodes", 1024);
    
    private static final int WARN_REVISION_AGE = Integer.getInteger("oak.mongoMK.revisionAge", 10000);
    
    private static final Logger LOG = LoggerFactory.getLogger(MongoMK.class);
    
    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    // TODO test observation with multiple Oak instances
    protected static final long ASYNC_DELAY = 1000;

    /**
     * For revisions that are older than this many seconds, the MongoMK will
     * assume the revision is valid. For more recent changes, the MongoMK needs
     * to verify it first (by reading the revision root). The default is
     * Integer.MAX_VALUE, meaning no revisions are trusted. Once the garbage
     * collector removes old revisions, this value is changed.
     */
    private static final int trustedRevisionAge = Integer.MAX_VALUE;

    AtomicBoolean isDisposed = new AtomicBoolean();

    /**
     * The MongoDB store (might be used by multiple MongoMKs).
     */
    private final DocumentStore store;

    /**
     * The MongoDB blob store.
     */
    private final BlobStore blobStore;

    /**
     * The unique cluster id, similar to the unique machine id in MongoDB.
     */
    private final int clusterId;

    /**
     * The node cache.
     *
     * Key: path@rev
     * Value: node
     */
    private final Map<String, Node> nodeCache = 
            new Cache<String, Node>(CACHE_NODES);
    
    /**
     * Child node cache.
     */
    private Cache<String, Node.Children> nodeChildrenCache =
            new Cache<String, Node.Children>(CACHE_CHILDREN);

    /**
     * The unsaved write count increments.
     */
    private final Map<String, Long> writeCountIncrements = new HashMap<String, Long>();
    
    /**
     * The set of known valid revision.
     * The key is the revision id, the value is 1 (because a cache can't be a set).
     */
    private final Map<String, Long> revCache = new Cache<String, Long>(1024);
    
    /**
     * The last known head revision. This is the last-known revision.
     */
    private Revision headRevision;
    
    private Thread backgroundThread;

    /**
     * Maps branch commit revision to revision it is based on
     */
    private final Map<String, String> branchCommits =
            Collections.synchronizedMap(new HashMap<String, String>());

    /**
     * Create a new in-memory MongoMK used for testing.
     */
    public MongoMK() {
        this(new MemoryDocumentStore(), new MemoryBlobStore(), 0);
    }
    
    /**
     * Create a new MongoMK.
     * 
     * @param db the MongoDB connection (null for in-memory)
     * @param clusterId the cluster id (must be unique)
     */
    public MongoMK(DB db, int clusterId) {
        this(db == null ? new MemoryDocumentStore() : new MongoDocumentStore(db),
                db == null ? new MemoryBlobStore() : new MongoBlobStore(db), 
                clusterId);
    }

    /**
     * Create a new MongoMK.
     *
     * @param store the store (might be shared)
     * @param blobStore the blob store to use
     * @param clusterId the cluster id (must be unique)
     */
    public MongoMK(DocumentStore store, BlobStore blobStore, int clusterId) {
        this.store = store;
        this.blobStore = blobStore;
        this.clusterId = clusterId;
        backgroundThread = new Thread(
            new BackgroundOperation(this, isDisposed),
            "MongoMK background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();
        headRevision = Revision.newRevision(clusterId);
        Node n = readNode("/", headRevision);
        if (n == null) {
            // root node is missing: repository is not initialized
            Commit commit = new Commit(this, headRevision);
            n = new Node("/", headRevision);
            commit.addNode(n);
            commit.applyToDocumentStore();
        }
    }
    
    Revision newRevision() {
        return Revision.newRevision(clusterId);
    }
    
    void runBackgroundOperations() {
        // to be implemented
    }
    
    public void dispose() {
        if (!isDisposed.getAndSet(true)) {
            synchronized (isDisposed) {
                isDisposed.notifyAll();
            }
            try {
                backgroundThread.join();
            } catch (InterruptedException e) {
                // ignore
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
        Node node = nodeCache.get(key);
        if (node == null) {
            node = readNode(path, rev);
            if (node != null) {
                nodeCache.put(key, node);
            }
        }
        return node;
    }
    
    private void checkRevisionAge(Revision r, String path) {
        if (headRevision.getTimestamp() - r.getTimestamp() > WARN_REVISION_AGE) {
            LOG.warn("Requesting an old revision for path " + path + ", " + 
                    ((headRevision.getTimestamp() - r.getTimestamp()) / 1000) + " seconds old");
            if (LOG.isDebugEnabled()) {
                LOG.warn("Requesting an old revision", new Exception());
            }
        }
    }
    
    private boolean includeRevision(Revision x, Revision requestRevision) {
        if (x.getClusterId() == this.clusterId && 
                requestRevision.getClusterId() == this.clusterId) {
            // both revisions were created by this cluster node: 
            // compare timestamps only
            return requestRevision.compareRevisionTime(x) >= 0;
        }
        // TODO currently we only compare the timestamps
        return requestRevision.compareRevisionTime(x) >= 0;
    }
    
    boolean isRevisionNewer(Revision x, Revision previous) {
        // TODO currently we only compare the timestamps
        return x.compareRevisionTime(previous) > 0;
    }
    
    public Node.Children readChildren(String path, String nodeId, Revision rev, int limit) {
        Node.Children c;
        c = nodeChildrenCache.get(nodeId);
        if (c != null) {
            return c;
        }
        String from = PathUtils.concat(path, "a");
        from = Utils.getIdFromPath(from);
        from = from.substring(0, from.length() - 1);
        String to = PathUtils.concat(path, "z");
        to = Utils.getIdFromPath(to);
        to = to.substring(0, to.length() - 2) + "0";
        List<Map<String, Object>> list = store.query(DocumentStore.Collection.NODES, from, to, limit);
        c = new Node.Children(path, nodeId, rev);
        for (Map<String, Object> e : list) {
            // filter out deleted children
            if (getLiveRevision(e, rev) == null) {
                continue;
            }
            // TODO put the whole node in the cache
            String id = e.get(UpdateOp.ID).toString();
            String p = Utils.getPathFromId(id);
            c.children.add(p);
        }
        nodeChildrenCache.put(nodeId, c);
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
        Long w = writeCountIncrements.get(path);
        long writeCount = w == null ? 0 : w;
        for (String key : map.keySet()) {
            if (key.equals(UpdateOp.WRITE_COUNT)) {
                writeCount += (Long) map.get(key);
            }
            if (!Utils.isPropertyName(key)) {
                continue;
            }
            Object v = map.get(key);
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) v;
            if (valueMap != null) {
                String value = getLatestValue(valueMap, min, rev);
                String propertyName = Utils.unescapePropertyName(key);
                n.setProperty(propertyName, value);
            }
        }
        n.setWriteCount(writeCount);
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
    private String getLatestValue(Map<String, String> valueMap, Revision min, Revision max) {
        String value = null;
        Revision latestRev = null;
        for (String r : valueMap.keySet()) {
            Revision propRev = Revision.fromString(r);
            if (min != null) {
                if (isRevisionNewer(min, propRev)) {
                    continue;
                }
            }
            if (includeRevision(propRev, max)) {
                if (latestRev == null || isRevisionNewer(propRev, latestRev)) {
                    latestRev = propRev;
                    value = valueMap.get(r);
                }
            }
        }
        return value;
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        String head = headRevision.toString();
        while (branchCommits.containsKey(head)) {
            head = branchCommits.get(head);
        }
        return head;
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
        // TODO implement if needed
        if (true) {
            return "{}";        
        }
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
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
                w.tag('^').key(p).value(toValue).newline();
            }
        }
        for (String p : to.getPropertyNames()) {
            // added properties
            if (from.getProperty(p) == null) {
                w.tag('^').key(p).value(to.getProperty(p)).newline();
            }
        }
        Revision fromRev = Revision.fromString(fromRevisionId);
        Revision toRev = Revision.fromString(toRevisionId);
        // TODO this does not work well for large child node lists 
        // use a MongoDB index instead
        Children fromChildren = readChildren(path, from.getId(), fromRev, Integer.MAX_VALUE);
        Children toChildren = readChildren(path, to.getId(), toRev, Integer.MAX_VALUE);
        Set<String> childrenSet = new HashSet<String>(toChildren.children);
        for (String n : fromChildren.children) {
            if (!childrenSet.contains(n)) {
                w.tag('-').key(n).object().endObject().newline();
            } else {
                // TODO currently all children seem to diff, 
                // which is not necessarily the case
                // (compare write counters)
                w.tag('^').key(n).object().endObject().newline();
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
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
        revisionId = revisionId != null ? revisionId : headRevision.toString();
        if (revisionId.startsWith("b")) {
            // reading from the branch is reading from the trunk currently
            revisionId = revisionId.substring(1).replace('+', ' ').trim();
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
        // FIXME: must not read all children!
        Children c = readChildren(path, n.getId(), rev, Integer.MAX_VALUE);
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
    public synchronized String commit(String rootPath, String json, String revisionId,
            String message) throws MicroKernelException {
        revisionId = revisionId == null ? headRevision.toString() : revisionId;
        JsopReader t = new JsopTokenizer(json);
        Revision rev = Revision.newRevision(clusterId);
        Commit commit = new Commit(this, rev);
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
                    commit.getDiff().tag('^').key(path).value(null);
                } else {
                    value = t.readRawValue().trim();
                    commit.getDiff().tag('^').key(path).value(value);
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
                    targetPath = PathUtils.concat(path, targetPath);
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
                    targetPath = PathUtils.concat(path, targetPath);
                }
                commit.copyNode(sourcePath, targetPath);
                copyNode(sourcePath, targetPath, commit);
                break;
            }
            default:
                throw new MicroKernelException("token: " + (char) t.getTokenType());
            }
        }
        if (revisionId.startsWith("b")) {
            // just commit to head currently
            applyCommit(commit);
            // remember branch commit
            branchCommits.put(rev.toString(), revisionId.substring(1));

            return "b" + rev.toString();

            // String jsonBranch = branchCommits.remove(revisionId);
            // jsonBranch += commit.getDiff().toString();
            // String branchRev = revisionId + "+";
            // branchCommits.put(branchRev, jsonBranch);
            // return branchRev;
        }
        applyCommit(commit);
        return rev.toString();
    }

    private void copyNode(String sourcePath, String targetPath, Commit commit) {
        moveOrCopyNode(false, sourcePath, targetPath, commit);
    }
    
    private void moveNode(String sourcePath, String targetPath, Commit commit) {
        moveOrCopyNode(true, sourcePath, targetPath, commit);
    }
    
    private void moveOrCopyNode(boolean move, String sourcePath, String targetPath, Commit commit) {
        // TODO Optimize - Move logic would not work well with very move of very large subtrees
        // At minimum we can optimize by traversing breadth wise and collect node id
        // and fetch them via '$in' queries

        // TODO Transient Node - Current logic does not account for operations which are part
        // of this commit i.e. transient nodes. If its required it would need to be looked
        // into

        Node n = getNode(sourcePath, commit.getRevision());

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
        Node.Children c = readChildren(sourcePath, n.getId(),
                commit.getRevision(), Integer.MAX_VALUE);
        for (String srcChildPath : c.children) {
            String childName = PathUtils.getName(srcChildPath);
            String destChildPath = PathUtils.concat(targetPath, childName);
            moveOrCopyNode(move, srcChildPath, destChildPath, commit);
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
            nodeCache.remove(path + "@" + rev);
            
            if (n != null) {
                Node.Children c = readChildren(path, n.getId(), rev,
                        Integer.MAX_VALUE);
                for (String childPath : c.children) {
                    markAsDeleted(childPath, commit, true);
                }
            }
        }

        // Remove the node from the cache
        nodeCache.remove(path + "@" + rev);
    }
    
    /**
     * Get the latest revision where the node was alive at or before the the
     * provided revision.
     * 
     * @param nodeMap the node map
     * @param maxRev the maximum revision to return
     * @return the earliest revision, or null if the node is deleted at the
     *         given revision
     */
    private Revision getLiveRevision(Map<String, Object> nodeMap,
            Revision maxRev) {
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) nodeMap
                .get(UpdateOp.DELETED);
        Revision firstRev = null;
        String value = null;
        for (String r : valueMap.keySet()) {
            Revision propRev = Revision.fromString(r);
            if (isRevisionNewer(propRev, maxRev)) {
                continue;
            }
            String v = valueMap.get(r);
            if (firstRev == null || isRevisionNewer(propRev, firstRev)) {
                firstRev = propRev;
                value = v;
            }
        }
        if ("true".equals(value)) {
            return null;
        }
        return firstRev;
    }
    
    private static String stripBranchRevMarker(String revisionId) {
        if (revisionId.startsWith("b")) {
            return revisionId.substring(1);
        }
        return revisionId;
    }

    private void applyCommit(Commit commit) {
        headRevision = commit.getRevision();
        if (commit.isEmpty()) {
            return;
        }
        commit.applyToDocumentStore();
        commit.applyToCache();
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
    public String branch(String trunkRevisionId) throws MicroKernelException {
        // TODO improve implementation if needed
        String branchId = "b" + trunkRevisionId;
        // branchCommits.put(branchId, "");
        return branchId;
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO improve implementation if needed
        if (!branchRevisionId.startsWith("b")) {
            throw new MicroKernelException("Not a branch: " + branchRevisionId);
        }

        // reading from the branch is reading from the trunk currently
        String revisionId = branchRevisionId.substring(1).replace('+', ' ').trim();
        String baseRevId = revisionId;
        while (baseRevId != null) {
            baseRevId = branchCommits.remove(baseRevId);
        }
        return revisionId;

        // TODO improve implementation if needed
        // if (!branchRevisionId.startsWith("b")) {
        //     throw new MicroKernelException("Not a branch: " + branchRevisionId);
        // }
        //
        // String commit = branchCommits.remove(branchRevisionId);
        // return commit("", commit, null, null);
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
    
    /**
     * A simple cache.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    static class Cache<K, V> extends LinkedHashMap<K, V> {

        private static final long serialVersionUID = 1L;
        private int size;
        private TreeSet<K> keySet = new TreeSet<K>();

        Cache(int size) {
            super(size, (float) 0.75, true);
            this.size = size;
        }
        
        public synchronized V put(K key, V value) {
            keySet.add(key);
            return super.put(key, value);
        }
        
        public synchronized V remove(Object key) {
            keySet.remove(key);
            return super.remove(key);
        }

        protected synchronized boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            boolean remove = size() > size;
            if (remove) {
                Object k = eldest.getKey();
                if (k != null) {
                    keySet.remove(k);
                }
            }
            return remove;
        }
        
        public String toString() {
            return super.toString().replace(',', '\n');
        }
        
        // public synchronized SortedSet<K> subSet(K fromElement, K toElement) {
        //   return keySet.subSet(fromElement, toElement);
        // }

    }

    /**
     * A background thread.
     */
    static class BackgroundOperation implements Runnable {
        final WeakReference<MongoMK> ref;
        private final AtomicBoolean isDisposed;
        BackgroundOperation(MongoMK mk, AtomicBoolean isDisposed) {
            ref = new WeakReference<MongoMK>(mk);
            this.isDisposed = isDisposed;
        }
        public void run() {
            while (!isDisposed.get()) {
                synchronized (isDisposed) {
                    try {
                        isDisposed.wait(ASYNC_DELAY);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                MongoMK mk = ref.get();
                if (mk != null) {
                    mk.runBackgroundOperations();
                }
            }
        }
    }

    public void applyChanges(Revision rev, String path, 
            boolean isNew, boolean isDelete, boolean isWritten, 
            long oldWriteCount, long writeCountInc,
            ArrayList<String> added, ArrayList<String> removed) {
        if (!isWritten) {
            if (writeCountInc == 0) {
                writeCountIncrements.remove(path);
            } else {
                writeCountIncrements.put(path, writeCountInc);
            }
        } else {
            writeCountIncrements.remove(path);
        }
        long newWriteCount = oldWriteCount + writeCountInc;
        Children c = nodeChildrenCache.get(path + "@" + (newWriteCount - 1));
        if (isNew || (!isDelete && c != null)) {
            String id = path + "@" + newWriteCount;
            Children c2 = new Children(path, id, rev);
            TreeSet<String> set = new TreeSet<String>();
            if (c != null) {
                set.addAll(c.children);
            }
            set.removeAll(removed);
            set.addAll(added);
            c2.children.addAll(set);
            if (nodeChildrenCache.get(id) != null) {
                // TODO should not happend, 
                // probably a problem with add/delete/add
                MicroKernelException e = new MicroKernelException(
                        "New child list already cached for id " + id);
                LOG.error("Error updating the cache", e);
            }
            nodeChildrenCache.put(id, c2);
        }
    }

    public long getWriteCountIncrement(String path) {
        Long x = writeCountIncrements.get(path);
        return x == null ? 0 : x;
    }

}
