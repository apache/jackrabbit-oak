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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.jackrabbit.mongomk.impl.blob.MongoBlobStore;
import org.apache.jackrabbit.mongomk.prototype.Node.Children;
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.DB;

/**
 * A MicroKernel implementation that stores the data in a MongoDB.
 */
public class MongoMK implements MicroKernel {
    
    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
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
    private final Map<String, Node> nodeCache = new Cache<String, Node>(1024);
    
    /**
     * Child node cache.
     */
    private Cache<String, Node.Children> nodeChildrenCache =
            new Cache<String, Node.Children>(1024);

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
    
    private final Map<String, String> branchCommits = new HashMap<String, String>();

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
    
    private boolean includeRevision(Revision x, Revision requestRevision) {
        if (x.getClusterId() == this.clusterId && 
                requestRevision.getClusterId() == this.clusterId) {
            // both revisions were created by this cluster node: 
            // compare timestamps only
            return requestRevision.compareRevisionTime(x) >= 0;
        }
        // TODO currently we only compare the timestamps
        return x.compareRevisionTime(requestRevision) >= 0;
    }
    
    private boolean isRevisionNewer(Revision x, Revision previous) {
        // TODO currently we only compare the timestamps
        return x.compareRevisionTime(previous) >= 0;
    }
    
    public Node.Children readChildren(String path, String nodeId, Revision rev, int limit) {
        Node.Children c;
        c = nodeChildrenCache.get(nodeId);
        if (c != null) {
            return c;
        }
        String from = PathUtils.concat(path, "a");
        from = Node.convertPathToDocumentId(from);
        from = from.substring(0, from.length() - 1);
        String to = PathUtils.concat(path, "z");
        to = Node.convertPathToDocumentId(to);
        to = to.substring(0, to.length() - 2) + "0";
        List<Map<String, Object>> list = store.query(DocumentStore.Collection.NODES, from, to, limit);
        c = new Node.Children(path, nodeId, rev);
        for (Map<String, Object> e : list) {
            // Filter out deleted children
            if (isDeleted(e, rev)) {
                continue;
            }
            // TODO put the whole node in the cache
            String id = e.get("_id").toString();
            String p = id.substring(2);
            c.children.add(p);
        }
        nodeChildrenCache.put(nodeId, c);
        return c;
    }

    private Node readNode(String path, Revision rev) {
        String id = Node.convertPathToDocumentId(path);
        Map<String, Object> map = store.find(DocumentStore.Collection.NODES, id);
        if (map == null) {
            return null;
        }
        if (isDeleted(map, rev)) {
            return null;
        }
        Node n = new Node(path, rev);
        Long w = writeCountIncrements.get(path);
        long writeCount = w == null ? 0 : w;
        for (String key : map.keySet()) {
            if (key.equals("_writeCount")) {
                writeCount += (Long) map.get(key);
            }
            if (key.startsWith("_")) {
                // TODO property name escaping
                continue;
            }
            Object v = map.get(key);
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) v;
            if (valueMap != null) {
                Revision latestRev = null;
                for (String r : valueMap.keySet()) {
                    Revision propRev = Revision.fromString(r);
                    if (includeRevision(propRev, rev)) {
                        if (latestRev == null || isRevisionNewer(propRev, latestRev)) {
                            latestRev = propRev;
                            n.setProperty(key, valueMap.get(r));
                        }
                    }
                }
            }
        }
        n.setWriteCount(writeCount);
        return n;
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
        // TODO implement if needed
        return "{}";
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
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
        Children c = readChildren(path, n.getId(), rev, maxChildNodes);
        for (String s : c.children) {
            String name = PathUtils.getName(s);
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
    public String commit(String rootPath, String json, String revisionId,
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
                // TODO possibly support target position notation
                t.read(':');
                String target = t.readString();
                if (!PathUtils.isAbsolute(target)) {
                    target = PathUtils.concat(rootPath, target);
                }
                String to = PathUtils.relativize("/", target);
                // TODO support copy operations
                break;
            }
            default:
                throw new MicroKernelException("token: " + (char) t.getTokenType());
            }
        }
        if (revisionId.startsWith("b")) {
            // just commit to head currently
            applyCommit(commit);
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

    private void moveNode(String sourcePath, String targetPath, Commit commit) {
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
        markAsDeleted(sourcePath, commit, false);
        Node.Children c = readChildren(sourcePath, n.getId(),
                commit.getRevision(), Integer.MAX_VALUE);
        for (String srcChildPath : c.children) {
            String childName = PathUtils.getName(srcChildPath);
            String destChildPath = PathUtils.concat(targetPath, childName);
            moveNode(srcChildPath, destChildPath, commit);
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

            Node.Children c = readChildren(path, n.getId(), rev,
                    Integer.MAX_VALUE);
            for (String childPath : c.children) {
                markAsDeleted(childPath, commit, true);
            }
        }

        // Remove the node from the cache
        nodeCache.remove(path + "@" + rev);
    }

    private boolean isDeleted(Map<String, Object> nodeProps, Revision rev) {
        @SuppressWarnings("unchecked")
        Map<String, String> valueMap = (Map<String, String>) nodeProps
                .get("_deleted");
        if (valueMap != null) {
            for (Map.Entry<String, String> e : valueMap.entrySet()) {
                // TODO What if multiple revisions are there?. Should we sort
                // them and then
                // determine include revision based on that
                Revision propRev = Revision.fromString(e.getKey());
                if (includeRevision(propRev, rev)) {
                    if ("true".equals(e.getValue())) {
                        return true;
                    }
                }
            }
        }
        return false;
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
        // reading from the branch is reading from the trunk currently
        String revisionId = branchRevisionId.substring(1).replace('+', ' ').trim();
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

        Cache(int size) {
            super(size, (float) 0.75, true);
            this.size = size;
        }

        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > size;
        }
        
        public String toString() {
            return super.toString().replace(',', '\n');
        }

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
            boolean isNew, boolean isWritten, 
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
        if (isNew || c != null) {
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
                throw new AssertionError("New child list already cached");
            }
            nodeChildrenCache.put(id, c2);
        }
    }

    public long getWriteCountIncrement(String path) {
        Long x = writeCountIncrements.get(path);
        return x == null ? 0 : x;
    }

}
