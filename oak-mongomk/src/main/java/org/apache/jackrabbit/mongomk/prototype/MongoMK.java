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
import java.util.LinkedHashMap;
import java.util.Map;
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
import org.apache.jackrabbit.oak.commons.PathUtils;

import com.mongodb.DB;

/**
 * A MicroKernel implementation that stores the data in a MongoDB.
 */
public class MongoMK implements MicroKernel {

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
    // TODO: should be path@id
    private final Map<String, Node> nodeCache = new Cache<String, Node>(1024);

    /**
     * For revisions that are older than this many seconds, the MongoMK will
     * assume the revision is valid. For more recent changes, the MongoMK needs
     * to verify it first (by reading the revision root). The default is
     * Integer.MAX_VALUE, meaning no revisions are trusted. Once the garbage
     * collector removes old revisions, this value is changed.
     */
    private static final int trustedRevisionAge = Integer.MAX_VALUE;

    /**
     * The delay for asynchronous operations (delayed commit propagation and
     * cache update).
     */
    protected static final long ASYNC_DELAY = 1000;

    /**
     * The set of known valid revision.
     * The key is the revision id, the value is 1 (because a cache can't be a set).
     */
    private final Map<String, Long> revCache = new Cache<String, Long>(1024);
    
    /**
     * The last known head revision. This is the last-known revision.
     */
    private String headRevision;
    
    AtomicBoolean isDisposed = new AtomicBoolean();
    
    private Thread backgroundThread;

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
        backgroundThread = new Thread(new Runnable() {
            public void run() {
                while (!isDisposed.get()) {
                    synchronized (this) {
                        try {
                            wait(ASYNC_DELAY);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                    runBackgroundOperations();
                }
            }
        }, "MongoMK background thread");
        backgroundThread.setDaemon(true);
        backgroundThread.start();
    }
    
    Revision newRevision() {
        return Revision.newRevision(clusterId);
    }
    
    void runBackgroundOperations() {
        // to be implemented
    }
    
    public void dispose() {
        if (!isDisposed.getAndSet(true)) {
            synchronized (backgroundThread) {
                backgroundThread.notifyAll();
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
            return x.compareRevisionTime(requestRevision) >= 0;
        }
        // TODO currently we only compare the timestamps
        return x.compareRevisionTime(requestRevision) >= 0;
    }

    private Node readNode(String path, Revision rev) {
        String id = Node.convertPathToDocumentId(path);
        Map<String, Object> map = store.find(DocumentStore.Collection.NODES, id);
        if (map == null) {
            return null;
        }
        Node n = new Node(path, rev);
        for(String key : map.keySet()) {
            if (key.startsWith("_")) {
                // TODO property name escaping
                continue;
            }
            @SuppressWarnings("unchecked")
            Map<String, String> valueMap = (Map<String, String>) map.get(key);
            for (String r : valueMap.keySet()) {
                Revision propRev = Revision.fromString(r);
                if (includeRevision(propRev, rev)) {
                    n.setProperty(key, valueMap.get(r));
                }
            }
        }
        return n;
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        return headRevision;
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws MicroKernelException {
        // TODO implement if needed
        return null;
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        // TODO implement if needed
        return null;
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) throws MicroKernelException {
        // TODO implement if needed
        return null;
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path,
            int depth) throws MicroKernelException {
        // TODO implement if needed
        return null;
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
        // TODO implement if needed
        return 0;
    }

    @Override
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        Revision rev = Revision.fromString(revisionId);
        Node n = getNode(path, rev);
        JsopStream json = new JsopStream();
        n.append(json);
        return json.toString();
    }

    @Override
    public String commit(String rootPath, String json, String revisionId,
            String message) throws MicroKernelException {
        JsopReader t = new JsopTokenizer(json);
        revisionId = revisionId == null ? headRevision : revisionId;
        Revision rev = Revision.newRevision(clusterId);
        Commit commit = new Commit(rev);
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
                // TODO support remove operations
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
                UpdateOp op = commit.getUpdateOperationForNode(p);
                op.set(propertyName, value);
                break;
            case '>': {
                t.read(':');
                String name = PathUtils.getName(path);
                String position, target, to;
                boolean rename;
                if (t.matches('{')) {
                    rename = false;
                    position = t.readString();
                    t.read(':');
                    target = t.readString();
                    t.read('}');
                    if (!PathUtils.isAbsolute(target)) {
                        target = PathUtils.concat(rootPath, target);
                    }
                } else {
                    rename = true;
                    position = null;
                    target = t.readString();
                    if (!PathUtils.isAbsolute(target)) {
                        target = PathUtils.concat(rootPath, target);
                    }
                }
                boolean before = false;
                if ("last".equals(position)) {
                    target = PathUtils.concat(target, name);
                    position = null;
                } else if ("first".equals(position)) {
                    target = PathUtils.concat(target, name);
                    position = null;
                    before = true;
                } else if ("before".equals(position)) {
                    position = PathUtils.getName(target);
                    target = PathUtils.getParentPath(target);
                    target = PathUtils.concat(target, name);
                    before = true;
                } else if ("after".equals(position)) {
                    position = PathUtils.getName(target);
                    target = PathUtils.getParentPath(target);
                    target = PathUtils.concat(target, name);
                } else if (position == null) {
                    // move
                } else {
                    throw new MicroKernelException("position: " + position);
                }
                to = PathUtils.relativize("/", target);
                boolean inPlaceRename = false;
                if (rename) {
                    if (PathUtils.getParentPath(path).equals(PathUtils.getParentPath(to))) {
                        inPlaceRename = true;
                        position = PathUtils.getName(path);
                    }
                }
                // TODO support move operations
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
        commit.apply(store);
        headRevision = rev.toString();
        return headRevision;
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
    }

    @Override
    public String branch(String trunkRevisionId) throws MicroKernelException {
        // TODO implement if needed
        return null;
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO implement if needed
        return null;
    }

    @Override
    @Nonnull
    public String rebase(@Nonnull String branchRevisionId, String newBaseRevisionId)
            throws MicroKernelException {
        // TODO implement if needed
        return null;
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

    }

    public DocumentStore getDocumentStore() {
        return store;
    }

}
