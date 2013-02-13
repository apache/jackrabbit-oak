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

import javax.annotation.Nonnull;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;

public class MongoMK implements MicroKernel {
    
    /**
     * The MongoDB store (might be used by multiple MongoMKs).
     */
    private final DocumentStore store;
    
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
     * The set of known valid revision.
     * The key is the revision id, the value is 1 (because a cache can't be a set).
     */
    private final Map<String, Long> revCache = new Cache<String, Long>(1024);

    /**
     * Create a new MongoMK.
     * 
     * @param store the store (might be shared)
     * @param clusterId the cluster id (must be unique)
     */
    public MongoMK(MemoryDocumentStore store, int clusterId) {
        this.store = store;
        this.clusterId = clusterId;
    }
    
    String getNewRevision() {
        return Utils.createRevision(clusterId);
    }
    
    /**
     * Get the node for the given path and revision. The returned object might
     * not be modified directly.
     * 
     * @param path
     * @param rev
     * @return the node
     */
    Node getNode(String path, String rev) {
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
    
    /**
     * Try to add a node.
     * 
     * @param rev the revision
     * @param n the node
     * @throw IllegalStateException if the node already existed
     */
    public void addNode(String rev, String commitRoot, Node n) {
        UpdateOp node = new UpdateOp(n.path);
        int depth = Utils.pathDepth(n.path);
        node.set("_path", n.path);
        node.set("_pathDepth", depth);
        int commitDepth = depth - Utils.pathDepth(commitRoot);
        node.addMapEntry("_commitDepth", rev, commitDepth);
        
        // the affected (direct) children of this revision
        node.addMapEntry("_affectedChildren", rev, "");
        
        node.increment("_changeCount", 1L);
//        setCommitRoot(path);


        for (String p : n.properties.keySet()) {
            node.addMapEntry(p, rev, n.properties.get(p));
        }
        Map<String, Object> map = store.createOrUpdate(DocumentStore.Collection.NODES, node);        
        if (map != null) {
            // TODO rollback changes
            throw new IllegalStateException("Node already exists: " + n.path);
        }
    }
    
    private Node readNode(String path, String rev) {
        Map<String, Object> map = store.find(DocumentStore.Collection.NODES, path);        
        if (map == null) {
            return null;
        }
        Node n = new Node(path, rev);
        for(String key : map.keySet()) {
            Object v = map.get(key);
            
        }
        return n;
    }
    
    @Override
    public String getHeadRevision() throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path,
            int depth) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String commit(String path, String jsonDiff, String revisionId,
            String message) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String branch(String trunkRevisionId) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    @Nonnull
    public String rebase(@Nonnull String branchRevisionId, String newBaseRevisionId)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        // TODO Auto-generated method stub
        return null;
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

}
