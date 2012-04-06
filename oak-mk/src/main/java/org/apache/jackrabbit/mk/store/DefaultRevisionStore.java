/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.store;

import java.io.Closeable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.model.MutableNode;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.persistence.Persistence;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;

/**
 * Default revision store implementation, passing calls to a <code>Persistence</code>
 * and a <code>BlobStore</code>, respectively and providing caching. 
 */
public class DefaultRevisionStore extends AbstractRevisionStore
        implements Closeable {

    public static final String CACHE_SIZE = "mk.cacheSize";
    public static final int DEFAULT_CACHE_SIZE = 10000;

    private boolean initialized;
    private Id head;
    private long headCounter;
    private final ReentrantReadWriteLock headLock = new ReentrantReadWriteLock();
    private final Persistence pm;

    private Map<Id, Object> cache;

    public DefaultRevisionStore(Persistence pm) {
        this.pm = pm;
    }
    
    public void initialize() throws Exception {
        if (initialized) {
            throw new IllegalStateException("already initialized");
        }

        cache = Collections.synchronizedMap(SimpleLRUCache.<Id, Object>newInstance(determineInitialCacheSize()));

        // make sure we've got a HEAD commit
        head = pm.readHead();
        if (head == null || head.getBytes().length == 0) {
            // assume virgin repository
            byte[] rawHead = longToBytes(++headCounter);
            head = new Id(rawHead);
            
            Id rootNodeId = pm.writeNode(new MutableNode(this, "/"));
            MutableCommit initialCommit = new MutableCommit();
            initialCommit.setCommitTS(System.currentTimeMillis());
            initialCommit.setRootNodeId(rootNodeId);
            pm.writeCommit(head, initialCommit);
            pm.writeHead(head);
        } else {
            headCounter = Long.parseLong(head.toString(), 16);
        }

        initialized = true;
    }

    public void close() {
        verifyInitialized();

        cache.clear();
        if (pm instanceof Closeable) {
            IOUtils.closeQuietly((Closeable) pm);
        }
        initialized = false;
    }

    protected void verifyInitialized() {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
    }

    protected int determineInitialCacheSize() {
        String val = System.getProperty(CACHE_SIZE);
        return (val != null) ? Integer.parseInt(val) : DEFAULT_CACHE_SIZE;
    }

    /**
     * Convert a long value into a fixed-size byte array of size 8.
     * 
     * @param value value
     * @return byte array
     */
    private static byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        
        for (int i = result.length - 1; i >= 0 && value != 0; i--) {
            result[i] = (byte) (value & 0xff);
            value >>>= 8;
        }
        return result;
    }

    //--------------------------------------------------------< RevisionStore >

    public Id putNode(MutableNode node) throws Exception {
        verifyInitialized();

        PersistHook callback = null;
        if (node instanceof PersistHook) {
            callback = (PersistHook) node;
            callback.prePersist(this);
        }

        Id id = pm.writeNode(node);
        
        if (callback != null)  {
            callback.postPersist(this);
        }
        
        cache.put(id, new StoredNode(id, node, this));

        return id;
    }

    public Id putCNEMap(ChildNodeEntriesMap map) throws Exception {
        verifyInitialized();

        PersistHook callback = null;
        if (map instanceof PersistHook) {
            callback = (PersistHook) map;
            callback.prePersist(this);
        }

       Id id = pm.writeCNEMap(map);

        if (callback != null)  {
            callback.postPersist(this);
        }

        cache.put(id, map);

        return id;
    }

    public void lockHead() {
        headLock.writeLock().lock();
    }

    public Id putHeadCommit(MutableCommit commit) throws Exception {
        verifyInitialized();
        if (!headLock.writeLock().isHeldByCurrentThread()) {
            throw new IllegalStateException("putCommit called without holding write lock.");
        }

        PersistHook callback = null;
        if (commit instanceof PersistHook) {
            callback = (PersistHook) commit;
            callback.prePersist(this);
        }

        Id id = commit.getId();
        if (id == null) {
            id = new Id(longToBytes(++headCounter));
        }
        pm.writeCommit(id, commit);
        setHeadCommitId(id);

        if (callback != null)  {
            callback.postPersist(this);
        }
        cache.put(id, new StoredCommit(id, commit));

        return id;
    }

    private void setHeadCommitId(Id id) throws Exception {
        pm.writeHead(id);
        head = id;
        
        long headCounter = Long.parseLong(id.toString(), 16);
        if (headCounter > this.headCounter) {
            this.headCounter = headCounter;
        }
    }

    public void unlockHead() {
        headLock.writeLock().unlock();
    }

    //-----------------------------------------------------< RevisionProvider >

    public StoredNode getNode(Id id) throws NotFoundException, Exception {
        verifyInitialized();

        StoredNode node = (StoredNode) cache.get(id);
        if (node != null) {
            return node;
        }

        node = new StoredNode(id, this);
        pm.readNode(node);

        cache.put(id, node);

        return node;
    }

    public ChildNodeEntriesMap getCNEMap(Id id) throws NotFoundException, Exception {
        verifyInitialized();

        ChildNodeEntriesMap map = (ChildNodeEntriesMap) cache.get(id);
        if (map != null) {
            return map;
        }

        map = pm.readCNEMap(id);

        cache.put(id, map);

        return map;
    }

    public StoredCommit getCommit(Id id) throws NotFoundException, Exception {
        verifyInitialized();

        StoredCommit commit = (StoredCommit) cache.get(id);
        if (commit != null) {
            return commit;
        }

        commit = pm.readCommit(id);
        cache.put(id, commit);

        return commit;
    }

    public StoredNode getRootNode(Id commitId) throws NotFoundException, Exception {
        return getNode(getCommit(commitId).getRootNodeId());
    }

    public StoredCommit getHeadCommit() throws Exception {
        return getCommit(getHeadCommitId());
    }

    public Id getHeadCommitId() throws Exception {
        verifyInitialized();

        headLock.readLock().lock();
        try {
            return head;
        } finally {
            headLock.readLock().unlock();
        }
    }

}
