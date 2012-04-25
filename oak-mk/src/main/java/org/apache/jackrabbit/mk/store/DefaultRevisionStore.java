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
import java.util.Iterator;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.mk.model.ChildNode;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.model.MutableNode;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.NodeDiffHandler;
import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.NodeStateDiff;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.persistence.GCPersistence;
import org.apache.jackrabbit.mk.persistence.Persistence;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;

/**
 * Default revision store implementation, passing calls to a {@code Persistence}
 * and a {@code BlobStore}, respectively and providing caching.
 */
public class DefaultRevisionStore extends AbstractRevisionStore implements
        Closeable {

    public static final String CACHE_SIZE = "mk.cacheSize";
    public static final int DEFAULT_CACHE_SIZE = 10000;

    private boolean initialized;
    private Id head;
    private AtomicLong commitCounter;
    private final ReentrantReadWriteLock headLock = new ReentrantReadWriteLock();
    private final Persistence pm;
    protected final GCPersistence gcpm;

    /* avoid synthetic accessor */int initialCacheSize;
    /* avoid synthetic accessor */Map<Id, Object> cache;

    /**
     * GC run state constants.
     */
    private static final int NOT_ACTIVE = 0;
    private static final int STARTING = 1;
    private static final int MARKING = 2;
    private static final int SWEEPING = 3;

    /**
     * GC run state.
     */
    private final AtomicInteger gcState = new AtomicInteger();

    /**
     * GC executor.
     */
    private ScheduledExecutorService gcExecutor;

    /**
     * Put tokens (Key: token, Value: null).
     */
    private final Map<PutTokenImpl, Object> putTokens = Collections.synchronizedMap(new WeakHashMap<PutTokenImpl, Object>());

    /**
     * Mark lock for put tokens.
     */
    private final ReentrantReadWriteLock markLock = new ReentrantReadWriteLock();

    public DefaultRevisionStore(Persistence pm) {
        this.pm = pm;
        this.gcpm = (pm instanceof GCPersistence) ? (GCPersistence) pm : null;

        commitCounter = new AtomicLong();
    }

    public void initialize() throws Exception {
        if (initialized) {
            throw new IllegalStateException("already initialized");
        }

        initialCacheSize = determineInitialCacheSize();
        cache = Collections.synchronizedMap(SimpleLRUCache
                .<Id, Object> newInstance(initialCacheSize));

        // make sure we've got a HEAD commit
        head = pm.readHead();
        if (head == null || head.getBytes().length == 0) {
            // assume virgin repository
            byte[] rawHead = Id.fromLong(commitCounter.incrementAndGet())
                    .getBytes();
            head = new Id(rawHead);

            Id rootNodeId = pm.writeNode(new MutableNode(this, "/"));
            MutableCommit initialCommit = new MutableCommit();
            initialCommit.setCommitTS(System.currentTimeMillis());
            initialCommit.setRootNodeId(rootNodeId);
            pm.writeCommit(head, initialCommit);
            pm.writeHead(head);
        } else {
            commitCounter.set(Long.parseLong(head.toString(), 16));
        }

        if (gcpm != null) {
            gcExecutor = Executors.newScheduledThreadPool(1);
            gcExecutor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    if (cache.size() >= initialCacheSize) {
                        gc();
                    }
                }
            }, 60, 60, TimeUnit.SECONDS);
        }

        initialized = true;
    }

    public void close() {
        verifyInitialized();

        if (gcExecutor != null) {
            gcExecutor.shutdown();
        }

        cache.clear();

        IOUtils.closeQuietly(pm);

        initialized = false;
    }

    protected void verifyInitialized() {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
    }

    protected static int determineInitialCacheSize() {
        String val = System.getProperty(CACHE_SIZE);
        return (val != null) ? Integer.parseInt(val) : DEFAULT_CACHE_SIZE;
    }

    // --------------------------------------------------------< RevisionStore >

    /**
     * Put token implementation.
     */
    static class PutTokenImpl extends PutToken {

        private static int idCounter;
        private int id;
        private StoredNode lastModifiedNode;

        public PutTokenImpl() {
            this.id = ++idCounter;
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof PutTokenImpl) {
                return ((PutTokenImpl) obj).id == id;
            }
            return super.equals(obj);
        }

        public void updateLastModifed(StoredNode lastModifiedNode) {
            this.lastModifiedNode = lastModifiedNode;
        }

        public StoredNode getLastModified() {
            return lastModifiedNode;
        }
    }

    public RevisionStore.PutToken createPutToken() {
        return new PutTokenImpl();
    }

    public Id putNode(PutToken token, MutableNode node) throws Exception {
        verifyInitialized();

        PersistHook callback = null;
        if (node instanceof PersistHook) {
            callback = (PersistHook) node;
            callback.prePersist(this, token);
        }

        /*
         * Make sure that a GC cycle can not sweep this newly persisted node
         * before we have updated our token
         */
        markLock.readLock().lock();

        try {
            Id id = pm.writeNode(node);

            if (callback != null) {
                callback.postPersist(this, token);
            }

            StoredNode snode = new StoredNode(id, node, this);
            cache.put(id, snode);

            PutTokenImpl pti = (PutTokenImpl) token;
            pti.updateLastModifed(snode);
            putTokens.put(pti, null);
            return id;

        } finally {
            markLock.readLock().unlock();
        }
    }

    public Id putCNEMap(PutToken token, ChildNodeEntriesMap map)
            throws Exception {
        verifyInitialized();

        PersistHook callback = null;
        if (map instanceof PersistHook) {
            callback = (PersistHook) map;
            callback.prePersist(this, token);
        }

        Id id = pm.writeCNEMap(map);

        if (callback != null) {
            callback.postPersist(this, token);
        }

        cache.put(id, map);

        return id;
    }

    public void lockHead() {
        headLock.writeLock().lock();
    }

    public Id putHeadCommit(PutToken token, MutableCommit commit)
            throws Exception {
        verifyInitialized();
        if (!headLock.writeLock().isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "putHeadCommit called without holding write lock.");
        }

        Id id = writeCommit(token, commit);
        setHeadCommitId(id);
        putTokens.remove(token);

        return id;
    }

    public Id putCommit(PutToken token, MutableCommit commit) throws Exception {
        verifyInitialized();

        Id commitId = writeCommit(token, commit);
        putTokens.remove(token);

        return commitId;
    }

    public void unlockHead() {
        headLock.writeLock().unlock();
    }

    // -----------------------------------------------------< RevisionProvider >

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

    public ChildNodeEntriesMap getCNEMap(Id id) throws NotFoundException,
            Exception {
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

    public StoredNode getRootNode(Id commitId) throws NotFoundException,
            Exception {
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

    // -------------------------------------------------------< implementation >

    private Id writeCommit(RevisionStore.PutToken token, MutableCommit commit)
            throws Exception {
        PersistHook callback = null;
        if (commit instanceof PersistHook) {
            callback = (PersistHook) commit;
            callback.prePersist(this, token);
        }

        Id id = commit.getId();
        if (id == null) {
            id = Id.fromLong(commitCounter.incrementAndGet());
        }
        pm.writeCommit(id, commit);

        if (callback != null) {
            callback.postPersist(this, token);
        }
        cache.put(id, new StoredCommit(id, commit));
        return id;
    }

    private void setHeadCommitId(Id id) throws Exception {
        // non-synchronized since we're called from putHeadCommit
        // which requires a write lock
        pm.writeHead(id);
        head = id;

        long counter = Long.parseLong(id.toString(), 16);
        if (counter > commitCounter.get()) {
            commitCounter.set(counter);
        }
    }

    // ------------------------------------------------------------< overrides >

    @Override
    public void compare(final NodeState before, final NodeState after,
            final NodeStateDiff diff) {
        // OAK-46: Efficient diffing of large child node lists

        Node beforeNode = ((StoredNodeAsState) before).unwrap();
        Node afterNode = ((StoredNodeAsState) after).unwrap();

        beforeNode.diff(afterNode, new NodeDiffHandler() {
            @Override
            public void propAdded(String propName, String value) {
                diff.propertyAdded(after.getProperty(propName));
            }

            @Override
            public void propChanged(String propName, String oldValue,
                    String newValue) {
                diff.propertyChanged(before.getProperty(propName),
                        after.getProperty(propName));
            }

            @Override
            public void propDeleted(String propName, String value) {
                diff.propertyDeleted(before.getProperty(propName));
            }

            @Override
            public void childNodeAdded(ChildNode added) {
                String name = added.getName();
                diff.childNodeAdded(name, after.getChildNode(name));
            }

            @Override
            public void childNodeDeleted(ChildNode deleted) {
                String name = deleted.getName();
                diff.childNodeDeleted(name, before.getChildNode(name));
            }

            @Override
            public void childNodeChanged(ChildNode changed, Id newId) {
                String name = changed.getName();
                diff.childNodeChanged(name, before.getChildNode(name),
                        after.getChildNode(name));
            }
        });
    }

    // -----------------------------------------------------------------------
    // GC

    /**
     * Perform a garbage collection. If a garbage collection cycle is already
     * running, this method returns immediately.
     */
    public void gc() {
        if (gcpm == null || !gcState.compareAndSet(NOT_ACTIVE, STARTING)) {
            // already running
            return;
        }

        // Mark all nodes that belong to currently active puts
        markLock.writeLock().lock();

        try {
            gcpm.start();
            gcState.set(MARKING);

            for (PutTokenImpl token : putTokens.keySet()) {
                markNode(token.getLastModified());
            }

        } catch (Exception e) {
            /* unable to perform GC */
            gcState.set(NOT_ACTIVE);
            e.printStackTrace();
            return;
        } finally {
            markLock.writeLock().unlock();
        }

        try {
            doMark();
        } catch (Exception e) {
            /* unable to perform GC */
            gcState.set(NOT_ACTIVE);
            e.printStackTrace();
            return;
        }
        gcState.set(SWEEPING);

        try {
            gcpm.sweep();
            cache.clear();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            gcState.set(NOT_ACTIVE);
        }
    }

    /**
     * Mark all commits and nodes in a garbage collection cycle. Can be
     * customized by subclasses. If this method throws an exception, the cycle
     * will be stopped without sweeping.
     * 
     * @throws Exception
     *             if an error occurs
     */
    protected void doMark() throws Exception {
        StoredCommit commit = getHeadCommit();
        long tsLimit = commit.getCommitTS() - (60 * 60 * 1000);

        for (;;) {
            markCommit(commit);
            Id id = commit.getParentId();
            if (id == null) {
                break;
            }
            StoredCommit parentCommit = getCommit(id);
            if (parentCommit.getCommitTS() < tsLimit) {
                break;
            }
            commit = parentCommit;
        }
        if (commit.getParentId() != null) {
            MutableCommit firstCommit = new MutableCommit(commit);
            firstCommit.setParentId(null);
            gcpm.replaceCommit(firstCommit.getId(), firstCommit);
        }
    }

    /**
     * Mark a commit. This marks all nodes belonging to this commit as well.
     * 
     * @param commit commit
     * @throws Exception if an error occurs
     */
    protected void markCommit(StoredCommit commit) throws Exception {
        if (!gcpm.markCommit(commit.getId())) {
            return;
        }
        markNode(getNode(commit.getRootNodeId()));
    }

    /**
     * Mark a node. This marks all children as well.
     * 
     * @param node node
     * @throws Exception if an error occurs
     */
    private void markNode(StoredNode node) throws Exception {
        if (!gcpm.markNode(node.getId())) {
            return;
        }
        Iterator<ChildNode> iter = node.getChildNodeEntries(0, -1);
        while (iter.hasNext()) {
            ChildNode c = iter.next();
            markNode(getNode(c.getId()));
        }
    }
}
