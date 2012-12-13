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

import org.apache.jackrabbit.mk.model.ChildNodeEntries;
import org.apache.jackrabbit.mk.model.ChildNodeEntriesMap;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.MutableCommit;
import org.apache.jackrabbit.mk.model.MutableNode;
import org.apache.jackrabbit.mk.model.Node;
import org.apache.jackrabbit.mk.model.NodeDiffHandler;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.NodeStateDiff;
import org.apache.jackrabbit.mk.persistence.GCPersistence;
import org.apache.jackrabbit.mk.persistence.Persistence;
import org.apache.jackrabbit.mk.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Default revision store implementation, passing calls to a {@code Persistence}
 * and a {@code BlobStore}, respectively and providing caching.
 */
public class DefaultRevisionStore extends AbstractRevisionStore implements
        Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRevisionStore.class);
    
    public static final String CACHE_SIZE = "mk.cacheSize";
    
    // default cache size is 32 MB
    public static final int DEFAULT_CACHE_SIZE = 32 * 1024 * 1024;

    private boolean initialized;
    private Id head;

    private final AtomicLong commitCounter = new AtomicLong();

    private final ReentrantReadWriteLock headLock = new ReentrantReadWriteLock();
    private final Persistence pm;
    protected final GCPersistence gcpm;

    /* avoid synthetic accessor */ int initialCacheSize;
    /* avoid synthetic accessor */ Cache<Id, CacheObject> cache;

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
    
    private int markedNodes, markedCommits;

    /**
     * GC executor.
     */
    private ScheduledExecutorService gcExecutor;
    
    /**
     * Active put tokens (Key: token, Value: null).
     */
    private final Map<PutTokenImpl, Object> putTokens = Collections.synchronizedMap(new WeakHashMap<PutTokenImpl, Object>());

    /**
     * Read-write lock for put tokens.
     */
    private final ReentrantReadWriteLock tokensLock = new ReentrantReadWriteLock();

    /**
     * Active branches (Key: current branch head, Value: branch root id).
     */
    private final TreeMap<Id,Id> branches = new TreeMap<Id, Id>();

    public DefaultRevisionStore(Persistence pm) {
        this(pm, (pm instanceof GCPersistence) ? (GCPersistence) pm : null);
    }

    /**
     * Alternative constructor that allows disabling of garbage collection
     * for an in-memory test repository.
     *
     * @param pm persistence manager
     * @param gcpm the same persistence manager, or {@code null} for no GC
     */
    public DefaultRevisionStore(Persistence pm, GCPersistence gcpm) {
        this.pm = pm;
        this.gcpm = gcpm;
    }

    public void initialize() throws Exception {
        if (initialized) {
            throw new IllegalStateException("already initialized");
        }

        initialCacheSize = determineInitialCacheSize();
        
        cache = CacheBuilder.newBuilder()
                .maximumWeight(initialCacheSize)
                .weigher(new Weigher<Id, CacheObject>() {
                    public int weigh(Id id, CacheObject obj) {
                        return obj.getMemory();
                    }
                })
                .build();

        // make sure we've got a HEAD commit
        Id[] ids = pm.readIds();
        head = ids[0];
        if (head == null || head.getBytes().length == 0) {
            // assume virgin repository
            byte[] rawHead = Id.fromLong(commitCounter.incrementAndGet())
                    .getBytes();
            head = new Id(rawHead);

            Id rootNodeId = pm.writeNode(new MutableNode(this));
            MutableCommit initialCommit = new MutableCommit();
            initialCommit.setCommitTS(System.currentTimeMillis());
            initialCommit.setRootNodeId(rootNodeId);
            pm.writeCommit(head, initialCommit);
            pm.writeHead(head);
        } else {
            Id lastCommitId = head;
            if (ids[1] != null && ids[1].compareTo(lastCommitId) > 0) {
                lastCommitId = ids[1];
            }
            commitCounter.set(Long.parseLong(lastCommitId.toString(), 16));
        }

        if (gcpm != null) {
            gcExecutor = Executors.newScheduledThreadPool(1,
                    new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "RevisionStore-GC");
                        }
                    });
            gcExecutor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    if (cache.size() >= initialCacheSize) {
                        gc();
                    }
                }
            }, 60, 1, TimeUnit.MINUTES); // TODO: Should start earlier
        }

        initialized = true;
    }
    
    public void close() {
        verifyInitialized();

        if (gcExecutor != null) {
            gcExecutor.shutdown();
        }

        cache.invalidateAll();

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
        tokensLock.readLock().lock();

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
            tokensLock.readLock().unlock();
        }
    }

    public Id putCNEMap(PutToken token, ChildNodeEntries map)
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

    public Id putHeadCommit(PutToken token, MutableCommit commit, Id branchRootId, Id branchRevId)
            throws Exception {
        verifyInitialized();
        if (!headLock.writeLock().isHeldByCurrentThread()) {
            throw new IllegalStateException(
                    "putHeadCommit called without holding write lock.");
        }

        if (commit.getBranchRootId() != null) {
            // OAK-267
            throw new IllegalStateException("private branch commit [" + commit + "] cannot become HEAD");
        }

        Id id = writeCommit(token, commit);
        setHeadCommitId(id);
        
        putTokens.remove(token);
        if (branchRevId != null) {
            synchronized (branches) {
                branches.remove(branchRevId);
            }
        }
        return id;
    }

    public Id putCommit(PutToken token, MutableCommit commit) throws Exception {
        verifyInitialized();

        Id commitId = writeCommit(token, commit);
        putTokens.remove(token);

        Id branchRootId = commit.getBranchRootId();
        if (branchRootId != null) {
            synchronized (branches) {
                Id parentId = commit.getParentId();
                if (!parentId.equals(branchRootId)) {
                    /* not the first branch commit, replace its head */
                    branches.remove(parentId);
                }
                branches.put(commitId, branchRootId);
            }
        }
        return commitId;
    }

    public void unlockHead() {
        headLock.writeLock().unlock();
    }

    // -----------------------------------------------------< RevisionProvider >

    public StoredNode getNode(final Id id) throws NotFoundException, Exception {
        verifyInitialized();

        StoredNode node = (StoredNode) cache.get(id,
                new Callable<StoredNode>() {
                    @Override
                    public StoredNode call() throws Exception {
                        StoredNode node = new StoredNode(id,
                                DefaultRevisionStore.this);
                        pm.readNode(node);
                        return node;
                    }
                });
        return node;
    }

    public ChildNodeEntriesMap getCNEMap(final Id id) throws NotFoundException,
            Exception {
        verifyInitialized();

        ChildNodeEntriesMap map = (ChildNodeEntriesMap) cache.get(id,
                new Callable<ChildNodeEntriesMap>() {
                    @Override
                    public ChildNodeEntriesMap call() throws Exception {
                        return pm.readCNEMap(id);
                    }
                });
        return map;
    }

    public StoredCommit getCommit(final Id id) throws NotFoundException,
            Exception {
        verifyInitialized();

        StoredCommit commit = (StoredCommit) cache.get(id,
                new Callable<StoredCommit>() {
                    @Override
                    public StoredCommit call() throws Exception {
                        return pm.readCommit(id);
                    }
                });
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
            public void childNodeAdded(ChildNodeEntry added) {
                String name = added.getName();
                diff.childNodeAdded(name, after.getChildNode(name));
            }

            @Override
            public void childNodeDeleted(ChildNodeEntry deleted) {
                String name = deleted.getName();
                diff.childNodeDeleted(name, before.getChildNode(name));
            }

            @Override
            public void childNodeChanged(ChildNodeEntry changed, Id newId) {
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
        
        LOG.debug("GC started.");
        markedCommits = markedNodes = 0;

        try {
            markUncommittedNodes();
            Id firstBranchRootId = markBranches();
            if (firstBranchRootId != null) {
                LOG.debug("First branch root to be preserved: {}", firstBranchRootId);
            }
            Id firstCommitId = markCommits();
            LOG.debug("First commit to be preserved: {}", firstCommitId);

            LOG.debug("Marked {} commits, {} nodes.", markedCommits, markedNodes);

            if (firstBranchRootId != null && firstBranchRootId.compareTo(firstCommitId) < 0) {
                firstCommitId = firstBranchRootId;
            }
            /* repair dangling parent commit of first preserved commit */
            StoredCommit commit = getCommit(firstCommitId);
            if (commit.getParentId() != null) {
                MutableCommit firstCommit = new MutableCommit(commit);
                firstCommit.setParentId(null);
                gcpm.replaceCommit(firstCommit.getId(), firstCommit);
            }
            
        } catch (Exception e) {
            /* unable to perform GC */
            LOG.error("Exception occurred in GC cycle", e);
            gcState.set(NOT_ACTIVE);
            return;
        }
        
        gcState.set(SWEEPING);

        try {
            int swept = gcpm.sweep();
            LOG.debug("GC cycle swept {} items", swept);
            cache.invalidateAll();
        } catch (Exception e) {
            LOG.error("Exception occurred in GC cycle", e);
        } finally {
            gcState.set(NOT_ACTIVE);
        }
        
        LOG.debug("GC stopped.");
    }
    
    /**
     * Mark nodes that have already been put but not committed yet.
     * 
     * @throws Exception
     *             if an error occurs
     */
    private void markUncommittedNodes() throws Exception {
        tokensLock.writeLock().lock();

        try {
            gcpm.start();
            gcState.set(MARKING);

            PutTokenImpl[] tokens = putTokens.keySet().toArray(new PutTokenImpl[putTokens.size()]);
            for (PutTokenImpl token : tokens) {
                markNode(token.getLastModified());
            }
        } finally {
            tokensLock.writeLock().unlock();
        }
    }

    /**
     * Mark branches.
     * 
     * @return first branch root id that needs to be preserved, or {@code null}
     * @throws Exception
     *             if an error occurs
     */
    @SuppressWarnings("unchecked")
    private Id markBranches() throws Exception {
        Map<Id,Id> tmpBranches;
        
        synchronized (branches) {
            tmpBranches = (Map<Id,Id>) branches.clone();
        }
        
        Id firstBranchRootId = null;
        
        /* Mark all branch commits */
        for (Entry<Id, Id> entry : tmpBranches.entrySet()) {
            Id branchRevId = entry.getKey();
            Id branchRootId = entry.getValue();
            while (!branchRevId.equals(branchRootId)) {
                StoredCommit commit = getCommit(branchRevId);
                markCommit(commit);
                branchRevId = commit.getParentId();
            }
            if (firstBranchRootId == null || firstBranchRootId.compareTo(branchRootId) > 0) {
                firstBranchRootId = branchRootId;
            }
        }
        /* Mark all master commits till the first branch root id */
        if (firstBranchRootId != null) {
            StoredCommit commit = getHeadCommit();

            for (;;) {
                markCommit(commit);
                if (commit.getId().equals(firstBranchRootId)) {
                    break;
                }
                commit = getCommit(commit.getParentId());
            }
            return firstBranchRootId;
        }
        return null;
    }
    
    /**
     * Mark all commits and nodes in a garbage collection cycle. Can be
     * customized by subclasses. The default implementation preserves all
     * commits that were created within 60 minutes of the current head commit.
     * <p/>
     * If this method throws an exception, the cycle will be stopped without
     * sweeping.
     * 
     * @return first commit id that will be preserved
     * @throws Exception
     *             if an error occurs
     */
    protected Id markCommits() throws Exception {
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
        return commit.getId();
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
        markedCommits++;

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
        markedNodes++;
        
        Iterator<ChildNodeEntry> iter = node.getChildNodeEntries(0, -1);
        while (iter.hasNext()) {
            ChildNodeEntry c = iter.next();
            markNode(getNode(c.getId()));
        }
    }
}
