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

import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.mongodb.DB;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.EmpiricalWeigher;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState.Children;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A MicroKernel implementation that stores the data in a {@link DocumentStore}.
 */
public class DocumentMK implements MicroKernel {

    static final Logger LOG = LoggerFactory.getLogger(DocumentMK.class);

    /**
     * The path where the persistent cache is stored.
     */
    static final String DEFAULT_PERSISTENT_CACHE_URI =
            System.getProperty("oak.documentMK.persCache");

    /**
     * The threshold where special handling for many child node starts.
     */
    static final int MANY_CHILDREN_THRESHOLD = Integer.getInteger(
            "oak.documentMK.manyChildren", 50);

    /**
     * Enable the LIRS cache.
     */
    static final boolean LIRS_CACHE = Boolean.parseBoolean(
            System.getProperty("oak.documentMK.lirsCache", "false"));

    /**
     * Enable fast diff operations.
     */
    static final boolean FAST_DIFF = Boolean.parseBoolean(
            System.getProperty("oak.documentMK.fastDiff", "true"));

    /**
     * The guava cache concurrency level.
     */
    static final int CACHE_CONCURRENCY = Integer.getInteger(
            "oak.documentMK.cacheConcurrency", 16);

    /**
     * The node store.
     */
    protected final DocumentNodeStore nodeStore;

    /**
     * The document store (might be used by multiple DocumentMKs).
     */
    protected final DocumentStore store;

    DocumentMK(Builder builder) {
        this.nodeStore = builder.getNodeStore();
        this.store = nodeStore.getDocumentStore();
    }

    public void dispose() {
        nodeStore.dispose();
    }

    void backgroundRead() {
        nodeStore.backgroundRead(true);
    }

    void backgroundWrite() {
        nodeStore.backgroundWrite();
    }

    void runBackgroundOperations() {
        nodeStore.runBackgroundOperations();
    }

    public DocumentNodeStore getNodeStore() {
        return nodeStore;
    }

    ClusterNodeInfo getClusterInfo() {
        return nodeStore.getClusterInfo();
    }

    int getPendingWriteCount() {
        return nodeStore.getPendingWriteCount();
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        return nodeStore.getHeadRevision().toString();
    }

    @Override @Nonnull
    public String checkpoint(long lifetime) throws MicroKernelException {
        try {
            return nodeStore.checkpoint(lifetime);
        } catch (DocumentStoreException e) {
            throw new MicroKernelException(e);
        }
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
    public String diff(String fromRevisionId,
                       String toRevisionId,
                       String path,
                       int depth) throws MicroKernelException {
        if (depth != 0) {
            throw new MicroKernelException("Only depth 0 is supported, depth is " + depth);
        }
        if (path == null || path.equals("")) {
            path = "/";
        }
        try {
            return nodeStore.diff(fromRevisionId, toRevisionId, path);
        } catch (DocumentStoreException e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        if (!PathUtils.isAbsolute(path)) {
            throw new MicroKernelException("Path is not absolute: " + path);
        }
        revisionId = revisionId != null ? revisionId : nodeStore.getHeadRevision().toString();
        Revision rev = Revision.fromString(revisionId);
        DocumentNodeState n;
        try {
            n = nodeStore.getNode(path, rev);
        } catch (DocumentStoreException e) {
            throw new MicroKernelException(e);
        }
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
        revisionId = revisionId != null ? revisionId : nodeStore.getHeadRevision().toString();
        Revision rev = Revision.fromString(revisionId);
        try {
            DocumentNodeState n = nodeStore.getNode(path, rev);
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
            Children c = nodeStore.getChildren(n, null, max);
            for (long i = offset; i < c.children.size(); i++) {
                if (maxChildNodes-- <= 0) {
                    break;
                }
                String name = c.children.get((int) i);
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
        } catch (DocumentStoreException e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String commit(String rootPath, String jsonDiff, String baseRevId,
            String message) throws MicroKernelException {
        boolean success = false;
        boolean isBranch = false;
        Revision rev;
        Commit commit = nodeStore.newCommit(baseRevId != null ? Revision.fromString(baseRevId) : null, null);
        try {
            Revision baseRev = commit.getBaseRevision();
            isBranch = baseRev != null && baseRev.isBranch();
            parseJsonDiff(commit, jsonDiff, rootPath);
            rev = commit.apply();
            success = true;
        } catch (DocumentStoreException e) {
            throw new MicroKernelException(e);
        } finally {
            if (!success) {
                nodeStore.canceled(commit);
            } else {
                nodeStore.done(commit, isBranch, null);
            }
        }
        return rev.toString();
    }

    @Override
    public String branch(@Nullable String trunkRevisionId) throws MicroKernelException {
        // nothing is written when the branch is created, the returned
        // revision simply acts as a reference to the branch base revision
        Revision revision = trunkRevisionId != null
                ? Revision.fromString(trunkRevisionId) : nodeStore.getHeadRevision();
        return revision.asBranchRevision().toString();
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        // TODO improve implementation if needed
        Revision revision = Revision.fromString(branchRevisionId);
        if (!revision.isBranch()) {
            throw new MicroKernelException("Not a branch: " + branchRevisionId);
        }
        try {
            return nodeStore.merge(revision, null).toString();
        } catch (DocumentStoreException e) {
            throw new MicroKernelException(e);
        } catch (CommitFailedException e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    @Nonnull
    public String rebase(@Nonnull String branchRevisionId,
                         @Nullable String newBaseRevisionId)
            throws MicroKernelException {
        Revision r = Revision.fromString(branchRevisionId);
        Revision base = newBaseRevisionId != null ?
                Revision.fromString(newBaseRevisionId) :
                nodeStore.getHeadRevision();
        return nodeStore.rebase(r, base).toString();
    }

    @Nonnull
    @Override
    public String reset(@Nonnull String branchRevisionId,
                        @Nonnull String ancestorRevisionId)
            throws MicroKernelException {
        Revision branch = Revision.fromString(branchRevisionId);
        if (!branch.isBranch()) {
            throw new MicroKernelException("Not a branch revision: " + branchRevisionId);
        }
        Revision ancestor = Revision.fromString(ancestorRevisionId);
        if (!ancestor.isBranch()) {
            throw new MicroKernelException("Not a branch revision: " + ancestorRevisionId);
        }
        try {
            return nodeStore.reset(branch, ancestor, null).toString();
        } catch (DocumentStoreException e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        try {
            return nodeStore.getBlobStore().getBlobLength(blobId);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {
        try {
            return nodeStore.getBlobStore().readBlob(blobId, pos, buff, off, length);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        try {
            return nodeStore.getBlobStore().writeBlob(in);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    //-------------------------< accessors >------------------------------------

    public DocumentStore getDocumentStore() {
        return store;
    }

    //------------------------------< internal >--------------------------------

    private void parseJsonDiff(Commit commit, String json, String rootPath) {
        Revision baseRev = commit.getBaseRevision();
        String baseRevId = baseRev != null ? baseRev.toString() : null;
        Set<String> added = Sets.newHashSet();
        JsopReader t = new JsopTokenizer(json);
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
                    added.add(path);
                    break;
                case '-':
                    DocumentNodeState toRemove = nodeStore.getNode(path, commit.getBaseRevision());
                    if (toRemove == null) {
                        throw new MicroKernelException("Node not found: " + path + " in revision " + baseRevId);
                    }
                    commit.removeNode(path);
                    nodeStore.markAsDeleted(toRemove, commit, true);
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
                    if (!added.contains(p) && nodeStore.getNode(p, commit.getBaseRevision()) == null) {
                        throw new DocumentStoreException("Node not found: " + path + " in revision " + baseRevId);
                    }
                    String propertyName = PathUtils.getName(path);
                    commit.updateProperty(p, propertyName, value);
                    commit.updatePropertyDiff(p, propertyName, value);
                    break;
                case '>': {
                    // TODO support moving nodes that were modified within this commit
                    t.read(':');
                    String targetPath = t.readString();
                    if (!PathUtils.isAbsolute(targetPath)) {
                        targetPath = PathUtils.concat(rootPath, targetPath);
                    }
                    DocumentNodeState source = nodeStore.getNode(path, baseRev);
                    if (source == null) {
                        throw new MicroKernelException("Node not found: " + path + " in revision " + baseRevId);
                    } else if (nodeExists(targetPath, baseRevId)) {
                        throw new MicroKernelException("Node already exists: " + targetPath + " in revision " + baseRevId);
                    }
                    commit.moveNode(path, targetPath);
                    nodeStore.moveNode(source, targetPath, commit);
                    break;
                }
                case '*': {
                    // TODO support copying nodes that were modified within this commit
                    t.read(':');
                    String targetPath = t.readString();
                    if (!PathUtils.isAbsolute(targetPath)) {
                        targetPath = PathUtils.concat(rootPath, targetPath);
                    }
                    DocumentNodeState source = nodeStore.getNode(path, baseRev);
                    if (source == null) {
                        throw new MicroKernelException("Node not found: " + path + " in revision " + baseRevId);
                    } else if (nodeExists(targetPath, baseRevId)) {
                        throw new MicroKernelException("Node already exists: " + targetPath + " in revision " + baseRevId);
                    }
                    commit.copyNode(path, targetPath);
                    nodeStore.copyNode(source, targetPath, commit);
                    break;
                }
                default:
                    throw new MicroKernelException("token: " + (char) t.getTokenType());
            }
        }
    }

    private void parseAddNode(Commit commit, JsopReader t, String path) {
        DocumentNodeState n = new DocumentNodeState(nodeStore, path, commit.getRevision());
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

    //----------------------------< Builder >-----------------------------------

    /**
     * A builder for a DocumentMK instance.
     */
    public static class Builder {
        private static final long DEFAULT_MEMORY_CACHE_SIZE = 256 * 1024 * 1024;
        public static final int DEFAULT_NODE_CACHE_PERCENTAGE = 25;
        public static final int DEFAULT_CHILDREN_CACHE_PERCENTAGE = 10;
        public static final int DEFAULT_DIFF_CACHE_PERCENTAGE = 5;
        public static final int DEFAULT_DOC_CHILDREN_CACHE_PERCENTAGE = 3;
        public static final int DEFAULT_CACHE_SEGMENT_COUNT = 16;
        public static final int DEFAULT_CACHE_STACK_MOVE_DISTANCE = 16;
        private DocumentNodeStore nodeStore;
        private DocumentStore documentStore;
        private DiffCache diffCache;
        private BlobStore blobStore;
        private int clusterId  = Integer.getInteger("oak.documentMK.clusterId", 0);
        private int asyncDelay = 1000;
        private boolean timing;
        private boolean logging;
        private Weigher<CacheValue, CacheValue> weigher = new EmpiricalWeigher();
        private long memoryCacheSize = DEFAULT_MEMORY_CACHE_SIZE;
        private int nodeCachePercentage = DEFAULT_NODE_CACHE_PERCENTAGE;
        private int childrenCachePercentage = DEFAULT_CHILDREN_CACHE_PERCENTAGE;
        private int diffCachePercentage = DEFAULT_DIFF_CACHE_PERCENTAGE;
        private int cacheSegmentCount = DEFAULT_CACHE_SEGMENT_COUNT;
        private int cacheStackMoveDistance = DEFAULT_CACHE_STACK_MOVE_DISTANCE;
        private boolean useSimpleRevision;
        private long splitDocumentAgeMillis = 5 * 60 * 1000;
        private long maxReplicationLagMillis = TimeUnit.HOURS.toMillis(6);
        private boolean disableBranches;
        private Clock clock = Clock.SIMPLE;
        private Executor executor;
        private String persistentCacheURI = DEFAULT_PERSISTENT_CACHE_URI;
        private PersistentCache persistentCache;

        public Builder() {
        }

        /**
         * Use the given MongoDB as backend storage for the DocumentNodeStore.
         *
         * @param db the MongoDB connection
         * @return this
         */
        public Builder setMongoDB(DB db, int blobCacheSizeMB) {
            if (db != null) {
                if (this.documentStore == null) {
                    this.documentStore = new MongoDocumentStore(db, this);
                }

                if (this.blobStore == null) {
                    GarbageCollectableBlobStore s = new MongoBlobStore(db, blobCacheSizeMB * 1024 * 1024L);
                    PersistentCache p = getPersistentCache();
                    if (p != null) {
                        s = p.wrapBlobStore(s);
                    }
                    this.blobStore = s;
                }
            }
            return this;
        }

        /**
         * Set the MongoDB connection to use. By default an in-memory store is used.
         *
         * @param db the MongoDB connection
         * @return this
         */
        public Builder setMongoDB(DB db) {
            return setMongoDB(db, 16);
        }

        /**
         * Sets a {@link DataSource} to use for the RDB document and blob
         * stores.
         *
         * @return this
         */
        public Builder setRDBConnection(DataSource ds) {
            this.documentStore = new RDBDocumentStore(ds, this);
            if(this.blobStore == null) {
                this.blobStore = new RDBBlobStore(ds);
            }
            return this;
        }

        /**
         * Sets a {@link DataSource} to use for the RDB document and blob
         * stores, including {@link RDBOptions}.
         *
         * @return this
         */
        public Builder setRDBConnection(DataSource ds, RDBOptions options) {
            this.documentStore = new RDBDocumentStore(ds, this, options);
            if(this.blobStore == null) {
                this.blobStore = new RDBBlobStore(ds, options);
            }
            return this;
        }

        /**
         * Sets the persistent cache option.
         *
         * @return this
         */
        public Builder setPersistentCache(String persistentCache) {
            this.persistentCacheURI = persistentCache;
            return this;
        }

        /**
         * Sets a {@link DataSource}s to use for the RDB document and blob
         * stores.
         *
         * @return this
         */
        public Builder setRDBConnection(DataSource documentStoreDataSource, DataSource blobStoreDataSource) {
            this.documentStore = new RDBDocumentStore(documentStoreDataSource, this);
            this.blobStore = new RDBBlobStore(blobStoreDataSource);
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

        public DocumentNodeStore getNodeStore() {
            if (nodeStore == null) {
                nodeStore = new DocumentNodeStore(this);
            }
            return nodeStore;
        }

        public DiffCache getDiffCache() {
            if (diffCache == null) {
                diffCache = new TieredDiffCache(this);
            }
            return diffCache;
        }

        public Builder setDiffCache(DiffCache diffCache) {
            this.diffCache = diffCache;
            return this;
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

        public Builder setCacheSegmentCount(int cacheSegmentCount) {
            this.cacheSegmentCount = cacheSegmentCount;
            return this;
        }

        public Builder setCacheStackMoveDistance(int cacheSegmentCount) {
            this.cacheStackMoveDistance = cacheSegmentCount;
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

        public Weigher<CacheValue, CacheValue> getWeigher() {
            return weigher;
        }

        public Builder withWeigher(Weigher<CacheValue, CacheValue> weigher) {
            this.weigher = weigher;
            return this;
        }

        public Builder memoryCacheSize(long memoryCacheSize) {
            this.memoryCacheSize = memoryCacheSize;
            return this;
        }

        public Builder memoryCacheDistribution(int nodeCachePercentage,
                                               int childrenCachePercentage,
                                               int diffCachePercentage) {
            checkArgument(nodeCachePercentage >= 0);
            checkArgument(childrenCachePercentage>= 0);
            checkArgument(diffCachePercentage >= 0);
            checkArgument(nodeCachePercentage + childrenCachePercentage +
                    diffCachePercentage < 100);
            this.nodeCachePercentage = nodeCachePercentage;
            this.childrenCachePercentage = childrenCachePercentage;
            this.diffCachePercentage = diffCachePercentage;
            return this;
        }

        /**
         * @deprecated Use {@link #memoryCacheDistribution(int, int, int)}
         *  instead. As of 1.0.40, this method ignores the
         *  {@code docChildrenCachePercentage}.
         */
        public Builder memoryCacheDistribution(int nodeCachePercentage,
                                               int childrenCachePercentage,
                                               int docChildrenCachePercentage,
                                               int diffCachePercentage) {
            return memoryCacheDistribution(nodeCachePercentage,
                    childrenCachePercentage, diffCachePercentage);
        }

        public long getNodeCacheSize() {
            return memoryCacheSize * nodeCachePercentage / 100;
        }

        public long getChildrenCacheSize() {
            return memoryCacheSize * childrenCachePercentage / 100;
        }

        public long getDocumentCacheSize() {
            return memoryCacheSize - getNodeCacheSize() - getChildrenCacheSize()
                    - getDiffCacheSize();
        }

        public long getDiffCacheSize() {
            return memoryCacheSize * diffCachePercentage / 100;
        }

        public long getMemoryDiffCacheSize() {
            return getDiffCacheSize() / 2;
        }

        public long getLocalDiffCacheSize() {
            return getDiffCacheSize() / 2;
        }

        public Builder setUseSimpleRevision(boolean useSimpleRevision) {
            this.useSimpleRevision = useSimpleRevision;
            return this;
        }

        public boolean isUseSimpleRevision() {
            return useSimpleRevision;
        }

        public Executor getExecutor() {
            if(executor == null){
                return MoreExecutors.sameThreadExecutor();
            }
            return executor;
        }

        public Builder setExecutor(Executor executor){
            this.executor = executor;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Clock getClock() {
            return clock;
        }

        public Builder setMaxReplicationLag(long duration, TimeUnit unit){
            maxReplicationLagMillis = unit.toMillis(duration);
            return this;
        }

        public long getMaxReplicationLagMillis() {
            return maxReplicationLagMillis;
        }

        public Builder disableBranches() {
            disableBranches = true;
            return this;
        }

        public boolean isDisableBranches() {
            return disableBranches;
        }

        VersionGCSupport createVersionGCSupport() {
            DocumentStore store = getDocumentStore();
            if (store instanceof MongoDocumentStore) {
                return new MongoVersionGCSupport((MongoDocumentStore) store);
            } else if (store instanceof RDBDocumentStore) {
                return new RDBVersionGCSupport((RDBDocumentStore) store);
            } else {
                return new VersionGCSupport(store);
            }
        }

        /**
         * Open the DocumentMK instance using the configured options.
         *
         * @return the DocumentMK instance
         */
        public DocumentMK open() {
            return new DocumentMK(this);
        }

        public Cache<PathRev, DocumentNodeState> buildNodeCache(DocumentNodeStore store) {
            return buildCache(CacheType.NODE, getNodeCacheSize(), store, null);
        }

        public Cache<PathRev, DocumentNodeState.Children> buildChildrenCache() {
            return buildCache(CacheType.CHILDREN, getChildrenCacheSize(), null, null);
        }

        public Cache<PathRev, StringValue> buildMemoryDiffCache() {
            return buildCache(CacheType.DIFF, getMemoryDiffCacheSize(), null, null);
        }

        public Cache<RevisionsKey, LocalDiffCache.Diff> buildLocalDiffCache() {
            return buildCache(CacheType.LOCAL_DIFF, getLocalDiffCacheSize(), null, null);
        }

        public Cache<CacheValue, NodeDocument> buildDocumentCache(DocumentStore docStore) {
            return buildCache(CacheType.DOCUMENT, getDocumentCacheSize(), null, docStore);
        }

        public NodeDocumentCache buildNodeDocumentCache(DocumentStore docStore, NodeDocumentLocks locks) {
            Cache<CacheValue, NodeDocument> cache = buildDocumentCache(docStore);
            CacheStats cacheStats = new CacheStats(cache, "Document-Documents", getWeigher(), getDocumentCacheSize());
            return new NodeDocumentCache(cache, cacheStats, locks);
        }

        private <K extends CacheValue, V extends CacheValue> Cache<K, V> buildCache(
                CacheType cacheType,
                long maxWeight,
                DocumentNodeStore docNodeStore,
                DocumentStore docStore
                ) {
            Cache<K, V> cache = buildCache(maxWeight);
            PersistentCache p = getPersistentCache();
            if (p != null) {
                if (docNodeStore != null) {
                    docNodeStore.setPersistentCache(p);
                }
                cache = p.wrap(docNodeStore, docStore, cache, cacheType);
            }
            return cache;
        }

        private PersistentCache getPersistentCache() {
            if (persistentCacheURI == null) {
                return null;
            }
            if (persistentCache == null) {
                try {
                    persistentCache = new PersistentCache(persistentCacheURI);
                } catch (Throwable e) {
                    LOG.warn("Persistent cache not available; please disable the configuration", e);
                    throw new IllegalArgumentException(e);
                }
            }
            return persistentCache;
        }

        private <K extends CacheValue, V extends CacheValue> Cache<K, V> buildCache(
                long maxWeight) {
            // do not use LIRS cache when maxWeight is zero (OAK-6953)
            if ((LIRS_CACHE || persistentCacheURI != null) && maxWeight > 0) {
                return CacheLIRS.newBuilder().
                        weigher(weigher).
                        averageWeight(2000).
                        maximumWeight(maxWeight).
                        segmentCount(cacheSegmentCount).
                        stackMoveDistance(cacheStackMoveDistance).
                        recordStats().
                        build();
            }
            return CacheBuilder.newBuilder().
                    concurrencyLevel(CACHE_CONCURRENCY).
                    weigher(weigher).
                    maximumWeight(maxWeight).
                    recordStats().
                    build();
        }

    }

}
