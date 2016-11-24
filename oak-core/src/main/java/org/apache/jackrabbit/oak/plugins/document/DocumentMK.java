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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import com.mongodb.DB;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.cache.EmpiricalWeigher;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreStats;
import org.apache.jackrabbit.oak.plugins.blob.CachingBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState.Children;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobReferenceIterator;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoBlobStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoMissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.CacheType;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.EvictionListener;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCache;
import org.apache.jackrabbit.oak.plugins.document.persistentCache.PersistentCacheStats;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBVersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.RevisionsKey;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JSON-based wrapper around the NodeStore implementation that stores the
 * data in a {@link DocumentStore}. It is used for testing purpose only.
 */
public class DocumentMK {

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
     * Enable or disable the LIRS cache (null to use the default setting for this configuration).
     */
    static final Boolean LIRS_CACHE;

    static {
        String s = System.getProperty("oak.documentMK.lirsCache");
        LIRS_CACHE = s == null ? null : Boolean.parseBoolean(s);
    }

    /**
     * Enable fast diff operations.
     */
    static final boolean FAST_DIFF = Boolean.parseBoolean(
            System.getProperty("oak.documentMK.fastDiff", "true"));

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
        nodeStore.backgroundRead();
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

    public String getHeadRevision() throws DocumentStoreException {
        return nodeStore.getHeadRevision().toString();
    }

    public String checkpoint(long lifetime) throws DocumentStoreException {
        try {
            return nodeStore.checkpoint(lifetime);
        } catch (DocumentStoreException e) {
            throw new DocumentStoreException(e);
        }
    }

    public String diff(String fromRevisionId,
                       String toRevisionId,
                       String path,
                       int depth) throws DocumentStoreException {
        if (depth != 0) {
            throw new DocumentStoreException("Only depth 0 is supported, depth is " + depth);
        }
        if (path == null || path.equals("")) {
            path = "/";
        }
        RevisionVector fromRev = RevisionVector.fromString(fromRevisionId);
        RevisionVector toRev = RevisionVector.fromString(toRevisionId);
        final DocumentNodeState before = nodeStore.getNode(path, fromRev);
        final DocumentNodeState after = nodeStore.getNode(path, toRev);
        if (before == null || after == null) {
            String msg = String.format("Diff is only supported if the node exists in both cases. " +
                            "Node [%s], fromRev [%s] -> %s, toRev [%s] -> %s",
                    path, fromRev, before != null, toRev, after != null);
            throw new DocumentStoreException(msg);
        }

        JsopDiff diff = new JsopDiff(path, depth);
        after.compareAgainstBaseState(before, diff);
        return diff.toString();
    }

    public boolean nodeExists(String path, String revisionId)
            throws DocumentStoreException {
        if (!PathUtils.isAbsolute(path)) {
            throw new DocumentStoreException("Path is not absolute: " + path);
        }
        revisionId = revisionId != null ? revisionId : nodeStore.getHeadRevision().toString();
        RevisionVector rev = RevisionVector.fromString(revisionId);
        DocumentNodeState n;
        try {
            n = nodeStore.getNode(path, rev);
        } catch (DocumentStoreException e) {
            throw new DocumentStoreException(e);
        }
        return n != null;
    }

    public String getNodes(String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws DocumentStoreException {
        if (depth != 0) {
            throw new DocumentStoreException("Only depth 0 is supported, depth is " + depth);
        }
        revisionId = revisionId != null ? revisionId : nodeStore.getHeadRevision().toString();
        RevisionVector rev = RevisionVector.fromString(revisionId);
        try {
            DocumentNodeState n = nodeStore.getNode(path, rev);
            if (n == null) {
                return null;
            }
            JsopStream json = new JsopStream();
            boolean includeId = filter != null && filter.contains(":id");
            includeId |= filter != null && filter.contains(":hash");
            json.object();
            append(n, json, includeId);
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
                json.key(":childNodeCount").value(Long.MAX_VALUE);
            } else {
                json.key(":childNodeCount").value(c.children.size());
            }
            json.endObject();
            return json.toString();
        } catch (DocumentStoreException e) {
            throw new DocumentStoreException(e);
        }
    }

    public String commit(String rootPath, String jsonDiff, String baseRevId,
            String message) throws DocumentStoreException {
        boolean success = false;
        boolean isBranch;
        RevisionVector rev;
        Commit commit = nodeStore.newCommit(baseRevId != null ? RevisionVector.fromString(baseRevId) : null, null);
        try {
            RevisionVector baseRev = commit.getBaseRevision();
            isBranch = baseRev != null && baseRev.isBranch();
            parseJsonDiff(commit, jsonDiff, rootPath);
            commit.apply();
            rev = nodeStore.done(commit, isBranch, CommitInfo.EMPTY);
            success = true;
        } catch (DocumentStoreException e) {
            throw new DocumentStoreException(e);
        } finally {
            if (!success) {
                nodeStore.canceled(commit);
            }
        }
        return rev.toString();
    }

    public String branch(@Nullable String trunkRevisionId) throws DocumentStoreException {
        // nothing is written when the branch is created, the returned
        // revision simply acts as a reference to the branch base revision
        RevisionVector revision = trunkRevisionId != null
                ? RevisionVector.fromString(trunkRevisionId) : nodeStore.getHeadRevision();
        return revision.asBranchRevision(nodeStore.getClusterId()).toString();
    }

    public String merge(String branchRevisionId, String message)
            throws DocumentStoreException {
        RevisionVector revision = RevisionVector.fromString(branchRevisionId);
        if (!revision.isBranch()) {
            throw new DocumentStoreException("Not a branch: " + branchRevisionId);
        }
        try {
            return nodeStore.merge(revision, CommitInfo.EMPTY).toString();
        } catch (DocumentStoreException e) {
            throw new DocumentStoreException(e);
        } catch (CommitFailedException e) {
            throw new DocumentStoreException(e);
        }
    }

    @Nonnull
    public String rebase(@Nonnull String branchRevisionId,
                         @Nullable String newBaseRevisionId)
            throws DocumentStoreException {
        RevisionVector r = RevisionVector.fromString(branchRevisionId);
        RevisionVector base = newBaseRevisionId != null ?
                RevisionVector.fromString(newBaseRevisionId) :
                nodeStore.getHeadRevision();
        return nodeStore.rebase(r, base).toString();
    }

    @Nonnull
    public String reset(@Nonnull String branchRevisionId,
                        @Nonnull String ancestorRevisionId)
            throws DocumentStoreException {
        RevisionVector branch = RevisionVector.fromString(branchRevisionId);
        if (!branch.isBranch()) {
            throw new DocumentStoreException("Not a branch revision: " + branchRevisionId);
        }
        RevisionVector ancestor = RevisionVector.fromString(ancestorRevisionId);
        if (!ancestor.isBranch()) {
            throw new DocumentStoreException("Not a branch revision: " + ancestorRevisionId);
        }
        try {
            return nodeStore.reset(branch, ancestor).toString();
        } catch (DocumentStoreException e) {
            throw new DocumentStoreException(e);
        }
    }

    public long getLength(String blobId) throws DocumentStoreException {
        try {
            return nodeStore.getBlobStore().getBlobLength(blobId);
        } catch (Exception e) {
            throw new DocumentStoreException(e);
        }
    }

    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws DocumentStoreException {
        try {
            int read = nodeStore.getBlobStore().readBlob(blobId, pos, buff, off, length);
            return read < 0 ? 0 : read;
        } catch (Exception e) {
            throw new DocumentStoreException(e);
        }
    }

    public String write(InputStream in) throws DocumentStoreException {
        try {
            return nodeStore.getBlobStore().writeBlob(in);
        } catch (Exception e) {
            throw new DocumentStoreException(e);
        }
    }

    //-------------------------< accessors >------------------------------------

    public DocumentStore getDocumentStore() {
        return store;
    }

    //------------------------------< internal >--------------------------------

    private void parseJsonDiff(Commit commit, String json, String rootPath) {
        RevisionVector baseRev = commit.getBaseRevision();
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
                        throw new DocumentStoreException("Node not found: " + path + " in revision " + baseRevId);
                    }
                    commit.removeNode(path, toRemove);
                    markAsDeleted(toRemove, commit, true);
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
                    break;
                case '>': {
                    t.read(':');
                    String targetPath = t.readString();
                    if (!PathUtils.isAbsolute(targetPath)) {
                        targetPath = PathUtils.concat(rootPath, targetPath);
                    }
                    DocumentNodeState source = nodeStore.getNode(path, baseRev);
                    if (source == null) {
                        throw new DocumentStoreException("Node not found: " + path + " in revision " + baseRevId);
                    } else if (nodeExists(targetPath, baseRevId)) {
                        throw new DocumentStoreException("Node already exists: " + targetPath + " in revision " + baseRevId);
                    }
                    moveNode(source, targetPath, commit);
                    break;
                }
                case '*': {
                    t.read(':');
                    String targetPath = t.readString();
                    if (!PathUtils.isAbsolute(targetPath)) {
                        targetPath = PathUtils.concat(rootPath, targetPath);
                    }
                    DocumentNodeState source = nodeStore.getNode(path, baseRev);
                    if (source == null) {
                        throw new DocumentStoreException("Node not found: " + path + " in revision " + baseRevId);
                    } else if (nodeExists(targetPath, baseRevId)) {
                        throw new DocumentStoreException("Node already exists: " + targetPath + " in revision " + baseRevId);
                    }
                    copyNode(source, targetPath, commit);
                    break;
                }
                default:
                    throw new DocumentStoreException("token: " + (char) t.getTokenType());
            }
        }
    }

    private void parseAddNode(Commit commit, JsopReader t, String path) {
        List<PropertyState> props = Lists.newArrayList();
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    String childPath = PathUtils.concat(path, key);
                    parseAddNode(commit, t, childPath);
                } else {
                    String value = t.readRawValue().trim();
                    props.add(nodeStore.createPropertyState(key, value));
                }
            } while (t.matches(','));
            t.read('}');
        }
        DocumentNodeState n = new DocumentNodeState(nodeStore, path,
                new RevisionVector(commit.getRevision()), props, false, null);
        commit.addNode(n);
    }

    private void copyNode(DocumentNodeState source, String targetPath, Commit commit) {
        moveOrCopyNode(false, source, targetPath, commit);
    }

    private void moveNode(DocumentNodeState source, String targetPath, Commit commit) {
        moveOrCopyNode(true, source, targetPath, commit);
    }

    private void markAsDeleted(DocumentNodeState node, Commit commit, boolean subTreeAlso) {
        commit.removeNode(node.getPath(), node);

        if (subTreeAlso) {
            // recurse down the tree
            for (DocumentNodeState child : nodeStore.getChildNodes(node, null, Integer.MAX_VALUE)) {
                markAsDeleted(child, commit, true);
            }
        }
    }

    private void moveOrCopyNode(boolean move,
                                DocumentNodeState source,
                                String targetPath,
                                Commit commit) {
        RevisionVector destRevision = commit.getBaseRevision().update(commit.getRevision());
        DocumentNodeState newNode = new DocumentNodeState(nodeStore, targetPath, destRevision,
                source.getProperties(), false, null);

        commit.addNode(newNode);
        if (move) {
            markAsDeleted(source, commit, false);
        }
        for (DocumentNodeState child : nodeStore.getChildNodes(source, null, Integer.MAX_VALUE)) {
            String childName = PathUtils.getName(child.getPath());
            String destChildPath = concat(targetPath, childName);
            moveOrCopyNode(move, child, destChildPath, commit);
        }
    }

    private static void append(DocumentNodeState node,
                               JsopWriter json,
                               boolean includeId) {
        if (includeId) {
            json.key(":id").value(node.getId());
        }
        for (String name : node.getPropertyNames()) {
            json.key(name).encodedValue(node.getPropertyAsString(name));
        }
    }

    //----------------------------< Builder >-----------------------------------

    /**
     * A builder for a DocumentMK instance.
     */
    public static class Builder {
        private static final long DEFAULT_MEMORY_CACHE_SIZE = 256 * 1024 * 1024;
        public static final int DEFAULT_NODE_CACHE_PERCENTAGE = 25;
        public static final int DEFAULT_PREV_DOC_CACHE_PERCENTAGE = 4;
        public static final int DEFAULT_CHILDREN_CACHE_PERCENTAGE = 10;
        public static final int DEFAULT_DIFF_CACHE_PERCENTAGE = 5;
        public static final int DEFAULT_CACHE_SEGMENT_COUNT = 16;
        public static final int DEFAULT_CACHE_STACK_MOVE_DISTANCE = 16;
        private DocumentNodeStore nodeStore;
        private DocumentStore documentStore;
        private String mongoUri;
        private DiffCache diffCache;
        private BlobStore blobStore;
        private int clusterId  = Integer.getInteger("oak.documentMK.clusterId", 0);
        private int asyncDelay = 1000;
        private boolean timing;
        private boolean logging;
        private boolean leaseCheck = true; // OAK-2739 is enabled by default also for non-osgi
        private boolean isReadOnlyMode = false;
        private Weigher<CacheValue, CacheValue> weigher = new EmpiricalWeigher();
        private long memoryCacheSize = DEFAULT_MEMORY_CACHE_SIZE;
        private int nodeCachePercentage = DEFAULT_NODE_CACHE_PERCENTAGE;
        private int prevDocCachePercentage = DEFAULT_PREV_DOC_CACHE_PERCENTAGE;
        private int childrenCachePercentage = DEFAULT_CHILDREN_CACHE_PERCENTAGE;
        private int diffCachePercentage = DEFAULT_DIFF_CACHE_PERCENTAGE;
        private int cacheSegmentCount = DEFAULT_CACHE_SEGMENT_COUNT;
        private int cacheStackMoveDistance = DEFAULT_CACHE_STACK_MOVE_DISTANCE;
        private boolean useSimpleRevision;
        private long maxReplicationLagMillis = TimeUnit.HOURS.toMillis(6);
        private boolean disableBranches;
        private boolean prefetchExternalChanges;
        private Clock clock = Clock.SIMPLE;
        private Executor executor;
        private String persistentCacheURI = DEFAULT_PERSISTENT_CACHE_URI;
        private PersistentCache persistentCache;
        private String journalCacheURI;
        private PersistentCache journalCache;
        private LeaseFailureHandler leaseFailureHandler;
        private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;
        private BlobStoreStats blobStoreStats;
        private CacheStats blobStoreCacheStats;
        private DocumentStoreStatsCollector documentStoreStatsCollector;
        private DocumentNodeStoreStatsCollector nodeStoreStatsCollector;
        private Map<CacheType, PersistentCacheStats> persistentCacheStats =
                new EnumMap<CacheType, PersistentCacheStats>(CacheType.class);
        private boolean bundlingDisabled;

        public Builder() {
        }

        /**
         * Uses the given information to connect to to MongoDB as backend
         * storage for the DocumentNodeStore. The write concern is either
         * taken from the URI or determined automatically based on the MongoDB
         * setup. When running on a replica set without explicit write concern
         * in the URI, the write concern will be {@code MAJORITY}, otherwise
         * {@code ACKNOWLEDGED}.
         *
         * @param uri a MongoDB URI.
         * @param name the name of the database to connect to. This overrides
         *             any database name given in the {@code uri}.
         * @param blobCacheSizeMB the blob cache size in MB.
         * @return this
         * @throws UnknownHostException if one of the hosts given in the URI
         *          is unknown.
         */
        public Builder setMongoDB(@Nonnull String uri,
                                  @Nonnull String name,
                                  int blobCacheSizeMB)
                throws UnknownHostException {
            this.mongoUri = uri;

            DB db = new MongoConnection(uri).getDB(name);
            if (!MongoConnection.hasWriteConcern(uri)) {
                db.setWriteConcern(MongoConnection.getDefaultWriteConcern(db));
            }
            setMongoDB(db, blobCacheSizeMB);
            return this;
        }

        /**
         * Use the given MongoDB as backend storage for the DocumentNodeStore.
         *
         * @param db the MongoDB connection
         * @return this
         */
        public Builder setMongoDB(@Nonnull DB db,
                                  int blobCacheSizeMB) {
            if (!MongoConnection.hasSufficientWriteConcern(db)) {
                LOG.warn("Insufficient write concern: " + db.getWriteConcern()
                        + " At least " + MongoConnection.getDefaultWriteConcern(db) + " is recommended.");
            }
            if (this.documentStore == null) {
                this.documentStore = new MongoDocumentStore(db, this);
            }

            if (this.blobStore == null) {
                GarbageCollectableBlobStore s = new MongoBlobStore(db, blobCacheSizeMB * 1024 * 1024L);
                configureBlobStore(s);
                PersistentCache p = getPersistentCache();
                if (p != null) {
                    s = p.wrapBlobStore(s);
                }
                this.blobStore = s;
            }
            return this;
        }

        /**
         * Use the given MongoDB as backend storage for the DocumentNodeStore.
         *
         * @param db the MongoDB connection
         * @return this
         */
        public Builder setMongoDB(@Nonnull DB db) {
            return setMongoDB(db, 16);
        }

        /**
         * Returns the Mongo URI used in the {@link #setMongoDB(String, String, int)} method.
         *
         * @return the Mongo URI or null if the {@link #setMongoDB(String, String, int)} method hasn't
         * been called.
         */
        public String getMongoUri() {
            return mongoUri;
        }

        /**
         * Sets a {@link DataSource} to use for the RDB document and blob
         * stores.
         *
         * @return this
         */
        public Builder setRDBConnection(DataSource ds) {
            setRDBConnection(ds, new RDBOptions());
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
                configureBlobStore(blobStore);
            }
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
            configureBlobStore(blobStore);
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
         * Sets the journal cache option.
         *
         * @return this
         */
        public Builder setJournalCache(String journalCache) {
            this.journalCacheURI = journalCache;
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

        public Builder setLeaseCheck(boolean leaseCheck) {
            this.leaseCheck = leaseCheck;
            return this;
        }

        public boolean getLeaseCheck() {
            return leaseCheck;
        }

        public Builder setReadOnlyMode() {
            this.isReadOnlyMode = true;
            return this;
        }

        public boolean getReadOnlyMode() {
            return isReadOnlyMode;
        }

        public Builder setLeaseFailureHandler(LeaseFailureHandler leaseFailureHandler) {
            this.leaseFailureHandler = leaseFailureHandler;
            return this;
        }

        public LeaseFailureHandler getLeaseFailureHandler() {
            return leaseFailureHandler;
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
                configureBlobStore(blobStore);
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
                                               int prevDocCachePercentage,
                                               int childrenCachePercentage,
                                               int diffCachePercentage) {
            checkArgument(nodeCachePercentage >= 0);
            checkArgument(prevDocCachePercentage >= 0);
            checkArgument(childrenCachePercentage>= 0);
            checkArgument(diffCachePercentage >= 0);
            checkArgument(nodeCachePercentage + prevDocCachePercentage + childrenCachePercentage +
                    diffCachePercentage < 100);
            this.nodeCachePercentage = nodeCachePercentage;
            this.prevDocCachePercentage = prevDocCachePercentage;
            this.childrenCachePercentage = childrenCachePercentage;
            this.diffCachePercentage = diffCachePercentage;
            return this;
        }

        public long getNodeCacheSize() {
            return memoryCacheSize * nodeCachePercentage / 100;
        }

        public long getPrevDocumentCacheSize() {
            return memoryCacheSize * prevDocCachePercentage / 100;
        }

        public long getChildrenCacheSize() {
            return memoryCacheSize * childrenCachePercentage / 100;
        }

        public long getDocumentCacheSize() {
            return memoryCacheSize - getNodeCacheSize() - getPrevDocumentCacheSize() - getChildrenCacheSize()
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

        public Builder setStatisticsProvider(StatisticsProvider statisticsProvider){
            this.statisticsProvider = statisticsProvider;
            return this;
        }

        public StatisticsProvider getStatisticsProvider() {
            return this.statisticsProvider;
        }
        public DocumentStoreStatsCollector getDocumentStoreStatsCollector() {
            if (documentStoreStatsCollector == null) {
                documentStoreStatsCollector = new DocumentStoreStats(statisticsProvider);
            }
            return documentStoreStatsCollector;
        }

        public Builder setDocumentStoreStatsCollector(DocumentStoreStatsCollector documentStoreStatsCollector) {
            this.documentStoreStatsCollector = documentStoreStatsCollector;
            return this;
        }

        public DocumentNodeStoreStatsCollector getNodeStoreStatsCollector() {
            if (nodeStoreStatsCollector == null) {
                nodeStoreStatsCollector = new DocumentNodeStoreStats(statisticsProvider);
            }
            return nodeStoreStatsCollector;
        }

        public Builder setNodeStoreStatsCollector(DocumentNodeStoreStatsCollector statsCollector) {
            this.nodeStoreStatsCollector = statsCollector;
            return this;
        }

        @Nonnull
        public Map<CacheType, PersistentCacheStats> getPersistenceCacheStats() {
            return persistentCacheStats;
        }

        @CheckForNull
        public BlobStoreStats getBlobStoreStats() {
            return blobStoreStats;
        }

        @CheckForNull
        public CacheStats getBlobStoreCacheStats() {
            return blobStoreCacheStats;
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

        public Builder setBundlingDisabled(boolean enabled) {
            bundlingDisabled = enabled;
            return this;
        }

        public boolean isBundlingDisabled() {
            return bundlingDisabled;
        }

        public Builder setPrefetchExternalChanges(boolean b) {
            prefetchExternalChanges = b;
            return this;
        }

        public boolean isPrefetchExternalChanges() {
            return prefetchExternalChanges;
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

        Iterable<ReferencedBlob> createReferencedBlobs(final DocumentNodeStore ns) {
            final DocumentStore store = getDocumentStore();
            return new Iterable<ReferencedBlob>() {
                @Override
                public Iterator<ReferencedBlob> iterator() {
                    if(store instanceof MongoDocumentStore){
                        return new MongoBlobReferenceIterator(ns, (MongoDocumentStore) store);
                    }
                    return new BlobReferenceIterator(ns);
                }
            };
        }

        public MissingLastRevSeeker createMissingLastRevSeeker() {
            final DocumentStore store = getDocumentStore();
            if (store instanceof MongoDocumentStore) {
                return new MongoMissingLastRevSeeker((MongoDocumentStore) store, getClock());
            } else {
                return new MissingLastRevSeeker(store, getClock());
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

        public Cache<PathRev, DocumentNodeState.Children> buildChildrenCache(DocumentNodeStore store) {
            return buildCache(CacheType.CHILDREN, getChildrenCacheSize(), store, null);
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

        public Cache<StringValue, NodeDocument> buildPrevDocumentsCache(DocumentStore docStore) {
            return buildCache(CacheType.PREV_DOCUMENT, getPrevDocumentCacheSize(), null, docStore);
        }

        public NodeDocumentCache buildNodeDocumentCache(DocumentStore docStore, NodeDocumentLocks locks) {
            Cache<CacheValue, NodeDocument> nodeDocumentsCache = buildDocumentCache(docStore);
            CacheStats nodeDocumentsCacheStats = new CacheStats(nodeDocumentsCache, "Document-Documents", getWeigher(), getDocumentCacheSize());

            Cache<StringValue, NodeDocument> prevDocumentsCache = buildPrevDocumentsCache(docStore);
            CacheStats prevDocumentsCacheStats = new CacheStats(prevDocumentsCache, "Document-PrevDocuments", getWeigher(), getPrevDocumentCacheSize());

            return new NodeDocumentCache(nodeDocumentsCache, nodeDocumentsCacheStats, prevDocumentsCache, prevDocumentsCacheStats, locks);
        }

        @SuppressWarnings("unchecked")
        private <K extends CacheValue, V extends CacheValue> Cache<K, V> buildCache(
                CacheType cacheType,
                long maxWeight,
                DocumentNodeStore docNodeStore,
                DocumentStore docStore
                ) {
            Set<EvictionListener<K, V>> listeners = new CopyOnWriteArraySet<EvictionListener<K,V>>();
            Cache<K, V> cache = buildCache(cacheType.name(), maxWeight, listeners);
            PersistentCache p = null;
            if (cacheType == CacheType.DIFF || cacheType == CacheType.LOCAL_DIFF) {
                // use separate journal cache if configured
                p = getJournalCache();
            }
            if (p == null) {
                // otherwise fall back to single persistent cache
                p = getPersistentCache();
            }
            if (p != null) {
                cache = p.wrap(docNodeStore, docStore, cache, cacheType, statisticsProvider);
                if (cache instanceof EvictionListener) {
                    listeners.add((EvictionListener<K, V>) cache);
                }
                PersistentCacheStats stats = PersistentCache.getPersistentCacheStats(cache);
                if (stats != null) {
                    persistentCacheStats.put(cacheType, stats);
                }
            }
            return cache;
        }

        public PersistentCache getPersistentCache() {
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

        PersistentCache getJournalCache() {
            if (journalCacheURI == null) {
                return null;
            }
            if (journalCache == null) {
                try {
                    journalCache = new PersistentCache(journalCacheURI);
                } catch (Throwable e) {
                    LOG.warn("Journal cache not available; please disable the configuration", e);
                    throw new IllegalArgumentException(e);
                }
            }
            return journalCache;
        }

        private <K extends CacheValue, V extends CacheValue> Cache<K, V> buildCache(
                String module,
                long maxWeight,
                final Set<EvictionListener<K, V>> listeners) {
            // by default, use the LIRS cache when using the persistent cache,
            // but don't use it otherwise
            boolean useLirs = persistentCacheURI != null;
            // allow to override this by using the system property
            if (LIRS_CACHE != null) {
                useLirs = LIRS_CACHE;
            }
            if (useLirs) {
                return CacheLIRS.<K, V>newBuilder().
                        module(module).
                        weigher(new Weigher<K, V>() {
                            @Override
                            public int weigh(K key, V value) {
                                return weigher.weigh(key, value);
                            }
                        }).
                        averageWeight(2000).
                        maximumWeight(maxWeight).
                        segmentCount(cacheSegmentCount).
                        stackMoveDistance(cacheStackMoveDistance).
                        recordStats().
                        evictionCallback(new EvictionCallback<K, V>() {
                            @Override
                            public void evicted(K key, V value, RemovalCause cause) {
                                for (EvictionListener<K, V> l : listeners) {
                                    l.evicted(key, value, cause);
                                }
                            }
                        }).
                        build();
            }
            return CacheBuilder.newBuilder().
                    concurrencyLevel(cacheSegmentCount).
                    weigher(weigher).
                    maximumWeight(maxWeight).
                    recordStats().
                    removalListener(new RemovalListener<K, V>() {
                        @Override
                        public void onRemoval(RemovalNotification<K, V> notification) {
                            for (EvictionListener<K, V> l : listeners) {
                                l.evicted(notification.getKey(), notification.getValue(), notification.getCause());
                            }
                        }
                    }).
                    build();
        }

        /**
         * BlobStore which are created by builder might get wrapped.
         * So here we perform any configuration and also access any
         * service exposed by the store
         *
         * @param blobStore store to config
         */
        private void configureBlobStore(BlobStore blobStore) {
            if (blobStore instanceof AbstractBlobStore){
                this.blobStoreStats = new BlobStoreStats(statisticsProvider);
                ((AbstractBlobStore) blobStore).setStatsCollector(blobStoreStats);
            }

            if (blobStore instanceof CachingBlobStore){
                blobStoreCacheStats = ((CachingBlobStore) blobStore).getCacheStats();
            }
        }

    }

}
