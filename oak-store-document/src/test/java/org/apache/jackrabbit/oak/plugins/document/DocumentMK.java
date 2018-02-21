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

import static com.google.common.base.Suppliers.ofInstance;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;

import java.io.InputStream;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopStream;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.json.JsopWriter;
import org.apache.jackrabbit.oak.json.JsopDiff;
import org.apache.jackrabbit.oak.plugins.blob.ReferencedBlob;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState.Children;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentNodeStoreBuilderBase;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobReferenceIterator;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBBlobStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBVersionGCSupport;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JSON-based wrapper around the NodeStore implementation that stores the
 * data in a {@link DocumentStore}. It is used for testing purpose only.
 */
public class DocumentMK {

    static final Logger LOG = LoggerFactory.getLogger(DocumentMK.class);

    /**
     * The threshold where special handling for many child node starts.
     */
    static final int MANY_CHILDREN_THRESHOLD = DocumentNodeStoreBuilder.MANY_CHILDREN_THRESHOLD;
    /**
     * Number of content updates that need to happen before the updates
     * are automatically purged to the private branch.
     */
    static final int UPDATE_LIMIT = DocumentNodeStoreBuilder.UPDATE_LIMIT;

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
        nodeStore.runBackgroundReadOperations();
    }

    void backgroundWrite() {
        nodeStore.runBackgroundUpdateOperations();
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
    public static class Builder extends MongoDocumentNodeStoreBuilderBase<Builder> {
        public static final long DEFAULT_MEMORY_CACHE_SIZE = DocumentNodeStoreBuilder.DEFAULT_MEMORY_CACHE_SIZE;
        public static final int DEFAULT_NODE_CACHE_PERCENTAGE = DocumentNodeStoreBuilder.DEFAULT_NODE_CACHE_PERCENTAGE;
        public static final int DEFAULT_PREV_DOC_CACHE_PERCENTAGE = DocumentNodeStoreBuilder.DEFAULT_PREV_DOC_CACHE_PERCENTAGE;
        public static final int DEFAULT_CHILDREN_CACHE_PERCENTAGE = DocumentNodeStoreBuilder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
        public static final int DEFAULT_DIFF_CACHE_PERCENTAGE = DocumentNodeStoreBuilder.DEFAULT_DIFF_CACHE_PERCENTAGE;
        public static final int DEFAULT_CACHE_SEGMENT_COUNT = DocumentNodeStoreBuilder.DEFAULT_CACHE_SEGMENT_COUNT;
        public static final int DEFAULT_CACHE_STACK_MOVE_DISTANCE = DocumentNodeStoreBuilder.DEFAULT_CACHE_STACK_MOVE_DISTANCE;
        public static final int DEFAULT_UPDATE_LIMIT = DocumentNodeStoreBuilder.DEFAULT_UPDATE_LIMIT;
        private DocumentNodeStore nodeStore;

        public Builder() {
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
            this.documentStoreSupplier = ofInstance(new RDBDocumentStore(ds, this, options));
            if(blobStore == null) {
                GarbageCollectableBlobStore s = new RDBBlobStore(ds, options);
                setGCBlobStore(s);
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
            this.documentStoreSupplier = ofInstance(new RDBDocumentStore(documentStoreDataSource, this));
            if(blobStore == null) {
                GarbageCollectableBlobStore s = new RDBBlobStore(blobStoreDataSource);
                setGCBlobStore(s);
            }
            return this;
        }

        public DocumentNodeStore getNodeStore() {
            if (nodeStore == null) {
                nodeStore = build();
            }
            return nodeStore;
        }

        public VersionGCSupport createVersionGCSupport() {
            DocumentStore store = getDocumentStore();
            if (store instanceof RDBDocumentStore) {
                return new RDBVersionGCSupport((RDBDocumentStore) store);
            } else {
                return super.createVersionGCSupport();
            }
        }

        public Iterable<ReferencedBlob> createReferencedBlobs(DocumentNodeStore ns) {
            final DocumentStore store = getDocumentStore();
            if (store instanceof RDBDocumentStore) {
                return () -> new RDBBlobReferenceIterator(ns, (RDBDocumentStore) store);
            } else {
                return super.createReferencedBlobs(ns);
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
    }
}
