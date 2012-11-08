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
package org.apache.jackrabbit.mongomk.impl;

import java.io.InputStream;
import java.util.UUID;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.blobs.BlobStore;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.util.NodeFilter;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.json.JsonUtil;
import org.apache.jackrabbit.mongomk.impl.model.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.model.MongoCommit;
import org.apache.jackrabbit.mongomk.impl.model.tree.MongoNodeState;

/**
 * The {@code MongoDB} implementation of the {@link MicroKernel}.
 *
 * <p>
 * This class will transform and delegate to instances of {@link NodeStore} and
 * {@link BlobStore}.
 * </p>
 */
public class MongoMicroKernel implements MicroKernel {

    private final MongoConnection mongoConnection;
    private final BlobStore blobStore;
    private final NodeStore nodeStore;

    /**
     * Constructs a new {@code MongoMicroKernel}.
     *
     * @param mongoConnection Connection to MongoDB.
     * @param nodeStore The {@link NodeStore}.
     * @param blobStore The {@link BlobStore}.
     */
    public MongoMicroKernel(MongoConnection mongoConnection, NodeStore nodeStore,
            BlobStore blobStore) {
        this.mongoConnection = mongoConnection;
        this.nodeStore = nodeStore;
        this.blobStore = blobStore;
    }

    public void dispose() {
        mongoConnection.close();
    }

    /**
     * Returns the underlying blob store.
     *
     * @return Blob store.
     */
    public BlobStore getBlobStore() {
        return blobStore;
    }

    /**
     * Returns the underlying node store.
     *
     * @return Node store.
     */
    public NodeStore getNodeStore() {
        return nodeStore;
    }

    @Override
    public String branch(String trunkRevisionId) throws MicroKernelException {
        String revId = trunkRevisionId == null ? getHeadRevision() : trunkRevisionId;

        try {
            MongoCommit commit = (MongoCommit)CommitBuilder.build("", "", revId,
                    MongoNodeStore.INITIAL_COMMIT_MESSAGE);
            commit.setBranchId(UUID.randomUUID().toString());
            return nodeStore.commit(commit);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String commit(String path, String jsonDiff, String revisionId, String message) throws MicroKernelException {
        try {
            Commit commit = CommitBuilder.build(path, jsonDiff, revisionId, message);
            return nodeStore.commit(commit);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String path,
            int depth) throws MicroKernelException {
        try {
            return nodeStore.diff(fromRevisionId, toRevisionId, path, depth);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        Node node;
        try {
            node = nodeStore.getNodes(path, revisionId, 0, 0, -1, null);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
        if (node != null) {
            return node.getChildNodeCount();
        } else {
            throw new MicroKernelException("Path " + path + " not found in revision "
                    + revisionId);
        }
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        try {
            return nodeStore.getHeadRevision();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) throws MicroKernelException {
        try {
            return nodeStore.getJournal(fromRevisionId, toRevisionId, path);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
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
    public String getNodes(String path, String revisionId, int depth, long offset,
            int maxChildNodes, String filter) throws MicroKernelException {

        NodeFilter nodeFilter = filter == null || filter.isEmpty() ? null : NodeFilter.parse(filter);
        if (offset > 0 && nodeFilter != null && nodeFilter.getChildNodeFilter() != null) {
            // Both an offset > 0 and a filter on node names have been specified...
            throw new IllegalArgumentException("offset > 0 with child node filter");
        }

        try {
            // FIXME Should filter, offset, and maxChildNodes be handled in Mongo instead?
            Node rootNode = nodeStore.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
            if (rootNode == null) {
                return null;
            }

            JsopBuilder builder = new JsopBuilder().object();
            JsonUtil.toJson(builder, new MongoNodeState(rootNode), depth, (int)offset, maxChildNodes, true, nodeFilter);
            return builder.endObject().toString();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path)
            throws MicroKernelException {
        try {
            return nodeStore.getRevisionHistory(since, maxEntries, path);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        try {
            return nodeStore.merge(branchRevisionId, message);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws MicroKernelException {
        try {
            return nodeStore.nodeExists(path, revisionId);
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
    public String waitForCommit(String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        try {
            return nodeStore.waitForCommit(oldHeadRevisionId, timeout);
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
}