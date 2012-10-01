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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.util.NodeFilter;
import org.apache.jackrabbit.mongomk.api.BlobStore;
import org.apache.jackrabbit.mongomk.api.NodeStore;
import org.apache.jackrabbit.mongomk.api.model.Commit;
import org.apache.jackrabbit.mongomk.api.model.Node;
import org.apache.jackrabbit.mongomk.impl.builder.CommitBuilder;
import org.apache.jackrabbit.mongomk.impl.json.JsonUtil;

/**
 * The {@code MongoDB} implementation of the {@link MicroKernel}.
 *
 * <p>
 * This class will transform and delegate to instances of {@link NodeStore} and {@link BlobStore}.
 * </p>
 *
 * @author <a href="mailto:pmarx@adobe.com>Philipp Marx</a>
 */
public class MongoMicroKernel implements MicroKernel {

    private final BlobStore blobStore;
    private final NodeStore nodeStore;

    /**
     * Constructs a new {@code MongoMicroKernel}.
     *
     * @param nodeStore The {@link NodeStore}.
     * @param blobStore The {@link BlobStore}.
     */
    public MongoMicroKernel(NodeStore nodeStore, BlobStore blobStore) {
        this.nodeStore = nodeStore;
        this.blobStore = blobStore;
    }

    @Override
    public String branch(String trunkRevisionId) throws MicroKernelException {
        throw new UnsupportedOperationException("Branch is currently not supported.");
    }

    @Override
    public String commit(String path, String jsonDiff, String revisionId, String message) throws MicroKernelException {
        String newRevisionId = null;

        try {
            Commit commit = CommitBuilder.build(path, jsonDiff, message);
            newRevisionId = nodeStore.commit(commit);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        return newRevisionId;
    }

    @Override
    public String diff(String fromRevisionId, String toRevisionId, String filter,
            int depth) throws MicroKernelException {
        throw new UnsupportedOperationException("Diff is currently not supported.");
    }

    @Override
    public long getChildNodeCount(String path, String revisionId) throws MicroKernelException {
        long childNodeCount = 0L;

        try {
            String revId = null;
            if (revisionId != null) {
                revId = new String(revisionId);
            }
            Node rootOfPath = nodeStore.getNodes(path, revId, 0, 0, -1, null);
            if (rootOfPath != null) {
                childNodeCount = rootOfPath.getChildCount();
            }
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        return childNodeCount;
    }

    @Override
    public String getHeadRevision() throws MicroKernelException {
        String headRevisionId = null;

        try {
            headRevisionId = nodeStore.getHeadRevision();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        return headRevisionId;
    }

    @Override
    public String getJournal(String fromRevisionId, String toRevisionId,
            String path) throws MicroKernelException {
        return nodeStore.getJournal(fromRevisionId, toRevisionId, path);
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        long length = -1;

        try {
            length = blobStore.getBlobLength(blobId);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        return length;
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
            // FIXME [Mete] Should filter, offset, and maxChildNodes be handled in Mongo instead?
            Node rootNode = nodeStore.getNodes(path, revisionId, depth, offset, maxChildNodes, filter);
            if (rootNode == null) {
                return null;
            }
            return JsonUtil.convertToJson(rootNode, depth, (int)offset, maxChildNodes, true, nodeFilter);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    @Override
    public String getRevisionHistory(long since, int maxEntries, String path) throws MicroKernelException {
        return nodeStore.getRevisionHistory(since, maxEntries, path);
    }

    @Override
    public String merge(String branchRevisionId, String message) throws MicroKernelException {
        throw new UnsupportedOperationException("Merge is currently not supported.");
    }

    @Override
    public boolean nodeExists(String path, String revisionId) throws MicroKernelException {
        boolean exists = false;

        try {
            String revId = null;
            if (revisionId != null) {
                revId = new String(revisionId);
            }

            exists = nodeStore.nodeExists(path, revId);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        return exists;
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length) throws MicroKernelException {
        int totalBytes = -1;

        try {
            totalBytes = blobStore.readBlob(blobId, pos, buff, off, length);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        return totalBytes;
    }

    @Override
    public String waitForCommit(String oldHeadRevisionId, long timeout) throws MicroKernelException,
            InterruptedException {
        return nodeStore.waitForCommit(oldHeadRevisionId, timeout);
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        String blobId = null;

        try {
            blobId = blobStore.writeBlob(in);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        return blobId;
    }
}