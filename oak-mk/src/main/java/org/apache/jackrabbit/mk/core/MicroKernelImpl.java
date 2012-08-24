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
package org.apache.jackrabbit.mk.core;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.CommitBuilder;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.json.JsonObject;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.tree.ChildNode;
import org.apache.jackrabbit.mk.model.tree.DiffBuilder;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.PropertyState;
import org.apache.jackrabbit.mk.util.CommitGate;
import org.apache.jackrabbit.mk.util.NameFilter;
import org.apache.jackrabbit.mk.util.NodeFilter;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class MicroKernelImpl implements MicroKernel {

    private static final Logger LOG = LoggerFactory.getLogger(MicroKernelImpl.class);

    protected Repository rep;
    private final CommitGate gate = new CommitGate();

    public MicroKernelImpl(String homeDir) throws MicroKernelException {
        init(homeDir);
    }

    /**
     * Creates a new in-memory kernel instance that doesn't need to be
     * explicitly closed, i.e. standard Java garbage collection will take
     * care of releasing any acquired resources when no longer needed.
     * Useful especially for test cases and other similar scenarios.
     */
    public MicroKernelImpl() {
        this(new Repository());
    }

    /**
     * Alternate constructor, used for testing.
     *
     * @param rep repository, already initialized
     */
    public MicroKernelImpl(Repository rep) {
        this.rep = rep;
        try {
            // initialize commit gate with current head
            gate.commit(rep.getHeadRevision().toString());
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    protected void init(String homeDir) throws MicroKernelException {
        try {
            rep = new Repository(homeDir);
            rep.init();
            // initialize commit gate with current head
            gate.commit(rep.getHeadRevision().toString());
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public void dispose() {
        gate.commit("end");
        if (rep != null) {
            try {
                rep.shutDown();
            } catch (Exception ignore) {
                // fail silently
            }
            rep = null;
        }
    }

    public String getHeadRevision() throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        return getHeadRevisionId().toString();
    }

    /**
     * Same as {@code getHeadRevisionId}, with typed {@code Id} return value instead of string.
     *
     * @see #getHeadRevision()
     */
    private Id getHeadRevisionId() throws MicroKernelException {
        try {
            return rep.getHeadRevision();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String getRevisionHistory(long since, int maxEntries, String path) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        path = (path == null || "".equals(path)) ? "/" : path;
        boolean filtered = !"/".equals(path);

        maxEntries = maxEntries < 0 ? Integer.MAX_VALUE : maxEntries;
        List<StoredCommit> history = new ArrayList<StoredCommit>();
        try {
            StoredCommit commit = rep.getHeadCommit();
            while (commit != null
                    && history.size() < maxEntries
                    && commit.getCommitTS() >= since) {
                if (filtered) {
                    try {
                        String diff = new DiffBuilder(
                                rep.getNodeState(commit.getParentId(), "/"),
                                rep.getNodeState(commit.getId(), "/"),
                                "/", -1, rep.getRevisionStore(), path).build();
                        if (!diff.isEmpty()) {
                            history.add(commit);
                        }
                    } catch (Exception e) {
                        throw new MicroKernelException(e);
                    }
                } else {
                    history.add(commit);
                }

                Id commitId = commit.getParentId();
                if (commitId == null) {
                    break;
                }
                commit = rep.getCommit(commitId);
            }
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        JsopBuilder buff = new JsopBuilder().array();
        for (int i = history.size() - 1; i >= 0; i--) {
            StoredCommit commit = history.get(i);
            buff.object().
                    key("id").value(commit.getId().toString()).
                    key("ts").value(commit.getCommitTS()).
                    key("msg").value(commit.getMsg()).
                    endObject();
        }
        return buff.endArray().toString();
    }

    public String waitForCommit(String oldHeadRevisionId, long maxWaitMillis) throws MicroKernelException, InterruptedException {
        return gate.waitForCommit(oldHeadRevisionId, maxWaitMillis);
    }

    public String getJournal(String fromRevision, String toRevision, String path) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        path = (path == null || "".equals(path)) ? "/" : path;
        boolean filtered = !"/".equals(path);

        Id fromRevisionId = Id.fromString(fromRevision);
        Id toRevisionId = toRevision == null ? getHeadRevisionId() : Id.fromString(toRevision);

        List<StoredCommit> commits = new ArrayList<StoredCommit>();
        try {
            StoredCommit toCommit = rep.getCommit(toRevisionId);
            if (toCommit.getBranchRootId() != null) {
                throw new MicroKernelException("branch revisions are not supported: " + toRevisionId);
            }

            Commit fromCommit;
            if (toRevisionId.equals(fromRevisionId)) {
                fromCommit = toCommit;
            } else {
                fromCommit = rep.getCommit(fromRevisionId);
                if (fromCommit.getCommitTS() > toCommit.getCommitTS()) {
                    // negative range, return empty journal
                    return "[]";
                }
            }
            if (fromCommit.getBranchRootId() != null) {
                throw new MicroKernelException("branch revisions are not supported: " + fromRevisionId);
            }

            // collect commits, starting with toRevisionId
            // and traversing parent commit links until we've reached
            // fromRevisionId
            StoredCommit commit = toCommit;
            while (commit != null) {
                commits.add(commit);
                if (commit.getId().equals(fromRevisionId)) {
                    break;
                }
                Id commitId = commit.getParentId();
                if (commitId == null) {
                    // inconsistent revision history, ignore silently...
                    break;
                }
                commit = rep.getCommit(commitId);
                if (commit.getCommitTS() < fromCommit.getCommitTS()) {
                    // inconsistent revision history, ignore silently...
                    break;
                }
            }
        } catch (MicroKernelException e) {
            // re-throw
            throw e;
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }

        JsopBuilder commitBuff = new JsopBuilder().array();
        // iterate over commits in chronological order,
        // starting with oldest commit
        for (int i = commits.size() - 1; i >= 0; i--) {
            StoredCommit commit = commits.get(i);
            if (commit.getParentId() == null) {
                continue;
            }
            String diff = commit.getChanges();
            if (filtered) {
                try {
                    diff = new DiffBuilder(
                            rep.getNodeState(commit.getParentId(), "/"),
                            rep.getNodeState(commit.getId(), "/"),
                            "/", -1, rep.getRevisionStore(), path).build();
                    if (diff.isEmpty()) {
                        continue;
                    }
                } catch (Exception e) {
                    throw new MicroKernelException(e);
                }
            }
            commitBuff.object().
                    key("id").value(commit.getId().toString()).
                    key("ts").value(commit.getCommitTS()).
                    key("msg").value(commit.getMsg()).
                    key("changes").value(diff).endObject();
        }
        return commitBuff.endArray().toString();
    }

    public String diff(String fromRevision, String toRevision, String path, int depth) throws MicroKernelException {
        path = (path == null || "".equals(path)) ? "/" : path;

        if (depth < -1) {
            throw new IllegalArgumentException("depth");
        }

        Id fromRevisionId, toRevisionId;
        if (fromRevision == null || toRevision == null) {
            Id head = getHeadRevisionId();
            fromRevisionId = fromRevision == null ? head : Id.fromString(fromRevision);
            toRevisionId = toRevision == null ? head : Id.fromString(toRevision);
        } else {
            fromRevisionId = Id.fromString(fromRevision);
            toRevisionId = Id.fromString(toRevision);
        }

        if (fromRevisionId.equals(toRevisionId)) {
            return "";
        }

        try {
            if ("/".equals(path)) {
                StoredCommit toCommit = rep.getCommit(toRevisionId);
                if (toCommit.getParentId().equals(fromRevisionId)) {
                    // specified range spans a single commit:
                    // use diff stored in commit instead of building it dynamically
                    return toCommit.getChanges();
                }
            }
            NodeState before = rep.getNodeState(fromRevisionId, path);
            NodeState after = rep.getNodeState(toRevisionId, path);

            return new DiffBuilder(before, after, path, depth, rep.getRevisionStore(), path).build();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public boolean nodeExists(String path, String revisionId) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        Id revId = revisionId == null ? getHeadRevisionId() : Id.fromString(revisionId);
        try {
            return rep.nodeExists(revId, path);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public long getChildNodeCount(String path, String revisionId) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        Id revId = revisionId == null ? getHeadRevisionId() : Id.fromString(revisionId);

        NodeState nodeState;
        try {
            nodeState = rep.getNodeState(revId, path);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
        if (nodeState != null) {
            return nodeState.getChildNodeCount();
        } else {
            throw new MicroKernelException("Path " + path + " not found in revision " + revisionId);
        }
    }

    public String getNodes(String path, String revisionId, int depth, long offset, int maxChildNodes, String filter) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        Id revId = revisionId == null ? getHeadRevisionId() : Id.fromString(revisionId);

        NodeFilter nodeFilter = filter == null || filter.isEmpty() ? null : NodeFilter.parse(filter);
        if (offset > 0 && nodeFilter != null && nodeFilter.getChildNodeFilter() != null) {
            // both an offset > 0 and a filter on node names have been specified...
            throw new IllegalArgumentException("offset > 0 with child node filter");
        }

        try {
            NodeState nodeState = rep.getNodeState(revId, path);
            if (nodeState == null) {
                return null;
            }

            JsopBuilder buf = new JsopBuilder().object();
            toJson(buf, nodeState, depth, (int) offset, maxChildNodes, true, nodeFilter);
            return buf.endObject().toString();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String commit(String path, String jsonDiff, String revisionId, String message) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        if (path.length() > 0 && !PathUtils.isAbsolute(path)) {
            throw new IllegalArgumentException("absolute path expected: " + path);
        }

        Id revId = revisionId == null ? getHeadRevisionId() : Id.fromString(revisionId);

        try {
            JsopTokenizer t = new JsopTokenizer(jsonDiff);
            CommitBuilder cb = rep.getCommitBuilder(revId, message);
            while (true) {
                int r = t.read();
                if (r == JsopReader.END) {
                    break;
                }
                int pos; // used for error reporting
                switch (r) {
                    case '+': {
                        pos = t.getLastPos();
                        String subPath = t.readString();
                        t.read(':');
                        if (t.matches('{')) {
                            String nodePath = PathUtils.concat(path, subPath);
                            if (!PathUtils.isAbsolute(nodePath)) {
                                throw new Exception("absolute path expected: " + nodePath + ", pos: " + pos);
                            }
                            String parentPath = PathUtils.getParentPath(nodePath);
                            String nodeName = PathUtils.getName(nodePath);
                            cb.addNode(parentPath, nodeName, JsonObject.create(t));
                        } else {
                            String value;
                            if (t.matches(JsopReader.NULL)) {
                                value = null;
                            } else {
                                value = t.readRawValue().trim();
                            }
                            String targetPath = PathUtils.concat(path, subPath);
                            if (!PathUtils.isAbsolute(targetPath)) {
                                throw new Exception("absolute path expected: " + targetPath + ", pos: " + pos);
                            }
                            String parentPath = PathUtils.getParentPath(targetPath);
                            String propName = PathUtils.getName(targetPath);
                            cb.setProperty(parentPath, propName, value);
                        }
                        break;
                    }
                    case '-': {
                        pos = t.getLastPos();
                        String subPath = t.readString();
                        String targetPath = PathUtils.concat(path, subPath);
                        if (!PathUtils.isAbsolute(targetPath)) {
                            throw new Exception("absolute path expected: " + targetPath + ", pos: " + pos);
                        }
                        cb.removeNode(targetPath);
                        break;
                    }
                    case '^': {
                        pos = t.getLastPos();
                        String subPath = t.readString();
                        t.read(':');
                        String value;
                        if (t.matches(JsopReader.NULL)) {
                            value = null;
                        } else {
                            value = t.readRawValue().trim();
                        }
                        String targetPath = PathUtils.concat(path, subPath);
                        if (!PathUtils.isAbsolute(targetPath)) {
                            throw new Exception("absolute path expected: " + targetPath + ", pos: " + pos);
                        }
                        String parentPath = PathUtils.getParentPath(targetPath);
                        String propName = PathUtils.getName(targetPath);
                        cb.setProperty(parentPath, propName, value);
                        break;
                    }
                    case '>': {
                        pos = t.getLastPos();
                        String subPath = t.readString();
                        String srcPath = PathUtils.concat(path, subPath);
                        if (!PathUtils.isAbsolute(srcPath)) {
                            throw new Exception("absolute path expected: " + srcPath + ", pos: " + pos);
                        }
                        t.read(':');
                        pos = t.getLastPos();
                        String targetPath = t.readString();
                        if (!PathUtils.isAbsolute(targetPath)) {
                            targetPath = PathUtils.concat(path, targetPath);
                            if (!PathUtils.isAbsolute(targetPath)) {
                                throw new Exception("absolute path expected: " + targetPath + ", pos: " + pos);
                            }
                        }
                        cb.moveNode(srcPath, targetPath);
                        break;
                    }
                    case '*': {
                        pos = t.getLastPos();
                        String subPath = t.readString();
                        String srcPath = PathUtils.concat(path, subPath);
                        if (!PathUtils.isAbsolute(srcPath)) {
                            throw new Exception("absolute path expected: " + srcPath + ", pos: " + pos);
                        }
                        t.read(':');
                        pos = t.getLastPos();
                        String targetPath = t.readString();
                        if (!PathUtils.isAbsolute(targetPath)) {
                            targetPath = PathUtils.concat(path, targetPath);
                            if (!PathUtils.isAbsolute(targetPath)) {
                                throw new Exception("absolute path expected: " + targetPath + ", pos: " + pos);
                            }
                        }
                        cb.copyNode(srcPath, targetPath);
                        break;
                    }
                    default:
                        throw new AssertionError("token type: " + t.getTokenType());
                }
            }
            Id newHead = cb.doCommit();
            if (!newHead.equals(revId)) {
                // non-empty commit
                if (rep.getCommit(newHead).getBranchRootId() == null) {
                    // OAK-265: only trigger commit gate for non-branch commits
                    gate.commit(newHead.toString());
                }
            }
            return newHead.toString();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String branch(String trunkRevisionId) throws MicroKernelException {
        // create a private branch

        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        Id revId = trunkRevisionId == null ? getHeadRevisionId() : Id.fromString(trunkRevisionId);

        try {
            CommitBuilder cb = rep.getCommitBuilder(revId, "");
            return cb.doCommit(true).toString();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String merge(String branchRevisionId, String message) throws MicroKernelException {
        // merge a private branch with current head revision

        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        Id revId = Id.fromString(branchRevisionId);

        try {
            return rep.getCommitBuilder(revId, message).doMerge().toString();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public long getLength(String blobId) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        try {
            return rep.getBlobStore().getBlobLength(blobId);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public int read(String blobId, long pos, byte[] buff, int off, int length) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        try {
            return rep.getBlobStore().readBlob(blobId, pos, buff, off, length);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String write(InputStream in) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        try {
            return rep.getBlobStore().writeBlob(in);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    //-------------------------------------------------------< implementation >

    void toJson(JsopBuilder builder, NodeState node,
                int depth, int offset, int maxChildNodes,
                boolean inclVirtualProps, NodeFilter filter) {
        for (PropertyState property : node.getProperties()) {
            if (filter == null || filter.includeProperty(property.getName())) {
                builder.key(property.getName()).encodedValue(property.getEncodedValue());
            }
        }
        long childCount = node.getChildNodeCount();
        if (inclVirtualProps) {
            if (filter == null || filter.includeProperty(":childNodeCount")) {
                // :childNodeCount is by default always included
                // unless it is explicitly excluded in the filter
                builder.key(":childNodeCount").value(childCount);
            }
            // check whether :hash has been explicitly included
            if (filter != null) {
                NameFilter nf = filter.getPropertyFilter();
                if (nf != null
                        && nf.getInclusionPatterns().contains(":hash")
                        && !nf.getExclusionPatterns().contains(":hash")) {
                    builder.key(":hash").value(rep.getRevisionStore().getId(node).toString());
                }
            }
        }
        if (childCount > 0 && depth >= 0) {
            if (filter != null) {
                NameFilter childFilter = filter.getChildNodeFilter();
                if (childFilter != null && !childFilter.containsWildcard()) {
                    // optimization for large child node lists:
                    // no need to iterate over the entire child node list if the filter
                    // does not include wildcards
                    int count = maxChildNodes == -1 ? Integer.MAX_VALUE : maxChildNodes;
                    for (String name : childFilter.getInclusionPatterns()) {
                        NodeState child = node.getChildNode(name);
                        if (child != null) {
                            boolean incl = true;
                            for (String exclName : childFilter.getExclusionPatterns()) {
                                if (name.equals(exclName)) {
                                    incl = false;
                                    break;
                                }
                            }
                            if (incl) {
                                if (count-- <= 0) {
                                    break;
                                }
                                builder.key(name).object();
                                if (depth > 0) {
                                    toJson(builder, child, depth - 1, 0, maxChildNodes, inclVirtualProps, filter);
                                }
                                builder.endObject();
                            }
                        }
                    }
                    return;
                }
            }

            int count = maxChildNodes;
            if (count != -1
                    && filter != null
                    && filter.getChildNodeFilter() != null) {
                // specific maxChildNodes limit and child node filter
                count = -1;
            }
            int numSiblings = 0;
            for (ChildNode entry : node.getChildNodeEntries(offset, count)) {
                if (filter == null || filter.includeNode(entry.getName())) {
                    if (maxChildNodes != -1 && ++numSiblings > maxChildNodes) {
                        break;
                    }
                    builder.key(entry.getName()).object();
                    if (depth > 0) {
                        toJson(builder, entry.getNode(), depth - 1, 0, maxChildNodes, inclVirtualProps, filter);
                    }
                    builder.endObject();
                }
            }
        }
    }
}
