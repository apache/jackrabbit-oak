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
package org.apache.jackrabbit.mk;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.mk.model.ChildNodeEntry;
import org.apache.jackrabbit.mk.model.Commit;
import org.apache.jackrabbit.mk.model.CommitBuilder;
import org.apache.jackrabbit.mk.model.Id;
import org.apache.jackrabbit.mk.model.StoredCommit;
import org.apache.jackrabbit.mk.model.StoredNode;
import org.apache.jackrabbit.mk.model.TraversingNodeDiffHandler;
import org.apache.jackrabbit.mk.store.NotFoundException;
import org.apache.jackrabbit.mk.util.CommitGate;
import org.apache.jackrabbit.mk.util.PathUtils;
import org.apache.jackrabbit.mk.util.SimpleLRUCache;
import org.apache.jackrabbit.oak.model.NodeState;
import org.apache.jackrabbit.oak.model.PropertyState;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MicroKernelImpl implements MicroKernel {

    protected Repository rep;
    private final CommitGate gate = new CommitGate();
    private final Map<String, String> diffCache = Collections.synchronizedMap(SimpleLRUCache.<String, String>newInstance(100));

    public MicroKernelImpl(String homeDir) throws MicroKernelException {
        init(homeDir);
    }
    
    /**
     * Alternate constructor, used for testing.
     * 
     * @param rep repository, already initialized
     */
    public MicroKernelImpl(Repository rep) {
        this.rep = rep;
    }

    protected void init(String homeDir) throws MicroKernelException {
        try {
            rep = new Repository(homeDir);
            rep.init();
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
        diffCache.clear();
    }

    public String getHeadRevision() throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        try {
            return rep.getHeadRevision();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String getRevisions(long since, int maxEntries) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        maxEntries = maxEntries < 0 ? Integer.MAX_VALUE : maxEntries;
        List<StoredCommit> history = new ArrayList<StoredCommit>();
        try {
            StoredCommit commit = rep.getHeadCommit();
            while (commit != null
                    && history.size() < maxEntries
                    && commit.getCommitTS() >= since) {
                history.add(commit);

                String commitId = commit.getParentId();
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
                    key("id").value(commit.getId()).
                    key("ts").value(commit.getCommitTS()).
                    endObject();
        }
        return buff.endArray().toString();
    }

    public String waitForCommit(String oldHeadRevision, long maxWaitMillis) throws MicroKernelException, InterruptedException {
        return gate.waitForCommit(oldHeadRevision, maxWaitMillis);
    }

    public String getJournal(String fromRevisionId, String toRevisionId, String filter) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        toRevisionId = toRevisionId == null ? getHeadRevision() : toRevisionId;

        List<StoredCommit> commits = new ArrayList<StoredCommit>();
        try {
            StoredCommit toCommit = rep.getCommit(toRevisionId);

            Commit fromCommit;
            if (toRevisionId.equals(fromRevisionId)) {
                fromCommit = toCommit;
            } else {
                fromCommit = rep.getCommit(fromRevisionId);
                if (fromCommit.getCommitTS() > toCommit.getCommitTS()) {
                    // negative range, return empty array
                    return "[]";
                }
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
                String commitId = commit.getParentId();
                if (commitId == null) {
                    break;
                }
                commit = rep.getCommit(commitId);
            }
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
            commitBuff.object().
                    key("id").value(commit.getId()).
                    key("ts").value(commit.getCommitTS()).
                    key("msg").value(commit.getMsg());
            String diff = diffCache.get(commit.getId());
            if (diff == null) {
                diff = diff(commit.getParentId(), commit.getId(), filter);
                diffCache.put(commit.getId(), diff);
            }
            commitBuff.key("changes").value(diff).endObject();
        }
        return commitBuff.endArray().toString();
    }

    public String diff(String fromRevisionId, String toRevisionId, String filter) throws MicroKernelException {
        // TODO extract and evaluate filter criteria (such as e.g. 'path') specified in 'filter' parameter
        String path = "/";

        toRevisionId = toRevisionId == null ? getHeadRevision() : toRevisionId;

        try {
            final JsopBuilder buff = new JsopBuilder();
            // maps (key: id of target node, value: path/to/target)
            // for tracking added/removed nodes; this allows us
            // to detect 'move' operations
            final HashMap<Id, String> addedNodes = new HashMap<Id, String>();
            final HashMap<Id, String> removedNodes = new HashMap<Id, String>();
            StoredNode node1, node2;
            try {
                node1 = rep.getNode(fromRevisionId, path);
            } catch (NotFoundException e) {
                node1 = null;
            }
            try {
                node2 = rep.getNode(toRevisionId, path);
            } catch (NotFoundException e) {
                node2 = null;
            }

            if (node1 == null) {
                if (node2 != null) {
                    buff.tag('+').key(path).object();
                    toJson(buff, rep.getRevisionStore().getNodeState(node2),
                            Integer.MAX_VALUE, 0, -1, false);
                    return buff.endObject().newline().toString();
                } else {
                    throw new MicroKernelException("path doesn't exist in the specified revisions: " + path);
                }
            } else if (node2 == null) {
                buff.tag('-');
                buff.value(path);
                return buff.newline().toString();
            }

            TraversingNodeDiffHandler diffHandler = new TraversingNodeDiffHandler(rep.getRevisionStore()) {
                @Override
                public void propAdded(String propName, String value) {
                    buff.tag('+').
                            key(PathUtils.concat(getCurrentPath(), propName)).
                            encodedValue(value).
                            newline();
                }

                @Override
                public void propChanged(String propName, String oldValue, String newValue) {
                    buff.tag('^').
                            key(PathUtils.concat(getCurrentPath(), propName)).
                            encodedValue(newValue).
                            newline();
                }

                @Override
                public void propDeleted(String propName, String value) {
                    // since property and node deletions can't be distinguished
                    // using the "- <path>" notation we're representing
                    // property deletions as "^ <path>:null"
                    buff.tag('^').
                            key(PathUtils.concat(getCurrentPath(), propName)).
                            value(null).
                            newline();
                }

                @Override
                public void childNodeAdded(ChildNodeEntry added) {
                    addedNodes.put(added.getId(), PathUtils.concat(getCurrentPath(), added.getName()));
                    buff.tag('+').
                            key(PathUtils.concat(getCurrentPath(), added.getName())).object();
                    try {
                        toJson(buff, store.getNodeState(store.getNode(added.getId())), Integer.MAX_VALUE, 0, -1, false);
                    } catch (Exception e) {
                        buff.value("ERROR: failed to retrieve node " + added.getId());
                    }
                    buff.endObject().newline();
                }

                @Override
                public void childNodeDeleted(ChildNodeEntry deleted) {
                    removedNodes.put(deleted.getId(), PathUtils.concat(getCurrentPath(), deleted.getName()));
                    buff.tag('-');
                    buff.value(PathUtils.concat(getCurrentPath(), deleted.getName()));
                    buff.newline();
                }
            };
            diffHandler.start(node1, node2, path);

            // check if this commit includes 'move' operations
            // by building intersection of added and removed nodes
            addedNodes.keySet().retainAll(removedNodes.keySet());
            if (!addedNodes.isEmpty()) {
                // this commit includes 'move' operations
                removedNodes.keySet().retainAll(addedNodes.keySet());
                // addedNodes & removedNodes now only contain information about moved nodes

                // re-build the diff in a 2nd pass, this time representing moves correctly
                buff.resetWriter();

                // TODO refactor code, avoid duplication

                diffHandler = new TraversingNodeDiffHandler(rep.getRevisionStore()) {
                    @Override
                    public void propAdded(String propName, String value) {
                        buff.tag('+').
                                key(PathUtils.concat(getCurrentPath(), propName)).
                                encodedValue(value).
                                newline();
                    }

                    @Override
                    public void propChanged(String propName, String oldValue, String newValue) {
                        buff.tag('^').
                                key(PathUtils.concat(getCurrentPath(), propName)).
                                encodedValue(newValue).
                                newline();
                    }

                    @Override
                    public void propDeleted(String propName, String value) {
                        // since property and node deletions can't be distinguished
                        // using the "- <path>" notation we're representing
                        // property deletions as "^ <path>:null"
                        buff.tag('^').
                                key(PathUtils.concat(getCurrentPath(), propName)).
                                value(null).
                                newline();
                    }

                    @Override
                    public void childNodeAdded(ChildNodeEntry added) {
                        if (addedNodes.containsKey(added.getId())) {
                            // moved node, will be processed separately
                            return;
                        }
                        buff.tag('+').
                                key(PathUtils.concat(getCurrentPath(), added.getName())).object();
                        try {
                            toJson(buff, store.getNodeState(store.getNode(added.getId())), Integer.MAX_VALUE, 0, -1, false);
                        } catch (Exception e) {
                            buff.value("ERROR: failed to retrieve node " + added.getId());
                        }
                        buff.endObject().newline();
                    }

                    @Override
                    public void childNodeDeleted(ChildNodeEntry deleted) {
                        if (addedNodes.containsKey(deleted.getId())) {
                            // moved node, will be processed separately
                            return;
                        }
                        buff.tag('-');
                        buff.value(PathUtils.concat(getCurrentPath(), deleted.getName()));
                        buff.newline();
                    }

                };
                diffHandler.start(node1, node2, path);

                // finally process moved nodes
                for (Map.Entry<Id, String> entry : addedNodes.entrySet()) {
                    buff.tag('>').
                            // path/to/deleted/node
                            key(removedNodes.get(entry.getKey())).
                            // path/to/added/node
                            value(entry.getValue()).
                            newline();
                }
            }
            return buff.toString();

        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public boolean nodeExists(String path, String revisionId) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        revisionId = revisionId == null ? getHeadRevision() : revisionId;

        return rep.nodeExists(revisionId, path);
    }

    public long getChildNodeCount(String path, String revisionId) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        revisionId = revisionId == null ? getHeadRevision() : revisionId;

        try {
            return rep.getNodeState(revisionId, path).getChildNodeCount();
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String getNodes(String path, String revisionId) throws MicroKernelException {
        return getNodes(path, revisionId, 1, 0, -1, null);
    }

    public String getNodes(String path, String revisionId, int depth, long offset, int count, String filter) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }

        revisionId = revisionId == null ? getHeadRevision() : revisionId;

        // TODO extract and evaluate filter criteria (such as e.g. ':hash') specified in 'filter' parameter

        try {
            JsopBuilder buf = new JsopBuilder().object();
            toJson(buf, rep.getNodeState(revisionId, path), depth, (int) offset, count, true);
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

        revisionId = revisionId == null ? getHeadRevision() : revisionId;

        try {
            JsopTokenizer t = new JsopTokenizer(jsonDiff);
            CommitBuilder cb = rep.getCommitBuilder(revisionId, message);
            while (true) {
                int r = t.read();
                if (r == JsopTokenizer.END) {
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
                            // build the list of added nodes recursively
                            LinkedList<AddNodeOperation> list = new LinkedList<AddNodeOperation>();
                            addNode(list, parentPath, nodeName, t);
                            for (AddNodeOperation op : list) {
                                cb.addNode(op.path, op.name, op.props);
                            }
                        } else {
                            String value;
                            if (t.matches(JsopTokenizer.NULL)) {
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
                        if (t.matches(JsopTokenizer.NULL)) {
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
            String newHead = cb.doCommit();
            if (newHead.equals(revisionId)) {
                // 'empty' commit
                return newHead;
            }
            gate.commit(newHead);
            return newHead;
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public long getLength(String blobId) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        try {
            return rep.getRevisionStore().getBlobLength(blobId);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public int read(String blobId, long pos, byte[] buff, int off, int length) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        try {
            return rep.getRevisionStore().getBlob(blobId, pos, buff, off, length);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    public String write(InputStream in) throws MicroKernelException {
        if (rep == null) {
            throw new IllegalStateException("this instance has already been disposed");
        }
        try {
            return rep.getRevisionStore().putBlob(in);
        } catch (Exception e) {
            throw new MicroKernelException(e);
        }
    }

    //-------------------------------------------------------< implementation >

    void toJson(JsopBuilder builder, NodeState node, int depth, int offset, int count, boolean inclVirtualProps) throws Exception {
        for (PropertyState property : node.getProperties()) {
            builder.key(property.getName()).encodedValue(property.getEncodedValue());
        }
        long childCount = node.getChildNodeCount();
        if (inclVirtualProps) {
            builder.key(":childNodeCount").value(childCount);
        }
        if (childCount > 0 && depth >= 0) {
            // TODO: Use an import once the conflict with .mk.model is resolved
            for (org.apache.jackrabbit.oak.model.ChildNodeEntry entry
                    : node.getChildNodeEntries(offset, count)) {
                builder.key(entry.getName()).object();
                if (depth > 0) {
                    toJson(builder, entry.getNode(), depth - 1, 0, -1, inclVirtualProps);
                }
                builder.endObject();
            }
        }
    }
    
    static void addNode(LinkedList<AddNodeOperation> list, String path, String name, JsopTokenizer t) throws Exception {
        AddNodeOperation op = new AddNodeOperation();
        op.path = path;
        op.name = name;
        list.add(op);
        if (!t.matches('}')) {
            do {
                String key = t.readString();
                t.read(':');
                if (t.matches('{')) {
                    addNode(list, PathUtils.concat(path, name), key, t);
                } else {
                    op.props.put(key, t.readRawValue().trim());
                }
            } while (t.matches(','));
            t.read('}');
        }
    }

    //--------------------------------------------------------< inner classes >
    static class AddNodeOperation {
        String path;
        String name;
        Map<String, String> props = new HashMap<String, String>();
    }

}
