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
package org.apache.jackrabbit.oak.kernel;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.newConcurrentMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.json.JsopReader;
import org.apache.jackrabbit.mk.json.JsopTokenizer;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.AbstractBlob;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * This is a simple {@link NodeStore}-based {@link MicroKernel} implementation.
 */
public class NodeStoreKernel implements MicroKernel {

    private static final CommitHook CONFLICT_HOOK =
            new EditorHook(new ValidatorProvider() {
                @Override
                protected Validator getRootValidator(
                        NodeState before, NodeState after, CommitInfo info) {
                    return new DefaultValidator() {
                        @Override
                        public Validator childNodeAdded(
                                String name, NodeState after)
                                throws CommitFailedException {
                            if (name.equals(MicroKernel.CONFLICT)) {
                                throw new CommitFailedException(
                                        CommitFailedException.STATE, 0, "Conflict");
                            }
                            return null;
                        }
                        @Override
                        public Validator childNodeChanged(
                                String name, NodeState before, NodeState after)
                                throws CommitFailedException {
                            return this;
                        }
                    };
                }
            });

    private final NodeStore store;

    private final Map<String, Revision> revisions = newLinkedHashMap();

    private final Map<String, Blob> blobs = newConcurrentMap();

    private final BlobSerializer blobSerializer = new BlobSerializer() {
        @Override
        public String serialize(Blob blob) {
            String id = AbstractBlob.calculateSha256(blob).toString();
            blobs.put(id, blob);
            return id;
        }
    };

    private Revision head;

    public NodeStoreKernel(NodeStore store) {
        this.store = store;
        this.head = new Revision(store.getRoot());
        revisions.put(head.id, head);
    }

    @Nonnull
    private synchronized Revision getRevision(@CheckForNull String id) {
        if (id == null) {
            return head;
        } else {
            Revision revision = revisions.get(id);
            if (revision != null) {
                return revision;
            } else {
                throw new MicroKernelException("Revision not found: " + id);
            }
        }
    }

    private NodeState getRoot(String id) throws MicroKernelException {
        return getRevision(id).root;
    }

    private NodeState getNode(String revision, String path)
            throws MicroKernelException {
        NodeState node = getRoot(revision);
        if (path != null) {
            for (String element : PathUtils.elements(path)) {
                node = node.getChildNode(element);
            }
        }
        return node;
    }

    private void applyJsop(NodeBuilder builder, String path, String jsonDiff)
            throws MicroKernelException {
        for (String element : PathUtils.elements(path)) {
            builder = builder.getChildNode(element);
        }
        if (builder.exists()) {
            applyJsop(builder, jsonDiff);
        } else {
            throw new MicroKernelException("Path not found: " + path);
        }
    }

    private void applyJsop(NodeBuilder builder, String jsonDiff) {
        JsopTokenizer tokenizer = new JsopTokenizer(jsonDiff);
        int token = tokenizer.read();
        while (token != JsopReader.END) {
            String path = tokenizer.readString();
            String name = getName(path);
            switch (token) {
            case '+':
                tokenizer.read(':');
                tokenizer.read('{');
                NodeBuilder parent = getNode(builder, getParentPath(path));
                if (builder.hasChildNode(name)) {
                    throw new MicroKernelException(
                            "Node already exists: " + path);
                }
                addNode(parent.setChildNode(name), tokenizer);
                break;
            case '-':
                getNode(builder, path).remove();
                break;
            case '^':
                tokenizer.read(':');
                NodeBuilder node = getNode(builder, getParentPath(path));
                switch (tokenizer.read()) {
                case JsopReader.NULL:
                    node.removeProperty(name);
                    break;
                case JsopReader.FALSE:
                    node.setProperty(name, Boolean.FALSE);
                    break;
                case JsopReader.TRUE:
                    node.setProperty(name, Boolean.TRUE);
                    break;
                case JsopReader.STRING:
                    node.setProperty(name, tokenizer.getToken());
                    break;
                case JsopReader.NUMBER:
                    String value = tokenizer.getToken();
                    try {
                        node.setProperty(name, Long.parseLong(value));
                    } catch (NumberFormatException e) {
                        node.setProperty(name, Double.parseDouble(value));
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
                break;
            case '>':
                tokenizer.read(':');
                String targetPath = tokenizer.readString();
                NodeBuilder targetParent =
                        getNode(builder, getParentPath(targetPath));
                String targetName = getName(targetPath);
                if (path.equals(targetPath) || PathUtils.isAncestor(path, targetPath)) {
                    throw new MicroKernelException(
                            "Target path must not be the same or a descendant of the source path: " + targetPath);
                }
                if (targetParent.hasChildNode(targetName)) {
                    throw new MicroKernelException(
                            "Target node exists: " + targetPath);
                } else if (!getNode(builder, path).moveTo(
                        targetParent, targetName)) {
                    throw new MicroKernelException("Move failed");
                }
                break;
            case '*':
                tokenizer.read(':');
                String copyTarget = tokenizer.readString();
                String copyTargetPath = getParentPath(copyTarget);
                String copyTargetName = getName(copyTarget);

                NodeState copySource = getNode(builder, path).getNodeState();
                NodeBuilder copyTargetParent = getNode(builder, copyTargetPath);
                if (copySource.exists()
                        && !copyTargetParent.hasChildNode(copyTargetName)) {
                    copyTargetParent.setChildNode(copyTargetName, copySource);
                } else {
                    throw new MicroKernelException("Copy failed");
                }
                break;
            default:
                throw new MicroKernelException(
                        "Unexpected token " + (char) token
                        + " in " + jsonDiff);
            }
            token = tokenizer.read();
        }
    }

    private NodeBuilder getNode(NodeBuilder builder, String path) {
        for (String element : PathUtils.elements(path)) {
            builder = builder.getChildNode(element);
        }
        if (builder.exists()) {
            return builder;
        } else {
            throw new MicroKernelException("Path not found: " + path);
        }
    }

    private void addNode(NodeBuilder builder, JsopTokenizer tokenizer)
            throws MicroKernelException {
        if (tokenizer.matches('}')) {
            return;
        }
        do {
            String name = tokenizer.readString();
            tokenizer.read(':');
            switch (tokenizer.read()) {
            case '{':
                NodeBuilder child = builder.setChildNode(name);
                addNode(child, tokenizer);
                break;
            case '[':
                // FIXME: proper array parsing with support for more types
                List<Long> array = newArrayList();
                while (tokenizer.matches(JsopReader.NUMBER)) {
                    array.add(Long.parseLong(tokenizer.getToken()));
                    tokenizer.matches(',');
                }
                tokenizer.read(']');
                builder.setProperty(name, array, Type.LONGS);
                break;
            case JsopReader.FALSE:
                builder.setProperty(name, Boolean.FALSE);
                break;
            case JsopReader.TRUE:
                builder.setProperty(name, Boolean.TRUE);
                break;
            case JsopReader.NUMBER:
                String value = tokenizer.getToken();
                try {
                    builder.setProperty(name, Long.parseLong(value));
                } catch (NumberFormatException e) {
                    builder.setProperty(name, Double.parseDouble(value));
                }
                break;
            case JsopReader.STRING:
                builder.setProperty(name, tokenizer.getToken());
                break;
            default:
                throw new MicroKernelException(
                        "Unexpected token: " + tokenizer.getEscapedToken());
            }
        } while (tokenizer.matches(','));
        tokenizer.read('}');
    }

    @Override
    public synchronized String getHeadRevision() {
        NodeState root = store.getRoot();
        if (!root.equals(head.root)) {
            head = new Revision(head, root, "external");
            revisions.put(head.id, head);
            notifyAll();
        }
        return head.id;
    }

    @Override
    public String checkpoint(long lifetime) {
        return getHeadRevision();
    }

    @Override
    public String getRevisionHistory(
            long since, int maxEntries, String path)
            throws MicroKernelException {
        if (maxEntries < 0) {
            maxEntries = Integer.MAX_VALUE;
        }

        LinkedList<Revision> list = newLinkedList();
        Revision revision = getRevision(null);
        while (revision != null && revision.base != null
                && revision.timestamp >= since) {
            list.addFirst(revision);
            revision = revision.base;
        }

        JsopBuilder json = new JsopBuilder();
        json.array();
        int count = 0;
        for (Revision rev : list) {
            if (!rev.hasPathChanged(path)) {
                if (count++ > maxEntries) {
                    break;
                }
                json.object();
                json.key("id").value(rev.id);
                json.key("ts").value(rev.timestamp);
                json.key("msg").value(rev.message);
                json.endObject();
            }
        }
        json.endArray();
        return json.toString();
    }

    @Override
    public synchronized String waitForCommit(
            String oldHeadRevisionId, long timeout)
            throws MicroKernelException, InterruptedException {
        long stop = System.currentTimeMillis() + timeout;
        while (head.id.equals(oldHeadRevisionId) && timeout > 0) {
            wait(timeout);
            timeout = stop - System.currentTimeMillis();
        }
        return head.id;
    }

    @Override
    public String getJournal(
            String fromRevisionId, String toRevisionId, String path)
            throws MicroKernelException {
        LinkedList<Revision> list = newLinkedList();
        Revision revision = getRevision(toRevisionId);
        while (revision != null) {
            list.addFirst(revision);
            if (revision.id.equals(fromRevisionId)) {
                break;
            } else if (revision.base == null) {
                if (getRevision(fromRevisionId).branch != null) {
                    throw new MicroKernelException();
                }
                list.clear();
                break;
            }
            revision = revision.base;
        }

        JsopBuilder json = new JsopBuilder();
        json.array();
        for (Revision rev : list) {
            String jsop = rev.getPathChanges(path, blobSerializer);
            if (!jsop.isEmpty()) {
                json.object();
                json.key("id").value(rev.id);
                json.key("ts").value(rev.timestamp);
                json.key("msg").value(rev.message);
                json.key("changes").value(jsop);
                json.endObject();
            }
        }
        json.endArray();
        return json.toString();
    }

    @Override
    public String diff(
            String fromRevisionId, String toRevisionId, String path, int depth)
            throws MicroKernelException {
        NodeState before = getNode(fromRevisionId, path);
        NodeState after = getNode(toRevisionId, path);

        JsopDiff diff = new JsopDiff(path, depth);
        after.compareAgainstBaseState(before, diff);
        return diff.toString();
    }

    @Override
    public boolean nodeExists(String path, String revisionId)
            throws MicroKernelException {
        return getNode(revisionId, path).exists();
    }

    @Override
    public long getChildNodeCount(String path, String revisionId)
            throws MicroKernelException {
        NodeState node = getNode(revisionId, path);
        if (node.exists()) {
            return node.getChildNodeCount(Long.MAX_VALUE);
        } else {
            throw new MicroKernelException(
                    "Node not found: " + revisionId + path);
        }
    }

    @Override
    public String getNodes(
            String path, String revisionId, int depth,
            long offset, int maxChildNodes, String filter)
            throws MicroKernelException {
        NodeState node = getNode(revisionId, path);
        if (node.exists()) {
            if (maxChildNodes < 0) {
                maxChildNodes = Integer.MAX_VALUE;
            }
            if (filter == null) {
                filter = "{}";
            }
            JsonSerializer json = new JsonSerializer(
                    depth, offset, maxChildNodes, filter, blobSerializer);
            json.serialize(node);
            return json.toString();
        } else {
            return null;
        }
    }

    @Override
    public synchronized String commit(
            String path, String jsonDiff, String revisionId, String message)
            throws MicroKernelException {
        Revision revision = getRevision(revisionId);

        NodeBuilder builder = revision.branch;
        if (builder == null) {
            builder = revision.root.builder();
        }

        applyJsop(builder, path, jsonDiff);

        if (revision.branch != null) {
            revision = new Revision(revision, builder.getNodeState(), message);
        } else {
            try {
                CommitInfo info =
                        new CommitInfo(CommitInfo.OAK_UNKNOWN, null);
                NodeState newRoot = store.merge(builder, CONFLICT_HOOK, info);
                if (!newRoot.equals(head.root)) {
                    revision = new Revision(head, newRoot, message);
                    head = revision;
                    notifyAll();
                } else {
                    return head.id;
                }
            } catch (CommitFailedException e) {
                throw new MicroKernelException(e);
            }
        }

        revisions.put(revision.id, revision);
        return revision.id;
    }

    @Override
    public synchronized String branch(String trunkRevisionId)
            throws MicroKernelException {
        Revision branch = new Revision(getRevision(trunkRevisionId));
        revisions.put(branch.id, branch);
        return branch.id;
    }

    @Override
    public synchronized String merge(String branchRevisionId, String message)
            throws MicroKernelException {
        Revision revision = getRevision(branchRevisionId);
        if (revision.branch == null) {
            throw new MicroKernelException(
                    "Branch not found: " + branchRevisionId);
        }

        try {
            CommitInfo info =
                    new CommitInfo(CommitInfo.OAK_UNKNOWN, null);
            NodeState newRoot =
                    store.merge(revision.branch, CONFLICT_HOOK, info);
            if (!newRoot.equals(head.root)) {
                head = new Revision(head, newRoot, message);
                revisions.put(head.id, head);
                notifyAll();
            }
        } catch (CommitFailedException e) {
            throw new MicroKernelException(e);
        }

        return head.id;
    }

    @Override
    public String rebase(String branchRevisionId, String newBaseRevisionId)
            throws MicroKernelException {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public synchronized String reset(@Nonnull String branchRevisionId,
                                     @Nonnull String ancestorRevisionId)
            throws MicroKernelException {
        Revision revision = getRevision(branchRevisionId);
        if (revision.branch == null) {
            throw new MicroKernelException(
                    "Branch not found: " + branchRevisionId);
        }
        Revision ancestor = getRevision(ancestorRevisionId);
        while (!ancestor.id.equals(revision.id)) {
            revision = revision.base;
            if (revision.branch == null) {
                throw new MicroKernelException(ancestorRevisionId + " is not " +
                        "an ancestor revision of " + branchRevisionId);
            }
        }
        Revision r = new Revision(ancestor);
        revisions.put(r.id, r);
        return r.id;
    }

    @Override
    public long getLength(String blobId) throws MicroKernelException {
        Blob blob = blobs.get(blobId);
        if (blob != null) {
            return blob.length();
        } else {
            throw new MicroKernelException("Blob not found: " + blobId);
        }
    }

    @Override
    public int read(String blobId, long pos, byte[] buff, int off, int length)
            throws MicroKernelException {
        Blob blob = blobs.get(blobId);
        if (blob != null) {
            try {
                InputStream stream = blob.getNewStream();
                try {
                    ByteStreams.skipFully(stream, pos);
                    return stream.read(buff, off, length);
                } finally {
                    stream.close();
                }
            } catch (IOException e) {
                throw new MicroKernelException("Failed to read a blob", e);
            }
        } else {
            throw new MicroKernelException("Blob not found: " + blobId);
        }
    }

    @Override
    public String write(InputStream in) throws MicroKernelException {
        try {
            final Hasher hasher = Hashing.sha256().newHasher();
            Blob blob = store.createBlob(
                    new CheckedInputStream(in, new Checksum() {
                        @Override
                        public void update(byte[] b, int off, int len) {
                            hasher.putBytes(b, off, len);
                        }
                        @Override
                        public void update(int b) {
                            hasher.putByte((byte) b);
                        }
                        @Override
                        public void reset() {
                            throw new UnsupportedOperationException();
                        }
                        @Override
                        public long getValue() {
                            throw new UnsupportedOperationException();
                        }
                    }));
            HashCode hash = hasher.hash();

            // wrap into AbstractBlob for the SHA-256 memorization feature
            if (!(blob instanceof AbstractBlob)) {
                final Blob b = blob;
                blob = new AbstractBlob(hash) {
                    @Override
                    public long length() {
                        return b.length();
                    }
                    @Override
                    public InputStream getNewStream() {
                        return b.getNewStream();
                    }
                };
            }

            String id = hash.toString();
            blobs.put(id, blob);
            return id;
        } catch (IOException e) {
            throw new MicroKernelException("Failed to create a blob", e);
        }
    }

    private static class Revision {

        private final Revision base;

        private final NodeBuilder branch;

        private final String id = UUID.randomUUID().toString();

        private final NodeState root;

        private final String message;

        private final long timestamp;

        Revision(NodeState root) {
            this.base = null;
            this.branch = null;
            this.root = root;
            this.message = "start";
            this.timestamp = 0;
        }

        Revision(Revision base) {
            this.base = base;
            this.branch = base.root.builder();
            this.root = base.root;
            this.message = "branch";
            this.timestamp = System.currentTimeMillis();
        }

        public Revision(Revision base, NodeState root, String message) {
            this.base = base;
            this.branch = base.branch;
            this.root = root;
            this.message = message;
            this.timestamp = System.currentTimeMillis();
        }

        boolean hasPathChanged(String path) {
            return base != null
                    && getNode(root, path).equals(getNode(base.root, path));
        }

        String getPathChanges(String path, BlobSerializer blobs) {
            JsopDiff diff = new JsopDiff(blobs, path);
            if (base != null) {
                getNode(root, path).compareAgainstBaseState(
                        getNode(base.root, path), diff);
            }
            return diff.toString();
        }

    }

    private static NodeState getNode(NodeState node, String path) {
        if (path != null) {
            for (String element : PathUtils.elements(path)) {
                node = node.getChildNode(element);
            }
        }
        return node;
    }

}
